from infrastructure_runtime.security import elasticsearch_options
from collections import Counter, defaultdict
from datetime import UTC, datetime

from elasticsearch import Elasticsearch
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.settings import settings
from app.repositories.analytics_repository import AnalyticsRepository
from app.schemas.dashboard import ChartPoint, DashboardOverviewResponse, EntityRiskPoint, SystemComponentHealth, SystemHealthResponse


class AnalyticsQueryService:
    def __init__(self, repository: AnalyticsRepository | None = None) -> None:
        self._repository = repository or AnalyticsRepository()

    def summary(self, db: Session) -> dict[str, int]:
        return self._repository.summary(db)

    def recent_events(self, db: Session, limit: int) -> list:
        return self._repository.recent_events(db, limit)

    def recent_predictions(self, db: Session, limit: int) -> list:
        return self._repository.recent_predictions(db, limit)

    def latest_prediction(self, db: Session, entity_id: str):
        return self._repository.latest_prediction_for_entity(db, entity_id)

    def high_risks(self, db: Session, limit: int) -> list:
        return self._repository.high_risks(db, limit)

    def search_events(self, query: str, limit: int = 25) -> list[dict]:
        try:
            client = Elasticsearch(settings.elasticsearch_host, request_timeout=2, **elasticsearch_options(settings.elasticsearch_host))
            if not client.ping():
                return []
            response = client.search(
                index="telemetry-events,analytics-events,ml-predictions",
                size=limit,
                query={"query_string": {"query": query, "default_field": "*"}},
                sort=[{"timestamp": {"order": "desc", "unmapped_type": "date"}}],
            )
            return [hit["_source"] for hit in response["hits"]["hits"]]
        except Exception:
            return []

    def dashboard_overview(self, db: Session) -> DashboardOverviewResponse:
        summary = self.summary(db)
        high_risk_count = summary["high_risk_predictions"]
        total_predictions = summary["anomaly_predictions"]
        total_events = summary["telemetry_events"] + summary["analytics_metrics"] + summary["risk_metrics"] + summary["feature_snapshots"]
        return DashboardOverviewResponse(
            total_events=total_events,
            total_predictions=total_predictions,
            anomaly_count=self._repository.anomaly_count(db),
            high_risk_count=high_risk_count,
            average_risk_score=round(self._repository.average_ml_risk(db), 2),
            system_status="operational" if total_events or total_predictions else "waiting_for_streams",
            generated_at=datetime.now(UTC),
        )

    def system_health(self, db: Session) -> SystemHealthResponse:
        components = [
            self._database_health(db),
            self._elasticsearch_health(),
        ]
        summary = self.summary(db)
        components.extend(
            [
                SystemComponentHealth(name="Kafka", status="observing", detail="Storage sink consumes Kafka topics when running."),
                SystemComponentHealth(
                    name="Spark",
                    status="active" if summary["analytics_metrics"] or summary["risk_metrics"] else "waiting",
                    detail="Analytics tables contain Spark outputs." if summary["analytics_metrics"] else "No Spark analytics rows persisted yet.",
                ),
                SystemComponentHealth(
                    name="ML Inference",
                    status="active" if summary["anomaly_predictions"] else "waiting",
                    detail="Prediction records are being persisted." if summary["anomaly_predictions"] else "No ML predictions persisted yet.",
                ),
                SystemComponentHealth(name="Backend", status="online", detail="FastAPI query service is responding."),
            ]
        )
        overall = "operational" if all(item.status in {"online", "active", "observing", "waiting"} for item in components) else "degraded"
        return SystemHealthResponse(status=overall, components=components, generated_at=datetime.now(UTC))

    def risk_trends(self, db: Session) -> list[ChartPoint]:
        buckets: dict[str, list[float]] = defaultdict(list)
        for prediction in reversed(self._repository.recent_predictions_for_charts(db)):
            label = self._bucket_label(prediction.timestamp)
            buckets[label].append(float(prediction.ml_risk_score or 0.0))
        return [ChartPoint(label=label, value=round(sum(values) / max(len(values), 1), 2), secondary_value=len(values)) for label, values in buckets.items()]

    def event_volume(self, db: Session) -> list[ChartPoint]:
        counts: Counter[str] = Counter()
        for event in self._repository.recent_events_for_charts(db):
            counts[self._bucket_label(event.timestamp)] += 1
        return [ChartPoint(label=label, value=value) for label, value in sorted(counts.items())]

    def severity_distribution(self, db: Session) -> list[ChartPoint]:
        return [ChartPoint(label=severity, value=count) for severity, count in self._repository.severity_distribution(db)]

    def event_type_distribution(self, db: Session) -> list[ChartPoint]:
        return [ChartPoint(label=event_type, value=count) for event_type, count in self._repository.event_type_distribution(db)]

    def top_entities(self, db: Session, limit: int = 10) -> list[EntityRiskPoint]:
        return [
            EntityRiskPoint(entity_id=entity_id, risk_score=risk_score, event_count=count, severity=severity)
            for entity_id, risk_score, count, severity in self._repository.top_entities(db, limit)
        ]

    @staticmethod
    def _bucket_label(value: datetime | None) -> str:
        if value is None:
            return "unknown"
        return value.strftime("%H:%M")

    @staticmethod
    def _database_health(db: Session) -> SystemComponentHealth:
        try:
            db.execute(text("SELECT 1"))
            return SystemComponentHealth(name="PostgreSQL", status="online", detail="Structured storage is reachable.")
        except Exception as exc:
            return SystemComponentHealth(name="PostgreSQL", status="offline", detail=str(exc))

    @staticmethod
    def _elasticsearch_health() -> SystemComponentHealth:
        try:
            client = Elasticsearch(settings.elasticsearch_host, request_timeout=2, **elasticsearch_options(settings.elasticsearch_host))
            if client.ping():
                return SystemComponentHealth(name="Elasticsearch", status="online", detail="Search cluster is reachable.")
            return SystemComponentHealth(name="Elasticsearch", status="degraded", detail="Search cluster did not respond to ping.")
        except Exception as exc:
            return SystemComponentHealth(name="Elasticsearch", status="degraded", detail=str(exc))
