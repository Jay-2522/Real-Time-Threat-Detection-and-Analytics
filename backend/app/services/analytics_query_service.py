from infrastructure_runtime.security import elasticsearch_options
from datetime import UTC, datetime

from elasticsearch import Elasticsearch
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.settings import settings
from app.repositories.analytics_repository import AnalyticsRepository
from app.schemas.dashboard import ChartPoint, DashboardOverviewResponse, EntityRiskPoint, SystemComponentHealth, SystemHealthResponse
from app.services.pipeline_health import pipeline_stages
from app.services.search_client import run_search


class AnalyticsQueryService:
    def __init__(self, repository: AnalyticsRepository | None = None) -> None:
        self._repository = repository or AnalyticsRepository()

    def summary(self, db: Session) -> dict[str, int]:
        return self._repository.summary(db)

    def recent_events(self, db: Session, limit: int, offset: int = 0) -> list:
        return self._repository.recent_events(db, limit, offset)

    def recent_predictions(self, db: Session, limit: int, offset: int = 0) -> list:
        return self._repository.recent_predictions(db, limit, offset)

    def latest_prediction(self, db: Session, entity_id: str):
        return self._repository.latest_prediction_for_entity(db, entity_id)

    def high_risks(self, db: Session, limit: int, offset: int = 0) -> list:
        return self._repository.high_risks(db, limit, offset)

    def search_events(self, query: str, limit: int = 25) -> list[dict]:
        return run_search("telemetry-events,analytics-events,ml-predictions", query, limit, newest_first=True)

    def dashboard_overview(self, db: Session) -> DashboardOverviewResponse:
        summary = self.summary(db)
        high_risk_count = summary["high_risk_predictions"]
        total_predictions = summary["anomaly_predictions"]
        # total_events is raw telemetry, the same number the monitoring and analytics endpoints report;
        # total_records adds the records Spark derives from it.
        total_events = summary["telemetry_events"]
        total_records = total_events + summary["analytics_metrics"] + summary["risk_metrics"] + summary["feature_snapshots"]
        return DashboardOverviewResponse(
            total_events=total_events,
            total_records=total_records,
            total_predictions=total_predictions,
            anomaly_count=self._repository.anomaly_count(db),
            high_risk_count=high_risk_count,
            average_risk_score=round(self._repository.average_ml_risk(db), 2),
            system_status="operational" if total_records or total_predictions else "waiting_for_streams",
            generated_at=datetime.now(UTC),
        )

    def system_health(self, db: Session) -> SystemHealthResponse:
        database = self._database_health(db)
        components = [database, self._elasticsearch_health()]
        if database.status == "online":
            components.extend(SystemComponentHealth(name=stage.name, status=stage.status, detail=stage.detail)
                              for stage in pipeline_stages(db))
        else:  # the stage status is read from the database, so it cannot be judged
            components.extend(SystemComponentHealth(name=name, status="degraded", detail="Cannot be judged: the database is unavailable.")
                              for name in ("Kafka", "Spark", "ML Inference"))
        components.append(SystemComponentHealth(name="Backend", status="online", detail="FastAPI query service is responding."))
        overall = "operational" if all(item.status in {"online", "active", "idle", "waiting"} for item in components) else "degraded"
        return SystemHealthResponse(status=overall, components=components, generated_at=datetime.now(UTC))

    def risk_trends(self, db: Session) -> list[ChartPoint]:
        return [ChartPoint(label=label, value=round(average, 2), secondary_value=count)
                for label, average, count in self._repository.risk_trend(db, datetime.now(UTC))]

    def event_volume(self, db: Session) -> list[ChartPoint]:
        return [ChartPoint(label=label, value=count) for label, count in self._repository.event_volume(db, datetime.now(UTC))]

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
    def _database_health(db: Session) -> SystemComponentHealth:
        try:
            db.execute(text("SELECT 1"))
            return SystemComponentHealth(name="PostgreSQL", status="online", detail="Structured storage is reachable.")
        except Exception as exc:
            return SystemComponentHealth(name="PostgreSQL", status="offline", detail=str(exc))

    @staticmethod
    def _elasticsearch_health() -> SystemComponentHealth:
        try:
            client = Elasticsearch(settings.elasticsearch_host, request_timeout=2, max_retries=0, **elasticsearch_options(settings.elasticsearch_host))
            if client.ping():
                return SystemComponentHealth(name="Elasticsearch", status="online", detail="Search cluster is reachable.")
            return SystemComponentHealth(name="Elasticsearch", status="degraded", detail="Search cluster did not respond to ping.")
        except Exception as exc:
            return SystemComponentHealth(name="Elasticsearch", status="degraded", detail=str(exc))
