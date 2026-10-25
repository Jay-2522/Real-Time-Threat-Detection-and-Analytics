from __future__ import annotations
from infrastructure_runtime.security import elasticsearch_options

import json
import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import base64
from infrastructure_runtime.security import section

from elasticsearch import Elasticsearch
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.settings import Settings
from app.db.models import Alert, AnalyticsMetric, AnomalyPrediction, DeadLetterEvent, FeatureSnapshot, Incident, RiskMetric, TelemetryEvent
from app.schemas.monitoring import ErrorSummary, MetricsSummary, MonitoringOverview, PipelineMetrics, ServiceHealth

logger = logging.getLogger(__name__)


class MonitoringService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def overview(self, db: Session) -> MonitoringOverview:
        services = self.services(db)
        counts = self._pipeline_counts(db)
        healthy = sum(1 for service in services if service.status == "healthy")
        down = sum(1 for service in services if service.status == "down")
        degraded = sum(1 for service in services if service.status == "degraded")
        status = "healthy" if down == 0 and degraded == 0 else "degraded" if down == 0 else "impaired"
        return MonitoringOverview(
            status=status,
            services_total=len(services),
            services_healthy=healthy,
            services_degraded=degraded,
            services_down=down,
            total_events=counts["telemetry_events"],
            total_predictions=counts["anomaly_predictions"],
            total_alerts=counts["alerts"],
            open_incidents=self._count(db, Incident, Incident.status.in_(["open", "investigating"])),
            generated_at=self._now(),
            prometheus_url=self._settings.prometheus_url,
            grafana_url=self._settings.grafana_url,
        )

    def services(self, db: Session) -> list[ServiceHealth]:
        now = self._now()
        prometheus_targets = self._prometheus_targets()
        db_status = self._postgres_status(db, now)
        elastic_status = self._elasticsearch_status(now)
        service_specs = [
            ("backend", "Backend API", "api", "http://localhost:8000/metrics"),
            ("ml-inference", "ML Inference", "ml", "http://localhost:9101/metrics"),
            ("storage-sink", "Storage Sink", "storage", "http://localhost:9102/metrics"),
            ("alert-engine", "Alert Engine", "security", "http://localhost:9103/metrics"),
            ("telemetry-producer", "Kafka Producer", "streaming", "http://localhost:9104/metrics"),
            ("threat-intelligence-engine", "Threat Intelligence Engine", "security", "http://localhost:9106/metrics"),
            ("prometheus", "Prometheus", "observability", "http://localhost:9090"),
        ]
        services = [self._service_from_target(job, name, category, endpoint, prometheus_targets, now) for job, name, category, endpoint in service_specs]
        services.extend(
            [
                db_status,
                elastic_status,
                ServiceHealth(name="Kafka", status="observing", detail="Kafka is monitored indirectly through pipeline throughput and service consumers.", category="streaming", endpoint="localhost:9092", last_checked=now),
                self._spark_status(prometheus_targets, now),
                ServiceHealth(name="Frontend", status="healthy", detail="React dashboard is served locally by the frontend container.", category="ui", endpoint="http://localhost:5173", last_checked=now),
            ]
        )
        return services

    def _spark_status(self, targets, now):
        active = self._prometheus_scalar('sum(spark_streaming_query_active)')
        age = self._prometheus_scalar('time() - max(spark_query_monitor_timestamp_seconds)')
        ready = targets.get('spark-driver') == 1 and active == 4 and age is not None and 0 <= age < 60
        return ServiceHealth(name="Spark", status="healthy" if ready else "degraded",
            detail="All four queries are active with a fresh authenticated observation." if ready
                   else "Four active Spark queries have not been verified; analytics processing is degraded.",
            category="processing", endpoint=None, last_checked=now)

    def pipeline(self, db: Session) -> PipelineMetrics:
        counts = self._pipeline_counts(db)
        return PipelineMetrics(generated_at=self._now(), **counts)

    def errors(self, db: Session) -> ErrorSummary:
        services = self.services(db)
        return ErrorSummary(
            deadletter_events=self._count(db, DeadLetterEvent),
            critical_alerts=self._count(db, Alert, Alert.severity == "critical"),
            failed_services=sum(1 for service in services if service.status == "down"),
            degraded_services=sum(1 for service in services if service.status == "degraded"),
            recent_errors=[],
            generated_at=self._now(),
        )

    def metrics_summary(self) -> MetricsSummary:
        queries = {
            "backend_request_rate": "sum(rate(http_requests_total[1m]))",
            "ml_predictions_total": "sum(ml_predictions_total)",
            "ml_anomalies_total": "sum(ml_anomalies_total)",
            "records_persisted_total": "sum(records_persisted_total)",
            "alerts_generated_total": "sum(alerts_generated_total)",
            "critical_alerts_total": "sum(critical_alerts_total)",
            "threat_events_processed_total": "sum(threat_events_processed_total)",
            "ioc_enrichments_total": "sum(ioc_enrichments_total)",
            "detection_rule_hits_total": "sum(detection_rule_hits_total)",
        }
        key_metrics: dict[str, float] = {}
        available = True
        for name, query in queries.items():
            value = self._prometheus_scalar(query)
            if value is None:
                available = False
                key_metrics[name] = 0.0
            else:
                key_metrics[name] = value

        targets = self._prometheus_targets()
        if not targets:
            available = False
        return MetricsSummary(
            prometheus_available=available,
            scraped_targets_up=sum(1 for target in targets.values() if target == 1),
            scraped_targets_down=sum(1 for target in targets.values() if target == 0),
            key_metrics=key_metrics,
            generated_at=self._now(),
        )

    def _service_from_target(
        self,
        job: str,
        name: str,
        category: str,
        endpoint: str,
        targets: dict[str, int],
        checked_at: datetime,
    ) -> ServiceHealth:
        if job not in targets:
            return ServiceHealth(name=name, status="degraded", detail="Prometheus has not reported this target yet.", category=category, endpoint=endpoint, last_checked=checked_at)
        if targets[job] == 1:
            return ServiceHealth(name=name, status="healthy", detail="Prometheus scrape target is UP.", category=category, endpoint=endpoint, last_checked=checked_at)
        return ServiceHealth(name=name, status="down", detail="Prometheus scrape target is DOWN.", category=category, endpoint=endpoint, last_checked=checked_at)

    def _postgres_status(self, db: Session, checked_at: datetime) -> ServiceHealth:
        try:
            db.execute(text("SELECT 1"))
            return ServiceHealth(name="PostgreSQL", status="healthy", detail="Database connection check succeeded.", category="storage", endpoint="localhost:5432", last_checked=checked_at)
        except SQLAlchemyError as exc:
            logger.warning("postgres_health_check_failed error=%s", exc)
            return ServiceHealth(name="PostgreSQL", status="down", detail="Database connection check failed.", category="storage", endpoint="localhost:5432", last_checked=checked_at)

    def _elasticsearch_status(self, checked_at: datetime) -> ServiceHealth:
        try:
            client = Elasticsearch(self._settings.elasticsearch_host, request_timeout=1, **elasticsearch_options(self._settings.elasticsearch_host))
            if client.ping():
                return ServiceHealth(name="Elasticsearch", status="healthy", detail="Elasticsearch ping succeeded.", category="search", endpoint="http://localhost:9200", last_checked=checked_at)
            return ServiceHealth(name="Elasticsearch", status="degraded", detail="Elasticsearch ping returned false.", category="search", endpoint="http://localhost:9200", last_checked=checked_at)
        except Exception as exc:
            logger.warning("elasticsearch_health_check_failed error=%s", exc)
            return ServiceHealth(name="Elasticsearch", status="degraded", detail="Elasticsearch is unavailable; search monitoring is degraded.", category="search", endpoint="http://localhost:9200", last_checked=checked_at)

    def _pipeline_counts(self, db: Session) -> dict[str, int]:
        return {
            "telemetry_events": self._count(db, TelemetryEvent),
            "analytics_metrics": self._count(db, AnalyticsMetric),
            "risk_metrics": self._count(db, RiskMetric),
            "feature_snapshots": self._count(db, FeatureSnapshot),
            "anomaly_predictions": self._count(db, AnomalyPrediction),
            "alerts": self._count(db, Alert),
            "incidents": self._count(db, Incident),
            "deadletter_events": self._count(db, DeadLetterEvent),
        }

    def _count(self, db: Session, model: Any, *filters: Any) -> int:
        try:
            statement = select(func.count()).select_from(model)
            for condition in filters:
                statement = statement.where(condition)
            return int(db.scalar(statement) or 0)
        except SQLAlchemyError as exc:
            logger.warning("monitoring_count_failed table=%s error=%s", getattr(model, "__tablename__", model), exc)
            return 0

    def _prometheus_targets(self) -> dict[str, int]:
        result = self._prometheus_query("up")
        targets: dict[str, int] = {}
        if not result:
            return targets
        for series in result:
            metric = series.get("metric", {})
            value = series.get("value", [None, "0"])
            job = str(metric.get("job", "unknown"))
            try:
                targets[job] = max(targets.get(job, 0), int(float(value[1])))
            except (TypeError, ValueError):
                targets[job] = 0
        return targets

    def _prometheus_scalar(self, query: str) -> float | None:
        result = self._prometheus_query(query)
        if not result:
            return None
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    def _prometheus_query(self, query: str) -> list[dict[str, Any]]:
        url = f"{self._settings.prometheus_url.rstrip('/')}/api/v1/query?{urlencode({'query': query})}"
        try:
            request = Request(url)
            credentials = section('prometheus')
            if credentials:
                encoded = base64.b64encode((credentials['username'] + ':' + credentials['password']).encode()).decode()
                request.add_header('Authorization', 'Basic ' + encoded)
            with urlopen(request, timeout=1.5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            logger.info("prometheus_query_unavailable query=%s error=%s", query, exc)
            return []
        if payload.get("status") != "success":
            return []
        return payload.get("data", {}).get("result", [])

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)
