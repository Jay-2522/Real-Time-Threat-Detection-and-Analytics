from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.repositories.alert_repository import AlertRepository
from app.schemas.alerts import AlertStatsResponse
from app.services.search_client import run_search


class AlertQueryService:
    def __init__(self, repository: AlertRepository | None = None) -> None:
        self._repository = repository or AlertRepository()

    def recent_alerts(self, db: Session, limit: int) -> list:
        return self._repository.recent_alerts(db, limit)

    def high_alerts(self, db: Session, limit: int) -> list:
        return self._repository.alerts_by_severity(db, ["high", "critical"], limit)

    def critical_alerts(self, db: Session, limit: int) -> list:
        return self._repository.alerts_by_severity(db, ["critical"], limit)

    def recent_incidents(self, db: Session, limit: int) -> list:
        return self._repository.recent_incidents(db, limit)

    def open_incidents(self, db: Session, limit: int) -> list:
        return self._repository.open_incidents(db, limit)

    def incident(self, db: Session, incident_id: str):
        return self._repository.incident(db, incident_id)

    def stats(self, db: Session) -> AlertStatsResponse:
        return AlertStatsResponse(**self._repository.stats(db))

    def update_alert_status(self, db: Session, alert_id: str, status: str):
        return self._repository.update_alert_status(db, alert_id, status)

    def update_incident_status(self, db: Session, incident_id: str, status: str, notes: str | None):
        return self._repository.update_incident_status(db, incident_id, status, notes)

    def search(self, query: str, limit: int = 25) -> list[dict]:
        return run_search("alerts,incidents", query, limit, newest_first=True)

    def live_message(self, db: Session):
        recent = self.recent_alerts(db, 50)
        critical = self.critical_alerts(db, 10)
        incidents = self.recent_incidents(db, 10)
        stats = self.stats(db)
        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "recent_alerts": [self._alert_payload(alert) for alert in recent],
            "critical_alerts": [self._alert_payload(alert) for alert in critical],
            "recent_incidents": [self._incident_payload(incident) for incident in incidents],
            "severity_counters": stats.severity_counts,
            "stats": stats.model_dump(),
        }

    @staticmethod
    def _alert_payload(alert) -> dict:
        return {
            "alert_id": alert.alert_id,
            "timestamp": alert.timestamp.isoformat(),
            "created_at": alert.created_at.isoformat(),
            "severity": alert.severity,
            "title": alert.title,
            "description": alert.description,
            "source_event_id": alert.source_event_id,
            "entity_id": alert.entity_id,
            "event_type": alert.event_type,
            "source_topic": alert.source_topic,
            "anomaly_score": alert.anomaly_score,
            "status": alert.status,
            "ml_risk_score": alert.ml_risk_score,
            "correlation_id": alert.correlation_id,
            "explanation": alert.explanation,
            "recommended_action": alert.recommended_action,
            "incident_id": alert.incident_id,
            "tags": alert.tags,
        }

    @staticmethod
    def _incident_payload(incident) -> dict:
        return {
            "incident_id": incident.incident_id,
            "updated_at": incident.updated_at.isoformat(),
            "severity": incident.severity,
            "title": incident.title,
            "status": incident.status,
            "event_count": incident.event_count,
            "entity_ids": incident.entity_ids,
            "escalation_level": incident.escalation_level,
        }

