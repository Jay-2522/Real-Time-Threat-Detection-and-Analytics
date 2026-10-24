from pipeline_runtime.durable import claim, enqueue
from pipeline_runtime.identity import logical_key, stable_uuid, digest
from sqlalchemy import text, update
from app.db.models import Alert
import logging

from sqlalchemy.orm import Session

from app.elastic.indexer import AlertIndexer
from app.incidents.correlator import IncidentCorrelator
from app.repositories.alert_repository import AlertRepository
from app.rules.rule_engine import AlertRulesEngine
from app.utils.metrics import ALERTS_GENERATED_TOTAL, ALERT_DEDUPLICATIONS_TOTAL, CRITICAL_ALERTS_TOTAL

logger = logging.getLogger(__name__)


class AlertProcessingService:
    def __init__(
        self,
        rules: AlertRulesEngine,
        correlator: IncidentCorrelator,
        repository: AlertRepository,
        indexer: AlertIndexer,
        dedup_cooldown_seconds: int,
    ) -> None:
        self._rules = rules
        self._correlator = correlator
        self._repository = repository
        self._indexer = indexer
        self._dedup_cooldown_seconds = dedup_cooldown_seconds

    def process(self, session: Session, source_topic: str, payload: dict) -> int:
        input_key = logical_key(source_topic, payload)
        if not claim(session, "alert", input_key, payload):
            return 0
        emitted = 0
        for alert in sorted(self._rules.evaluate(source_topic, payload), key=lambda alert: alert.correlation_id):
            alert.alert_id = str(stable_uuid("alert-rules-v1", input_key, alert.correlation_id, alert.severity))
            # Serialize distinct inputs sharing a correlation key, including the no-incident-yet case.
            session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": alert.correlation_id})
            if not self._repository.should_emit(session, alert, self._dedup_cooldown_seconds):
                ALERT_DEDUPLICATIONS_TOTAL.inc()
                logger.info("alert_deduplicated key=%s", alert.correlation_id)
                continue
            self._repository.persist_alert(session, alert)
            incident_id = self._correlator.correlate(session, alert)
            alert.incident_id = incident_id
            session.execute(update(Alert).where(Alert.alert_id == alert.alert_id).values(incident_id=incident_id))
            body = alert.model_dump(mode="json")
            enqueue(session, "alert", digest(alert.alert_id), "elasticsearch", body)
            emitted += 1
            ALERTS_GENERATED_TOTAL.labels(alert.severity).inc()
            if alert.severity == "critical":
                CRITICAL_ALERTS_TOTAL.inc()
            logger.info(
                "alert_generated alert_id=%s severity=%s entity_id=%s incident_id=%s",
                alert.alert_id,
                alert.severity,
                alert.entity_id,
                incident_id,
            )
        return emitted
