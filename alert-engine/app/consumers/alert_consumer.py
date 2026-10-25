from infrastructure_runtime.security import kafka_options
from pipeline_runtime.durable import OutboxWorker, quarantine_record, IdentityConflict
from pipeline_runtime.identity import quarantine
from app.schemas.alerts import AlertEvent
from pipeline_runtime.kafka import CONSUMER_OPTIONS, DeliveryLoop
from pipeline_runtime.identity import coordinate
import json
import logging
import signal
from types import FrameType

from confluent_kafka import Consumer
from sqlalchemy.orm import sessionmaker

from app.config.settings import AlertEngineSettings
from app.config.topics import ALERT_INPUT_TOPICS
from app.services.alert_service import AlertProcessingService
from app.utils.retry import retry_with_backoff
from app.utils.metrics import ALERT_ENGINE_ERRORS_TOTAL

logger = logging.getLogger(__name__)


class AlertEngineConsumer:
    def __init__(self, settings: AlertEngineSettings, session_factory: sessionmaker, service: AlertProcessingService) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._service = service
        self._consumer: Consumer | None = None
        self._running = True
        self._outbox = OutboxWorker(session_factory, "alert", {
            "elasticsearch": self._index_projection,
        })

    def _index_projection(self, key, payload):
        from app.db.models import Incident
        from datetime import datetime
        alert = AlertEvent.model_validate(payload)
        self._service._indexer.index_alert(alert)
        if alert.incident_id:
            with self._session_factory() as session:
                row = session.query(Incident).filter_by(incident_id=alert.incident_id).one()
                document = {column.name: getattr(row, column.name) for column in Incident.__table__.columns
                            if column.name != 'id'}
                document = {k: v.isoformat() if isinstance(v, datetime) else v for k,v in document.items()}
                version = int(row.updated_at.timestamp()*1_000_000)
            self._service._indexer.index_incident(document, version)

    def run(self) -> None:
        self._register_shutdown_handlers()
        self._consumer = retry_with_backoff(self._create_consumer, logger, "Kafka alert-engine consumer connection")
        logger.info("subscribing_alert_engine topics=%s group=%s", ALERT_INPUT_TOPICS, self._settings.consumer_group)
        self._outbox.start()
        try:
            DeliveryLoop(self._consumer, "alert").run(ALERT_INPUT_TOPICS,
                lambda message: self._handle_message(message.topic(), message.value(), coordinate(message)),
                lambda: self._running)
        finally:
            self._outbox.stop()

    def _create_consumer(self) -> Consumer:
        consumer = Consumer(
            {
                **kafka_options(),
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._settings.consumer_group,
                "client.id": "streaming-analytics-alert-engine",
                "auto.offset.reset": "earliest",
                **CONSUMER_OPTIONS,
            }
        )
        metadata = consumer.list_topics(timeout=10)
        logger.info("alert_engine_kafka_connected brokers=%s", len(metadata.brokers))
        return consumer

    def _handle_message(self, topic: str, payload: bytes | None, position=None) -> None:
        from pipeline_runtime.identity import logical_key, digest
        try:
            if payload is None:
                raise ValueError("Empty alert input")
            parsed = json.loads(payload.decode("utf-8"))
            logical_key(topic, parsed)
            digest(parsed)
            # Classify schema/rule errors before opening a transaction.
            self._service._rules.evaluate(topic, parsed)
        except (UnicodeError, ValueError, TypeError, KeyError) as exc:
            with self._session_factory() as session, session.begin():
                quarantine_record(session, "alert", quarantine(topic, payload, exc, position))
            ALERT_ENGINE_ERRORS_TOTAL.inc()
            logger.warning("alert_input_quarantined position=%s reason=%s", position, exc)
            return
        try:
            with self._session_factory() as session, session.begin():
                self._service.process(session, topic, parsed)
        except IdentityConflict as exc:
            with self._session_factory() as session, session.begin():
                quarantine_record(session, "alert", quarantine(topic, payload, exc, position))
            ALERT_ENGINE_ERRORS_TOTAL.inc()
            logger.error("alert_identity_conflict position=%s reason=%s", position, exc)
        except Exception:
            ALERT_ENGINE_ERRORS_TOTAL.inc()
            raise

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("received_shutdown_signal signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
