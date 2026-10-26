from infrastructure_runtime.security import kafka_options
from pipeline_runtime.durable import claim, claim_many, enqueue, enqueue_many, OutboxWorker, IdentityConflict
from pipeline_runtime.identity import logical_key, quarantine
from app.schemas.records import StorageRecord
from pipeline_runtime.kafka import CONSUMER_OPTIONS, DeliveryLoop, publish_confirmed
from pipeline_runtime.identity import coordinate
import json
import logging
import signal
from types import FrameType

from confluent_kafka import Consumer, Producer
from sqlalchemy.orm import sessionmaker

from app.config.settings import StorageSinkSettings
from app.config.topics import STORAGE_INPUT_TOPICS, TELEMETRY_DEADLETTER_EVENTS
from app.elastic.indexer import ElasticsearchIndexer
from app.repositories.postgres_repository import PostgresStorageRepository
from app.services.record_mapper import map_payload_to_record
from app.utils.retry import retry_with_backoff
from app.utils.metrics import POSTGRES_WRITE_ERRORS_TOTAL, RECORDS_PERSISTED_TOTAL

logger = logging.getLogger(__name__)


class StorageSinkConsumer:
    def __init__(
        self,
        settings: StorageSinkSettings,
        session_factory: sessionmaker,
        repository: PostgresStorageRepository,
        indexer: ElasticsearchIndexer,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._repository = repository
        self._indexer = indexer
        self._consumer: Consumer | None = None
        self._deadletter_producer: Producer | None = None
        self._running = True
        self._outbox = OutboxWorker(session_factory, "storage", {
            "elasticsearch": lambda key, payload: self._indexer.index(StorageRecord.model_validate(payload), key),
        })

    def run(self) -> None:
        self._register_shutdown_handlers()
        self._consumer = retry_with_backoff(self._create_consumer, logger, "Kafka storage sink consumer connection")
        self._deadletter_producer = retry_with_backoff(self._create_deadletter_producer, logger, "Kafka storage sink dead-letter producer connection")
        logger.info("subscribing_storage_sink topics=%s group=%s", STORAGE_INPUT_TOPICS, self._settings.kafka_consumer_group)
        self._outbox.start()
        try:
            DeliveryLoop(self._consumer, "storage").run_batch(
                STORAGE_INPUT_TOPICS, self._handle_batch, lambda: self._running,
                batch_size=self._settings.batch_size,
                batch_wait_seconds=self._settings.batch_wait_ms / 1000,
            )
        finally:
            self._outbox.stop()
            self._deadletter_producer.flush(10)

    def _create_consumer(self) -> Consumer:
        consumer = Consumer(
            {
                **kafka_options(),
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._settings.kafka_consumer_group,
                "client.id": self._settings.consumer_client_id,
                "auto.offset.reset": "earliest",
                **CONSUMER_OPTIONS,
            }
        )
        metadata = consumer.list_topics(timeout=10)
        logger.info("kafka_connected brokers=%s", len(metadata.brokers))
        return consumer

    def _create_deadletter_producer(self) -> Producer:
        producer = Producer(
            {
                **kafka_options(),
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "client.id": f"{self._settings.consumer_client_id}-deadletter",
                "acks": "all",
                "enable.idempotence": True,
                "delivery.timeout.ms": 10000,
                "retries": 5,
            }
        )
        producer.list_topics(timeout=10)
        return producer

    def _publish_deadletter(self, body: dict) -> None:
        assert self._deadletter_producer is not None
        publish_confirmed(
            self._deadletter_producer,
            topic=self._settings.deadletter_topic,
            key=body["event_id"],
            value=json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )

    def _persist(self, record, key):
        # Persist the fallback once; retries/reconstruction must not invent a new time.
        if record.timestamp is None:
            record.timestamp = record.timestamp_or_created
        with self._session_factory() as session, session.begin():
            if not claim(session, "storage", key, record.raw_payload):
                return
            self._repository.persist(session, record)
            body = record.model_dump(mode="json")
            enqueue(session, "storage", key, "elasticsearch", body)
        RECORDS_PERSISTED_TOTAL.labels(record.record_kind).inc()

    def _handle_message(self, topic: str, payload: bytes | None, position=None) -> None:
        try:
            if payload is None:
                raise ValueError("Kafka message payload is empty")
            parsed = json.loads(payload.decode("utf-8"))
            key = logical_key(topic, parsed)
            record = map_payload_to_record(topic, parsed)
            # Validate canonical JSON before opening the database transaction.
            from pipeline_runtime.identity import digest
            digest(parsed)
        except (UnicodeError, ValueError, TypeError, KeyError) as exc:
            body = quarantine(topic, payload, exc, position)
            if topic != TELEMETRY_DEADLETTER_EVENTS:
                self._publish_deadletter(body)
            record = map_payload_to_record(TELEMETRY_DEADLETTER_EVENTS, body)
            key = logical_key(TELEMETRY_DEADLETTER_EVENTS, body)
        try:
            self._persist(record, key)
        except IdentityConflict as exc:
            logger.error("storage_identity_conflict position=%s error=%s", position, exc)
            body = quarantine(topic, payload, exc, position)
            if topic != TELEMETRY_DEADLETTER_EVENTS:
                self._publish_deadletter(body)
            self._persist(map_payload_to_record(TELEMETRY_DEADLETTER_EVENTS, body),
                          logical_key(TELEMETRY_DEADLETTER_EVENTS, body))
        except Exception:
            POSTGRES_WRITE_ERRORS_TOTAL.inc()
            raise

    def _prepare_message(self, message):
        topic = message.topic()
        payload = message.value()
        position = coordinate(message)
        try:
            if payload is None:
                raise ValueError("Kafka message payload is empty")
            parsed = json.loads(payload.decode("utf-8"))
            key = logical_key(topic, parsed)
            record = map_payload_to_record(topic, parsed)
            from pipeline_runtime.identity import digest
            digest(parsed)
        except (UnicodeError, ValueError, TypeError, KeyError) as exc:
            body = quarantine(topic, payload, exc, position)
            if topic != TELEMETRY_DEADLETTER_EVENTS:
                self._publish_deadletter(body)
            record = map_payload_to_record(TELEMETRY_DEADLETTER_EVENTS, body)
            key = logical_key(TELEMETRY_DEADLETTER_EVENTS, body)
        return message, record, key

    def _persist_batch(self, prepared):
        from pipeline_runtime.identity import digest
        unique = {}
        conflicts = []
        for item in prepared:
            _message, record, key = item
            if record.timestamp is None:
                record.timestamp = record.timestamp_or_created
            existing = unique.get(key)
            if existing is None:
                unique[key] = item
            elif digest(existing[1].raw_payload) != digest(record.raw_payload):
                conflicts.append(item)
        with self._session_factory() as session, session.begin():
            claims = claim_many(session, "storage", {
                key: item[1].raw_payload for key, item in unique.items()
            })
            new_items = [item for key, item in unique.items() if key in claims.inserted]
            self._repository.persist_many(session, [item[1] for item in new_items])
            enqueue_many(session, "storage", "elasticsearch", {
                item[2]: item[1].model_dump(mode="json") for item in new_items
            })
        conflicts.extend(unique[key] for key in claims.conflicts)
        for _message, record, _key in new_items:
            RECORDS_PERSISTED_TOTAL.labels(record.record_kind).inc()
        return conflicts

    def _handle_batch(self, messages) -> None:
        prepared = [self._prepare_message(message) for message in messages]
        try:
            conflicts = self._persist_batch(prepared)
            if conflicts:
                deadletters = []
                for message, _record, key in conflicts:
                    exc = IdentityConflict(f"Conflicting payload for stable identity {key}")
                    position = coordinate(message)
                    logger.error("storage_identity_conflict position=%s error=%s", position, exc)
                    body = quarantine(message.topic(), message.value(), exc, position)
                    if message.topic() != TELEMETRY_DEADLETTER_EVENTS:
                        self._publish_deadletter(body)
                    deadletters.append((
                        message,
                        map_payload_to_record(TELEMETRY_DEADLETTER_EVENTS, body),
                        logical_key(TELEMETRY_DEADLETTER_EVENTS, body),
                    ))
                if self._persist_batch(deadletters):
                    raise IdentityConflict("Conflicting dead-letter identity")
        except Exception:
            POSTGRES_WRITE_ERRORS_TOTAL.inc()
            raise

    def _register_shutdown_handlers(self) -> None:
        def shutdown_handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("received_shutdown_signal signal=%s", signum)
            self._running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
