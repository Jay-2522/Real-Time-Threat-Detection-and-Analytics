from infrastructure_runtime.security import elasticsearch_options
import logging

from elasticsearch import Elasticsearch

from app.config.settings import StorageSinkSettings
from app.schemas.records import StorageRecord
from app.utils.metrics import ELASTICSEARCH_INDEX_ERRORS_TOTAL

logger = logging.getLogger(__name__)


class ElasticsearchIndexer:
    def __init__(self, settings: StorageSinkSettings) -> None:
        self._client = Elasticsearch(settings.elasticsearch_host, request_timeout=3, max_retries=0, **elasticsearch_options(settings.elasticsearch_host))
        self._available = False

    def initialize(self) -> None:
        try:
            self._available = bool(self._client.ping())
            if not self._available:
                logger.warning("elasticsearch_unavailable action=skip_indexing")
                return
            for index_name in ("telemetry-events", "analytics-events", "ml-predictions", "deadletter-events"):
                if not self._client.indices.exists(index=index_name):
                    self._client.indices.create(index=index_name, mappings=self._mapping())
            logger.info("elasticsearch_connected indexes_initialized=true")
        except Exception as exc:
            self._available = False
            logger.warning("elasticsearch_initialization_failed action=postgres_only reason=%s", exc)

    def index(self, record: StorageRecord, key: str) -> None:
        # Always attempt: recovery must not depend on availability at startup.
        try:
            self._client.index(index=self._index_for_record(record), id=key, document=self._document(record))
        except Exception:
            ELASTICSEARCH_INDEX_ERRORS_TOTAL.inc()
            raise

    @staticmethod
    def _index_for_record(record: StorageRecord) -> str:
        if record.record_kind == "prediction":
            return "ml-predictions"
        if record.record_kind == "deadletter":
            return "deadletter-events"
        if record.record_kind in {"analytics", "risk", "feature"}:
            return "analytics-events"
        return "telemetry-events"

    @staticmethod
    def _document(record: StorageRecord) -> dict:
        return {
            "timestamp": record.timestamp_or_created.isoformat(),
            "event_type": record.event_type,
            "entity_id": record.entity_id,
            "source_ip": record.raw_payload.get("source_ip"),
            "risk_score": record.risk_score,
            "ml_risk_score": record.ml_risk_score,
            "severity": record.severity,
            "explanation": record.explanation,
            "source_topic": record.source_topic,
            "raw_payload": record.raw_payload,
        }

    @staticmethod
    def _mapping() -> dict:
        return {
            "properties": {
                "timestamp": {"type": "date"},
                "event_type": {"type": "keyword"},
                "entity_id": {"type": "keyword"},
                "source_ip": {"type": "ip", "ignore_malformed": True},
                "risk_score": {"type": "double"},
                "ml_risk_score": {"type": "double"},
                "severity": {"type": "keyword"},
                "explanation": {"type": "text"},
                "source_topic": {"type": "keyword"},
                "raw_payload": {"type": "object", "enabled": True},
            }
        }
