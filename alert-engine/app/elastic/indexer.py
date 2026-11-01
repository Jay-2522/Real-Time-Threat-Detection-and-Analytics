from infrastructure_runtime.security import elasticsearch_options
import logging

from elasticsearch import Elasticsearch, ConflictError

from app.config.settings import AlertEngineSettings
from app.schemas.alerts import AlertEvent

logger = logging.getLogger(__name__)


class AlertIndexer:
    def __init__(self, settings: AlertEngineSettings) -> None:
        self._client = Elasticsearch(settings.elasticsearch_host, request_timeout=3, max_retries=0, **elasticsearch_options(settings.elasticsearch_host))
        self._available = False

    def initialize(self) -> None:
        try:
            self._available = bool(self._client.ping())
            if not self._available:
                logger.warning("elasticsearch_unavailable action=skip_alert_indexing")
                return
            for index_name in ("alerts", "incidents"):
                if not self._client.indices.exists(index=index_name):
                    self._client.indices.create(index=index_name, mappings=self._mapping())
            logger.info("alert_elasticsearch_connected")
        except Exception as exc:
            self._available = False
            logger.warning("alert_elasticsearch_initialization_failed reason=%s", exc)

    def index_alert(self, alert: AlertEvent) -> None:
        self._client.index(index="alerts", id=alert.alert_id, document=alert.model_dump(mode="json"))

    def index_incident(self, document: dict, version: int) -> None:
        try:
            self._client.index(index="incidents", id=document['incident_id'], document=document,
                               version=version, version_type="external_gte")
        except ConflictError:
            # A newer authoritative SQL snapshot already won an out-of-order retry.
            pass

    @staticmethod
    def _mapping() -> dict:
        return {
            "properties": {
                "timestamp": {"type": "date"},
                "severity": {"type": "keyword"},
                "entity_id": {"type": "keyword"},
                "status": {"type": "keyword"},
                "event_type": {"type": "keyword"},
                "title": {"type": "text"},
                "description": {"type": "text"},
                "explanation": {"type": "text"},
                "recommended_action": {"type": "text"},
                "tags": {"type": "keyword"},
            }
        }

