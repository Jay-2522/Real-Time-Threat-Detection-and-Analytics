from infrastructure_runtime.security import elasticsearch_options
from pipeline_runtime.search import ensure_indices
import logging

from elasticsearch import Elasticsearch, ConflictError

from app.config.settings import AlertEngineSettings
from app.schemas.alerts import AlertEvent

logger = logging.getLogger(__name__)

INDICES = ("alerts", "incidents")


class AlertIndexer:
    def __init__(self, settings: AlertEngineSettings) -> None:
        self._client = Elasticsearch(settings.elasticsearch_host, request_timeout=3, max_retries=0, **elasticsearch_options(settings.elasticsearch_host))
        self._indices_ready = False

    def _ensure_indices(self) -> None:
        if not self._indices_ready:
            ensure_indices(self._client, {name: self._mapping() for name in INDICES})
            self._indices_ready = True

    def initialize(self) -> None:
        """Best effort at startup; the first write creates the indices if Elasticsearch was not reachable yet."""
        try:
            self._ensure_indices()
            logger.info("alert_elasticsearch_connected")
        except Exception as exc:
            logger.warning("elasticsearch_unavailable action=indices_created_on_first_write reason=%s",
                           (str(exc).strip().splitlines() or [""])[0][:200])

    def index_alert(self, alert: AlertEvent) -> None:
        self._ensure_indices()
        self._client.index(index="alerts", id=alert.alert_id, document=alert.model_dump(mode="json"))

    def index_incident(self, document: dict, version: int) -> None:
        self._ensure_indices()
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

