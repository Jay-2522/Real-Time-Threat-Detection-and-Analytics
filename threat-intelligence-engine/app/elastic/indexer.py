from infrastructure_runtime.security import elasticsearch_options
import logging

from elasticsearch import Elasticsearch

from app.schemas.threat import ThreatEnrichment

logger = logging.getLogger(__name__)


class ThreatIndexer:
    def __init__(self, host: str) -> None:
        self._client = Elasticsearch(host, request_timeout=2, max_retries=0, **elasticsearch_options(host))
    def index(self, enrichment: ThreatEnrichment) -> None:
        document = enrichment.model_dump(mode="json")
        indexes = ["threat-intel-events"]
        if enrichment.iocs:
            indexes.append("ioc-matches")
        if enrichment.rule_hits:
            indexes.append("detection-rule-hits")
        if enrichment.entity_id:
            indexes.extend(["entity-profiles", "attack-timelines"])
        for index in indexes:
            # Raise to retain the SQL outbox row; retries overwrite deterministic IDs.
            self._client.index(index=index, id=f"{enrichment.event_id}:{index}", document=document)
