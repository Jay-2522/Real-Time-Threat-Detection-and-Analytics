from infrastructure_runtime.security import elasticsearch_options
from pipeline_runtime.search import ensure_indices
import logging

from elasticsearch import Elasticsearch

from app.schemas.threat import ThreatEnrichment

logger = logging.getLogger(__name__)

INDICES = ("threat-intel-events", "ioc-matches", "detection-rule-hits", "entity-profiles", "attack-timelines")
MAPPING = {
    "properties": {
        "timestamp": {"type": "date"},
        "event_id": {"type": "keyword"}, "source_topic": {"type": "keyword"}, "source_event_id": {"type": "keyword"},
        "entity_id": {"type": "keyword"}, "entity_type": {"type": "keyword"}, "event_type": {"type": "keyword"},
        "tactic": {"type": "keyword"}, "technique": {"type": "keyword"}, "mitre_id": {"type": "keyword"},
        "kill_chain_stage": {"type": "keyword"}, "geo_country": {"type": "keyword"}, "asn": {"type": "keyword"},
        "severity": {"type": "keyword"}, "mitre_confidence": {"type": "double"},
        "threat_score": {"type": "double"}, "confidence": {"type": "double"},
        "raw_payload": {"type": "object", "enabled": True},
    }
}


class ThreatIndexer:
    def __init__(self, host: str) -> None:
        self._client = Elasticsearch(host, request_timeout=2, max_retries=0, **elasticsearch_options(host))
        self._indices_ready = False

    def _ensure_indices(self) -> None:
        if not self._indices_ready:
            ensure_indices(self._client, {name: MAPPING for name in INDICES})
            self._indices_ready = True

    def index(self, enrichment: ThreatEnrichment) -> None:
        self._ensure_indices()
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
