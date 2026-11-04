from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import AttackTimeline, DetectionRuleCatalog, DetectionRuleHit, EntityProfile, IOCMatch, ThreatIntelEvent
from app.schemas.threat import ThreatEnrichment


class ThreatRepository:
    def sync_rule_catalog(self, session: Session, catalog: list[dict]) -> None:
        """Make the catalog table mirror the rules this engine just loaded: upsert them, drop the ones removed."""
        now = datetime.now(UTC)
        if catalog:
            statement = insert(DetectionRuleCatalog).values([{**rule, "loaded_at": now} for rule in catalog])
            session.execute(statement.on_conflict_do_update(
                index_elements=[DetectionRuleCatalog.rule_id],
                set_={column: getattr(statement.excluded, column) for column in
                      ("name", "severity", "enabled", "tactic", "technique", "mitre_id", "description", "loaded_at")}))
        session.execute(delete(DetectionRuleCatalog).where(DetectionRuleCatalog.rule_id.not_in([rule["rule_id"] for rule in catalog])))

    def persist(self, session: Session, enrichment: ThreatEnrichment) -> None:
        session.merge(
            ThreatIntelEvent(
                event_id=enrichment.event_id,
                timestamp=enrichment.timestamp,
                source_topic=enrichment.source_topic,
                entity_id=enrichment.entity_id,
                event_type=enrichment.event_type,
                tactic=enrichment.tactic,
                technique=enrichment.technique,
                mitre_id=enrichment.mitre_id,
                kill_chain_stage=enrichment.kill_chain_stage,
                threat_score=enrichment.threat_score,
                confidence=enrichment.confidence,
                severity=enrichment.severity,
                geo_country=enrichment.geo_country,
                asn=enrichment.asn,
                iocs=[ioc.model_dump() for ioc in enrichment.iocs],
                rule_hits=[hit.model_dump() for hit in enrichment.rule_hits],
                raw_payload=enrichment.raw_payload,
                created_at=datetime.now(UTC),
            )
        )
        for ioc in enrichment.iocs:
            session.add(
                IOCMatch(
                    ioc_value=ioc.value,
                    ioc_type=ioc.ioc_type,
                    entity_id=enrichment.entity_id,
                    source_event_id=enrichment.source_event_id,
                    source_topic=enrichment.source_topic,
                    confidence=ioc.confidence,
                    severity=ioc.severity,
                    feed_name=ioc.feed_name,
                    description=ioc.description,
                    timestamp=enrichment.timestamp,
                    raw_payload=enrichment.raw_payload,
                )
            )
        for hit in enrichment.rule_hits:
            session.add(
                DetectionRuleHit(
                    rule_id=hit.rule_id,
                    rule_name=hit.rule_name,
                    severity=hit.severity,
                    entity_id=enrichment.entity_id,
                    source_event_id=enrichment.source_event_id,
                    source_topic=enrichment.source_topic,
                    tactic=hit.tactic,
                    technique=hit.technique,
                    mitre_id=hit.mitre_id,
                    score=hit.score,
                    timestamp=enrichment.timestamp,
                    raw_payload=enrichment.raw_payload,
                )
            )
        if enrichment.entity_id:
            self._upsert_entity(session, enrichment)
            self._upsert_timeline(session, enrichment)

    def _upsert_entity(self, session: Session, enrichment: ThreatEnrichment) -> None:
        existing = session.scalars(select(EntityProfile).where(EntityProfile.entity_id == enrichment.entity_id).limit(1)).first()
        tactic = enrichment.tactic or "Unknown"
        technique = enrichment.technique or "Unknown"
        if existing:
            existing.last_seen = enrichment.timestamp
            existing.risk_score = round(max(existing.risk_score * 0.88, enrichment.threat_score), 2)
            existing.alert_count += len(enrichment.rule_hits)
            existing.anomaly_count += 1 if enrichment.raw_payload.get("is_anomaly") else 0
            existing.ioc_count += len(enrichment.iocs)
            existing.tactics = sorted(set(existing.tactics + [tactic]))
            existing.techniques = sorted(set(existing.techniques + [technique]))
            existing.risk_trend = (existing.risk_trend + [{"ts": enrichment.timestamp.isoformat(), "score": enrichment.threat_score}])[-50:]
            existing.raw_profile = {"latest_event": enrichment.raw_payload, "asn": enrichment.asn, "country": enrichment.geo_country}
            return
        session.add(
            EntityProfile(
                entity_id=enrichment.entity_id or "unknown",
                entity_type=enrichment.entity_type,
                first_seen=enrichment.timestamp,
                last_seen=enrichment.timestamp,
                risk_score=enrichment.threat_score,
                alert_count=len(enrichment.rule_hits),
                anomaly_count=1 if enrichment.raw_payload.get("is_anomaly") else 0,
                ioc_count=len(enrichment.iocs),
                tactics=[tactic],
                techniques=[technique],
                related_entities=[],
                risk_trend=[{"ts": enrichment.timestamp.isoformat(), "score": enrichment.threat_score}],
                raw_profile={"latest_event": enrichment.raw_payload, "asn": enrichment.asn, "country": enrichment.geo_country},
            )
        )

    def _upsert_timeline(self, session: Session, enrichment: ThreatEnrichment) -> None:
        timeline_id = f"timeline:{enrichment.entity_id}"
        existing = session.scalars(select(AttackTimeline).where(AttackTimeline.timeline_id == timeline_id).limit(1)).first()
        node = {"id": enrichment.event_id, "label": enrichment.technique or enrichment.event_type or "event", "type": "event", "severity": enrichment.severity, "score": enrichment.threat_score}
        tactic_node = {"id": enrichment.tactic or "Unknown", "label": enrichment.tactic or "Unknown", "type": "tactic"}
        edge = {"id": f"{enrichment.event_id}:{enrichment.tactic}", "source": enrichment.event_id, "target": enrichment.tactic or "Unknown", "label": enrichment.mitre_id or "mapped"}
        chain_item = {"timestamp": enrichment.timestamp.isoformat(), "event_id": enrichment.event_id, "tactic": enrichment.tactic, "technique": enrichment.technique, "severity": enrichment.severity, "score": enrichment.threat_score}
        if existing:
            existing.updated_at = enrichment.timestamp
            existing.severity = self._max_severity(existing.severity, enrichment.severity)
            existing.event_count += 1
            existing.attack_chain = (existing.attack_chain + [chain_item])[-100:]
            existing.nodes = self._dedupe_nodes(existing.nodes + [node, tactic_node])
            existing.edges = (existing.edges + [edge])[-150:]
            existing.raw_payload = enrichment.raw_payload
            return
        session.add(
            AttackTimeline(
                timeline_id=timeline_id,
                entity_id=enrichment.entity_id or "unknown",
                created_at=enrichment.timestamp,
                updated_at=enrichment.timestamp,
                severity=enrichment.severity,
                event_count=1,
                attack_chain=[chain_item],
                nodes=[node, tactic_node],
                edges=[edge],
                raw_payload=enrichment.raw_payload,
            )
        )

    @staticmethod
    def _dedupe_nodes(nodes: list[dict]) -> list[dict]:
        deduped = {node["id"]: node for node in nodes}
        return list(deduped.values())[-100:]

    @staticmethod
    def _max_severity(left: str, right: str) -> str:
        order = {"info": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
        return right if order.get(right, 0) > order.get(left, 0) else left
