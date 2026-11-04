from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.db.models import AttackTimeline, DetectionRuleCatalog, DetectionRuleHit, EntityProfile, IOCMatch, ThreatIntelEvent
from app.services.mitre_mapper import MitreMapper
from app.services.search_client import run_search

logger = logging.getLogger(__name__)


class ThreatIntelQueryService:
    def __init__(self) -> None:
        self._mitre = MitreMapper()

    def overview(self, db: Session) -> dict[str, Any]:
        severity_rows = db.execute(select(ThreatIntelEvent.severity, func.count()).group_by(ThreatIntelEvent.severity)).all()
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "total_enriched_events": db.scalar(select(func.count()).select_from(ThreatIntelEvent)) or 0,
            "ioc_matches": db.scalar(select(func.count()).select_from(IOCMatch)) or 0,
            "high_risk_entities": db.scalar(select(func.count()).select_from(EntityProfile).where(EntityProfile.risk_score >= 70)) or 0,
            "rule_hits": db.scalar(select(func.count()).select_from(DetectionRuleHit)) or 0,
            "active_timelines": db.scalar(select(func.count()).select_from(AttackTimeline)) or 0,
            "severity_counts": {row[0]: int(row[1]) for row in severity_rows},
        }

    def iocs(self, db: Session, limit: int = 100, offset: int = 0, ioc_type: str | None = None) -> list[IOCMatch]:
        statement = select(IOCMatch).order_by(desc(IOCMatch.timestamp), desc(IOCMatch.id)).limit(limit).offset(offset)
        if ioc_type:
            statement = statement.where(IOCMatch.ioc_type == ioc_type)
        return list(db.scalars(statement))

    def entities(self, db: Session, limit: int = 100, offset: int = 0) -> list[EntityProfile]:
        return list(db.scalars(select(EntityProfile).order_by(desc(EntityProfile.risk_score), desc(EntityProfile.last_seen), EntityProfile.id).limit(limit).offset(offset)))

    def entity(self, db: Session, entity_id: str) -> EntityProfile | None:
        return db.scalars(select(EntityProfile).where(EntityProfile.entity_id == entity_id).limit(1)).first()

    def high_risk_entities(self, db: Session, limit: int = 50, offset: int = 0) -> list[EntityProfile]:
        return list(db.scalars(select(EntityProfile).where(EntityProfile.risk_score >= 70).order_by(desc(EntityProfile.risk_score), EntityProfile.id).limit(limit).offset(offset)))

    def search_entities(self, db: Session, query: str, limit: int = 50) -> list[EntityProfile]:
        pattern = f"%{query}%"
        return list(db.scalars(select(EntityProfile).where(EntityProfile.entity_id.ilike(pattern)).order_by(desc(EntityProfile.risk_score)).limit(limit)))

    def mitre_summary(self, db: Session) -> list[dict[str, Any]]:
        rows = db.execute(
            select(ThreatIntelEvent.tactic, ThreatIntelEvent.technique, ThreatIntelEvent.mitre_id, func.count(), func.avg(ThreatIntelEvent.threat_score))
            .where(ThreatIntelEvent.tactic.is_not(None))
            .group_by(ThreatIntelEvent.tactic, ThreatIntelEvent.technique, ThreatIntelEvent.mitre_id)
            .order_by(desc(func.count()))
        ).all()
        return [
            {"tactic": row[0], "technique": row[1], "mitre_id": row[2], "count": int(row[3]), "avg_threat_score": round(float(row[4] or 0), 2)}
            for row in rows
        ]

    def attack_timeline(self, db: Session, entity_id: str | None = None, limit: int = 20) -> list[AttackTimeline]:
        statement = select(AttackTimeline).order_by(desc(AttackTimeline.updated_at), desc(AttackTimeline.id)).limit(limit)
        if entity_id:
            statement = statement.where(AttackTimeline.entity_id == entity_id)
        return list(db.scalars(statement))

    def risk_heatmap(self, db: Session) -> list[dict[str, Any]]:
        rows = db.execute(
            select(ThreatIntelEvent.geo_country, ThreatIntelEvent.tactic, func.count(), func.avg(ThreatIntelEvent.threat_score))
            .where(ThreatIntelEvent.geo_country.is_not(None))
            .group_by(ThreatIntelEvent.geo_country, ThreatIntelEvent.tactic)
            .order_by(desc(func.avg(ThreatIntelEvent.threat_score)))
            .limit(100)
        ).all()
        return [{"country": row[0], "tactic": row[1] or "Unknown", "count": int(row[2]), "risk_score": round(float(row[3] or 0), 2)} for row in rows]

    def threat_graph(self, db: Session, limit: int = 80) -> dict[str, Any]:
        events = list(db.scalars(select(ThreatIntelEvent).order_by(desc(ThreatIntelEvent.timestamp)).limit(limit)))
        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []
        for event in events:
            entity = event.entity_id or "unknown"
            tactic = event.tactic or "Unknown"
            nodes[entity] = {"id": entity, "label": entity, "type": "entity", "risk_score": event.threat_score}
            nodes[tactic] = {"id": tactic, "label": tactic, "type": "tactic", "risk_score": event.threat_score}
            edges.append({"id": f"{event.event_id}-{tactic}", "source": entity, "target": tactic, "label": event.mitre_id or "mapped", "weight": event.threat_score})
        return {"nodes": list(nodes.values()), "edges": edges}

    def detection_rules(self, db: Session) -> list[dict[str, Any]]:
        rows = db.execute(
            select(DetectionRuleHit.rule_id, DetectionRuleHit.rule_name, DetectionRuleHit.severity, func.count(), func.avg(DetectionRuleHit.score))
            .group_by(DetectionRuleHit.rule_id, DetectionRuleHit.rule_name, DetectionRuleHit.severity)
            .order_by(desc(func.count()))
        ).all()
        return [{"rule_id": row[0], "name": row[1], "severity": row[2], "enabled": True, "hit_count": int(row[3]), "avg_score": round(float(row[4] or 0), 2)} for row in rows]

    def loaded_detection_rules(self, db: Session) -> list[dict[str, Any]]:
        """Every detection rule the running threat engine reported at startup, enabled or not.

        Read from PostgreSQL, where the engine records it, so the answer does not depend on Prometheus being up.
        """
        rows = db.scalars(select(DetectionRuleCatalog).order_by(DetectionRuleCatalog.rule_id))
        return [{"rule_id": row.rule_id, "name": row.name, "severity": row.severity, "enabled": row.enabled,
                 "tactic": row.tactic, "technique": row.technique, "mitre_id": row.mitre_id,
                 "loaded_at": row.loaded_at.isoformat() if row.loaded_at else None} for row in rows]

    def detection_rule(self, db: Session, rule_id: str) -> dict[str, Any] | None:
        hits = list(db.scalars(select(DetectionRuleHit).where(DetectionRuleHit.rule_id == rule_id).order_by(desc(DetectionRuleHit.timestamp)).limit(50)))
        if not hits:
            return None
        return {"rule_id": rule_id, "name": hits[0].rule_name, "severity": hits[0].severity, "hits": [self._row(hit) for hit in hits]}

    def rule_stats(self, db: Session) -> dict[str, Any]:
        return {"total_rule_hits": db.scalar(select(func.count()).select_from(DetectionRuleHit)) or 0, "rules": self.detection_rules(db)}

    def search(self, query: str, limit: int = 50) -> list[dict[str, Any]]:
        return run_search("threat-intel-events,ioc-matches,entity-profiles,detection-rule-hits,attack-timelines", query, limit)

    def live_message(self, db: Session) -> dict[str, Any]:
        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "overview": self.overview(db),
            "recent_iocs": [self._row(row) for row in self.iocs(db, 10)],
            "high_risk_entities": [self._row(row) for row in self.high_risk_entities(db, 10)],
            "mitre": self.mitre_summary(db)[:10],
            "graph": self.threat_graph(db, 40),
        }

    def mitre_tactics(self) -> list[str]:
        return self._mitre.tactics

    def mitre_techniques(self) -> list[dict[str, Any]]:
        return self._mitre.techniques()

    @staticmethod
    def _row(row: Any, omit: frozenset[str] = frozenset()) -> dict[str, Any]:
        data = {column.name: getattr(row, column.name) for column in row.__table__.columns if column.name not in omit}
        for key, value in list(data.items()):
            if hasattr(value, "isoformat"):
                data[key] = value.isoformat()
        return data
