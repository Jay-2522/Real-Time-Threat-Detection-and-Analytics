from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ThreatIntelEvent(Base):
    __tablename__ = "threat_intel_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    event_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tactic: Mapped[str | None] = mapped_column(String(128), nullable=True)
    technique: Mapped[str | None] = mapped_column(String(255), nullable=True)
    mitre_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    kill_chain_stage: Mapped[str | None] = mapped_column(String(128), nullable=True)
    threat_score: Mapped[float] = mapped_column(Float, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    geo_country: Mapped[str | None] = mapped_column(String(128), nullable=True)
    asn: Mapped[str | None] = mapped_column(String(128), nullable=True)
    iocs: Mapped[list] = mapped_column(JSONB, nullable=False)
    rule_hits: Mapped[list] = mapped_column(JSONB, nullable=False)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index("ix_threat_intel_events_timestamp", "timestamp"),
        Index("ix_threat_intel_events_entity_id", "entity_id"),
        Index("ix_threat_intel_events_tactic", "tactic"),
        Index("ix_threat_intel_events_mitre_id", "mitre_id"),
        Index("ix_threat_intel_events_threat_score", "threat_score"),
        Index("ix_threat_intel_events_severity", "severity"),
    )


class IOCMatch(Base):
    __tablename__ = "ioc_matches"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ioc_value: Mapped[str] = mapped_column(String(512), nullable=False)
    ioc_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    feed_name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)


class EntityProfile(Base):
    __tablename__ = "entity_profiles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    risk_score: Mapped[float] = mapped_column(Float, nullable=False)
    alert_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    anomaly_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    ioc_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tactics: Mapped[list] = mapped_column(JSONB, nullable=False)
    techniques: Mapped[list] = mapped_column(JSONB, nullable=False)
    related_entities: Mapped[list] = mapped_column(JSONB, nullable=False)
    risk_trend: Mapped[list] = mapped_column(JSONB, nullable=False)
    raw_profile: Mapped[dict] = mapped_column(JSONB, nullable=False)


class DetectionRuleHit(Base):
    __tablename__ = "detection_rule_hits"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_id: Mapped[str] = mapped_column(String(128), nullable=False)
    rule_name: Mapped[str] = mapped_column(String(255), nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    tactic: Mapped[str | None] = mapped_column(String(128), nullable=True)
    technique: Mapped[str | None] = mapped_column(String(255), nullable=True)
    mitre_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)


class AttackTimeline(Base):
    __tablename__ = "attack_timelines"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timeline_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    entity_id: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False)
    attack_chain: Mapped[list] = mapped_column(JSONB, nullable=False)
    nodes: Mapped[list] = mapped_column(JSONB, nullable=False)
    edges: Mapped[list] = mapped_column(JSONB, nullable=False)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)


class DetectionRuleCatalog(Base):
    __tablename__ = "detection_rule_catalog"
    rule_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    tactic: Mapped[str | None] = mapped_column(String(128), nullable=True)
    technique: Mapped[str | None] = mapped_column(String(255), nullable=True)
    mitre_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    loaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
