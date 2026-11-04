"""Pipeline stage health measured from when each stage last persisted data, not asserted.

Kafka itself is not probed: its status is the flow of telemetry through it into the storage sink. A stage that
has gone quiet while its input is still arriving is degraded; one that has gone quiet because nothing arrives is
idle, which is normal when the producer is stopped.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import asc, desc, select
from sqlalchemy.orm import Session

from app.db.models import AnalyticsMetric, AnomalyPrediction, FeatureSnapshot, RiskMetric, TelemetryEvent

INGEST_FRESH_SECONDS = 120.0  # telemetry arrives continuously while a producer runs
STAGE_FRESH_SECONDS = 600.0  # Spark emits a window about 3 minutes after its events (2 min watermark + 1 min window)
SPARK_OUTPUTS = (FeatureSnapshot, AnalyticsMetric, RiskMetric)


@dataclass(frozen=True)
class StageHealth:
    name: str
    status: str  # active | idle | waiting | degraded
    detail: str
    last_activity_at: datetime | None


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _persisted_at(db: Session, models, *, newest: bool) -> datetime | None:
    """First or last persisted time across tables, read through the primary-key index (no table scan)."""
    order = desc if newest else asc
    found = [value for model in models
             if (value := db.scalar(select(model.created_at).order_by(order(model.id)).limit(1))) is not None]
    return _utc(max(found) if newest else min(found)) if found else None


def duration_text(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds} s"
    if seconds < 5400:
        return f"{round(seconds / 60)} min"
    return f"{round(seconds / 3600)} h"


def _stage(name: str, noun: str, last: datetime | None, now: datetime, *, fresh_seconds: float,
           upstream_active: bool, upstream_first: datetime | None) -> StageHealth:
    if last is None:
        started = (now - upstream_first).total_seconds() if upstream_first else 0.0
        if upstream_active and started > STAGE_FRESH_SECONDS:
            return StageHealth(name, "degraded", f"Input has been arriving for {duration_text(started)} but no {noun} has been stored.", None)
        return StageHealth(name, "waiting", f"No {noun} stored yet.", None)
    age = (now - last).total_seconds()
    if age <= fresh_seconds:
        return StageHealth(name, "active", f"Last {noun} stored {duration_text(age)} ago.", last)
    if upstream_active:
        return StageHealth(name, "degraded", f"Input is arriving but no new {noun} for {duration_text(age)}.", last)
    return StageHealth(name, "idle", f"No new input; last {noun} stored {duration_text(age)} ago.", last)


def pipeline_stages(db: Session, now: datetime | None = None) -> list[StageHealth]:
    now = now or datetime.now(UTC)
    ingest_last = _persisted_at(db, (TelemetryEvent,), newest=True)
    ingest_first = _persisted_at(db, (TelemetryEvent,), newest=False)
    spark_last = _persisted_at(db, SPARK_OUTPUTS, newest=True)
    spark_first = _persisted_at(db, SPARK_OUTPUTS, newest=False)
    ml_last = _persisted_at(db, (AnomalyPrediction,), newest=True)

    ingest_active = ingest_last is not None and (now - ingest_last).total_seconds() <= INGEST_FRESH_SECONDS
    spark_active = spark_last is not None and (now - spark_last).total_seconds() <= STAGE_FRESH_SECONDS
    if ingest_last is None:
        kafka = StageHealth("Kafka", "waiting", "No telemetry stored yet.", None)
    elif ingest_active:
        kafka = StageHealth("Kafka", "active", f"Telemetry is flowing: last event stored {duration_text((now - ingest_last).total_seconds())} ago.", ingest_last)
    else:
        kafka = StageHealth("Kafka", "idle", f"No new telemetry for {duration_text((now - ingest_last).total_seconds())}; the producer may be stopped.", ingest_last)
    spark = _stage("Spark", "analytics output", spark_last, now, fresh_seconds=STAGE_FRESH_SECONDS,
                   upstream_active=ingest_active, upstream_first=ingest_first)
    ml = _stage("ML Inference", "prediction", ml_last, now, fresh_seconds=STAGE_FRESH_SECONDS,
                upstream_active=spark_active, upstream_first=spark_first)
    return [kafka, spark, ml]
