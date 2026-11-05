"""Delete stored pipeline records once they are older than the retention period, so the tables do not grow forever.

Only the high-volume pipeline tables are pruned. Alerts, incidents and threat-intelligence records are security
history and are kept. Kafka's own retention is shorter than the default period here, so pruned records are not
replayed back in.
"""
import logging
import threading
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select

from app.db.models import AnalyticsMetric, AnomalyPrediction, DeadLetterEvent, FeatureSnapshot, RiskMetric, TelemetryEvent
from app.utils.metrics import RECORDS_PRUNED_TOTAL

logger = logging.getLogger(__name__)
TABLES = (TelemetryEvent, AnalyticsMetric, RiskMetric, FeatureSnapshot, AnomalyPrediction, DeadLetterEvent)
CHUNK = 5000


class RetentionWorker:
    """`retention_days` of 0 disables pruning."""

    def __init__(self, session_factory, retention_days: int, interval_seconds: float = 3600.0) -> None:
        self._factory = session_factory
        self._retention_days = retention_days
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def prune_once(self, now: datetime | None = None) -> dict[str, int]:
        if self._retention_days <= 0:
            return {}
        cutoff = (now or datetime.now(UTC)) - timedelta(days=self._retention_days)
        removed: dict[str, int] = {}
        for model in TABLES:
            total = 0
            while True:  # small transactions, so a large backlog never holds a long lock
                with self._factory() as session, session.begin():
                    ids = list(session.scalars(select(model.id).where(model.created_at < cutoff).order_by(model.id).limit(CHUNK)))
                    if ids:
                        session.execute(delete(model).where(model.id.in_(ids)))
                total += len(ids)
                if len(ids) < CHUNK or self._stop.is_set():
                    break
            if total:
                removed[model.__tablename__] = total
                RECORDS_PRUNED_TOTAL.labels(model.__tablename__).inc(total)
        if removed:
            logger.info("records_pruned retention_days=%s removed=%s", self._retention_days, removed)
        return removed

    def start(self) -> None:
        if self._retention_days <= 0:
            logger.info("retention_disabled")
            return

        def run() -> None:
            while not self._stop.is_set():
                try:
                    self.prune_once()
                except Exception as exc:  # the database may be briefly unavailable; try again next interval
                    logger.warning("retention_failed error=%s reason=%s", type(exc).__name__,
                                   (str(exc).strip().splitlines() or [""])[0][:200])
                self._stop.wait(self._interval_seconds)

        self._thread = threading.Thread(target=run, name="retention", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
