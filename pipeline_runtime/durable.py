"""PostgreSQL receipt + side-effect outbox, committed with each business transaction."""
import logging
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from sqlalchemy import String, Text, Integer, Boolean, DateTime, select, tuple_, update
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from prometheus_client import Counter, Gauge
from pipeline_runtime.identity import digest

logger = logging.getLogger(__name__)
OUTBOX_FAILURES = Counter("pipeline_outbox_failures_total", "Recoverable external side-effect failures", ["service", "target"])
OUTBOX_WORKER_FAILURES = Counter("pipeline_outbox_worker_failures_total", "Failed outbox database observations", ["service"])
OUTBOX_PENDING = Gauge("pipeline_outbox_pending", "Pending durable side effects", ["service"])
OUTBOX_DROPPED = Counter("pipeline_outbox_dropped_total", "Pending side effects discarded because the outbox was over capacity", ["service"])
OUTBOX_TARGET_DOWN = Gauge("pipeline_outbox_target_unavailable", "1 while a side-effect target is being probed instead of written", ["service", "target"])

# PostgreSQL is authoritative; the outbox only feeds projections such as the search index. A target that is down is
# probed on a growing delay instead of being written once per pending row, and the backlog is bounded so a long
# outage cannot fill the database. Rows dropped past the cap are missing from the projection until it is rebuilt.
MAX_PENDING = int(os.environ.get("PIPELINE_OUTBOX_MAX_PENDING", "500000"))
PROBE_FIRST_DELAY_SECONDS = 5.0
PROBE_MAX_DELAY_SECONDS = 60.0
_UNAVAILABLE_STATUSES = {429, 502, 503, 504}
_UNAVAILABLE_ERRORS = {"ConnectionError", "ConnectionTimeout", "TlsError"}


def target_unavailable(exc: BaseException) -> bool:
    """True when the target itself is unreachable or overloaded, not when one row is bad."""
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    if any(cls.__module__.startswith("elastic_transport") and cls.__name__ in _UNAVAILABLE_ERRORS
           for cls in type(exc).__mro__):
        return True
    return getattr(exc, "status_code", None) in _UNAVAILABLE_STATUSES


class TargetCircuit:
    """Stops writing to an unreachable target for a growing delay, then lets one row probe it."""

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self.failures = 0
        self.open_until = 0.0

    def is_open(self) -> bool:
        return self._clock() < self.open_until

    def record_failure(self) -> float:
        self.failures += 1
        delay = min(PROBE_MAX_DELAY_SECONDS, PROBE_FIRST_DELAY_SECONDS * 2 ** (self.failures - 1))
        self.open_until = self._clock() + delay
        return delay

    def record_success(self) -> bool:
        """Returns True when the target had been failing."""
        recovered = self.failures > 0
        self.failures, self.open_until = 0, 0.0
        return recovered


class Base(DeclarativeBase):
    pass


class Receipt(Base):
    __tablename__ = "pipeline_receipts"
    service: Mapped[str] = mapped_column(String(32), primary_key=True)
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Outbox(Base):
    __tablename__ = "pipeline_outbox"
    service: Mapped[str] = mapped_column(String(32), primary_key=True)
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    target: Mapped[str] = mapped_column(String(32), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    done: Mapped[bool] = mapped_column(Boolean, default=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class IdentityConflict(ValueError):
    pass


@dataclass(frozen=True)
class BatchClaimResult:
    inserted: set[str]
    duplicates: set[str]
    conflicts: set[str]


def claim(session, service, key, payload):
    fingerprint = digest(payload)
    inserted = session.execute(insert(Receipt).values(service=service, key=key, fingerprint=fingerprint)
        .on_conflict_do_nothing().returning(Receipt.key)).scalar_one_or_none()
    if inserted:
        return True
    existing = session.get(Receipt, (service, key))
    if existing.fingerprint != fingerprint:
        raise IdentityConflict(f"Conflicting payload for stable identity {key}")
    return False


def claim_many(session, service, payloads):
    """Claim stable identities in two bounded round trips, preserving conflict detection."""
    if not payloads:
        return BatchClaimResult(set(), set(), set())
    fingerprints = {key: digest(payload) for key, payload in payloads.items()}
    keys = list(fingerprints)
    existing = dict(session.execute(
        select(Receipt.key, Receipt.fingerprint).where(Receipt.service == service, Receipt.key.in_(keys))
    ).all())
    conflicts = {key for key, fingerprint in existing.items() if fingerprint != fingerprints[key]}
    duplicates = set(existing) - conflicts
    pending = [key for key in keys if key not in existing]
    inserted = set()
    if pending:
        statement = insert(Receipt).values([
            {"service": service, "key": key, "fingerprint": fingerprints[key]} for key in pending
        ]).on_conflict_do_nothing().returning(Receipt.key)
        inserted = set(session.scalars(statement).all())
        raced = set(pending) - inserted
        if raced:
            raced_rows = dict(session.execute(
                select(Receipt.key, Receipt.fingerprint).where(
                    Receipt.service == service, Receipt.key.in_(raced)
                )
            ).all())
            conflicts.update(key for key in raced if raced_rows.get(key) != fingerprints[key])
            duplicates.update(raced - conflicts)
    return BatchClaimResult(inserted, duplicates, conflicts)


def enqueue(session, service, key, target, payload):
    session.execute(insert(Outbox).values(service=service, key=key, target=target, payload=payload)
                    .on_conflict_do_nothing())


def enqueue_many(session, service, target, payloads):
    if not payloads:
        return
    session.execute(insert(Outbox).values([
        {"service": service, "key": key, "target": target, "payload": payload}
        for key, payload in payloads.items()
    ]).on_conflict_do_nothing())


class OutboxWorker:
    def __init__(self, factory, service, handlers):
        self.factory, self.service, self.handlers = factory, service, handlers
        self.stop_event = threading.Event()
        self.thread = None
        self.circuits = defaultdict(TargetCircuit)

    def drain_once(self, limit=20):
        from sqlalchemy import func
        processed = 0
        for _ in range(limit):
            blocked = [target for target, circuit in self.circuits.items() if circuit.is_open()]
            with self.factory() as session, session.begin():
                query = select(Outbox).where(Outbox.service == self.service, Outbox.done == False,
                    Outbox.next_attempt <= datetime.now(timezone.utc))
                if blocked:
                    query = query.where(Outbox.target.notin_(blocked))
                row = session.scalars(query.order_by(Outbox.next_attempt).with_for_update(skip_locked=True).limit(1)).first()
                if row is None:
                    break
                processed += 1
                circuit = self.circuits[row.target]
                try:
                    self.handlers[row.target](row.key, row.payload)
                    row.done = True
                    row.last_error = None
                    if circuit.record_success():
                        OUTBOX_TARGET_DOWN.labels(self.service, row.target).set(0)
                        logger.info("outbox_target_recovered service=%s target=%s", self.service, row.target)
                except Exception as exc:
                    row.last_error = str(exc)[:2000]
                    OUTBOX_FAILURES.labels(self.service, row.target).inc()
                    reason = (row.last_error.strip().splitlines() or [""])[0][:200]
                    if target_unavailable(exc):
                        # The target is down, not this row: keep the row's attempt count and stop writing for a while.
                        delay = circuit.record_failure()
                        OUTBOX_TARGET_DOWN.labels(self.service, row.target).set(1)
                        logger.warning("outbox_target_unavailable service=%s target=%s retry_in_seconds=%.0f error=%s reason=%s",
                                       self.service, row.target, delay, type(exc).__name__, reason)
                    else:
                        row.attempts += 1
                        row.next_attempt = datetime.now(timezone.utc) + timedelta(seconds=min(300, 2 ** min(row.attempts, 8)))
                        logger.warning("outbox_retry service=%s target=%s key=%s attempts=%s error=%s",
                                       self.service, row.target, row.key, row.attempts, reason)
        with self.factory() as session:
            count = session.scalar(select(func.count()).select_from(Outbox).where(Outbox.service == self.service, Outbox.done == False))
            OUTBOX_PENDING.labels(self.service).set(count)
        if count > MAX_PENDING:
            self._drop_oldest(count - MAX_PENDING)
        return processed

    def _drop_oldest(self, excess):
        with self.factory() as session, session.begin():
            oldest = select(Outbox.service, Outbox.key, Outbox.target).where(
                Outbox.service == self.service, Outbox.done == False).order_by(Outbox.next_attempt).limit(excess)
            dropped = session.execute(update(Outbox).where(tuple_(Outbox.service, Outbox.key, Outbox.target).in_(oldest))
                .values(done=True, last_error="dropped: outbox over capacity; the projection lacks this row, PostgreSQL has it")).rowcount
        OUTBOX_DROPPED.labels(self.service).inc(dropped)
        OUTBOX_PENDING.labels(self.service).set(MAX_PENDING)
        logger.warning("outbox_over_capacity service=%s dropped=%s cap=%s", self.service, dropped, MAX_PENDING)

    def start(self):
        def run():
            while not self.stop_event.is_set():
                try:
                    processed = self.drain_once()
                except Exception:
                    processed = 0
                    OUTBOX_WORKER_FAILURES.labels(self.service).inc()
                    logger.exception("outbox_worker_retry service=%s", self.service)
                self.stop_event.wait(0.01 if processed == 20 else 2)
        self.thread = threading.Thread(target=run, daemon=True, name=f"{self.service}-outbox")
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=10)


class Quarantine(Base):
    __tablename__ = "pipeline_quarantine"
    service: Mapped[str] = mapped_column(String(32), primary_key=True)
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


def quarantine_record(session, service, payload):
    session.execute(insert(Quarantine).values(service=service, key=digest(payload["event_id"]), payload=payload)
                    .on_conflict_do_nothing())
