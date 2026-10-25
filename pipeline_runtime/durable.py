"""PostgreSQL receipt + side-effect outbox, committed with each business transaction."""
import logging
import threading
from datetime import datetime, timedelta, timezone
from sqlalchemy import String, Text, Integer, Boolean, DateTime, select
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from prometheus_client import Counter, Gauge
from pipeline_runtime.identity import digest

logger = logging.getLogger(__name__)
OUTBOX_FAILURES = Counter("pipeline_outbox_failures_total", "Recoverable external side-effect failures", ["service", "target"])
OUTBOX_WORKER_FAILURES = Counter("pipeline_outbox_worker_failures_total", "Failed outbox database observations", ["service"])
OUTBOX_PENDING = Gauge("pipeline_outbox_pending", "Pending durable side effects", ["service"])


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


def enqueue(session, service, key, target, payload):
    session.execute(insert(Outbox).values(service=service, key=key, target=target, payload=payload)
                    .on_conflict_do_nothing())


class OutboxWorker:
    def __init__(self, factory, service, handlers):
        self.factory, self.service, self.handlers = factory, service, handlers
        self.stop_event = threading.Event()
        self.thread = None

    def drain_once(self, limit=20):
        from sqlalchemy import func
        processed = 0
        for _ in range(limit):
            with self.factory() as session, session.begin():
                row = session.scalars(select(Outbox).where(Outbox.service == self.service, Outbox.done == False,
                    Outbox.next_attempt <= datetime.now(timezone.utc)).order_by(Outbox.next_attempt)
                    .with_for_update(skip_locked=True).limit(1)).first()
                if row is None:
                    break
                processed += 1
                try:
                    self.handlers[row.target](row.key, row.payload)
                    row.done = True
                    row.last_error = None
                except Exception as exc:
                    row.attempts += 1
                    row.last_error = str(exc)[:2000]
                    row.next_attempt = datetime.now(timezone.utc) + timedelta(seconds=min(300, 2 ** min(row.attempts, 8)))
                    OUTBOX_FAILURES.labels(self.service, row.target).inc()
                    logger.warning("outbox_retry service=%s target=%s key=%s attempts=%s error=%s",
                                   self.service, row.target, row.key, row.attempts, row.last_error)
        with self.factory() as session:
            count = session.scalar(select(func.count()).select_from(Outbox).where(Outbox.service == self.service, Outbox.done == False))
            OUTBOX_PENDING.labels(self.service).set(count)
        return processed

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
