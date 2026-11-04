"""Close incidents nobody has touched once they go quiet, so the open count reflects work that still needs attention."""
import logging
import threading
from datetime import UTC, datetime, timedelta

from sqlalchemy import update

from app.db.models import Alert, Incident
from app.utils.metrics import INCIDENTS_AUTO_RESOLVED_TOTAL

logger = logging.getLogger(__name__)
CHUNK = 500


class IncidentLifecycleWorker:
    """Resolves incidents that are still `open` after `quiet_seconds` without a new alert, with their open alerts.

    An analyst's decision always wins: acknowledged and investigating incidents are never closed automatically.
    New activity on the same entity after resolution starts a new incident, because the correlator only joins
    unresolved incidents inside its correlation window, which is far shorter than the quiet period.
    `quiet_seconds` of 0 disables the worker.
    """

    def __init__(self, session_factory, quiet_seconds: int, interval_seconds: float = 60.0) -> None:
        self._factory = session_factory
        self._quiet_seconds = quiet_seconds
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def resolve_stale_once(self, now: datetime | None = None) -> int:
        if self._quiet_seconds <= 0:
            return 0
        cutoff = (now or datetime.now(UTC)) - timedelta(seconds=self._quiet_seconds)
        note = f"Auto-resolved: no new alerts for {self._quiet_seconds // 60} minutes."
        with self._factory() as session, session.begin():
            resolved = list(session.scalars(
                update(Incident).where(Incident.status == "open", Incident.updated_at < cutoff)
                .values(status="resolved", resolution_notes=note).returning(Incident.incident_id)))
            for start in range(0, len(resolved), CHUNK):
                session.execute(update(Alert).where(Alert.status == "open", Alert.incident_id.in_(resolved[start:start + CHUNK]))
                                .values(status="resolved"))
        if resolved:
            INCIDENTS_AUTO_RESOLVED_TOTAL.inc(len(resolved))
            logger.info("incidents_auto_resolved count=%s quiet_seconds=%s", len(resolved), self._quiet_seconds)
        return len(resolved)

    def start(self) -> None:
        if self._quiet_seconds <= 0:
            logger.info("incident_auto_resolution_disabled")
            return

        def run() -> None:
            while not self._stop.is_set():
                try:
                    self.resolve_stale_once()
                except Exception as exc:  # the database may be briefly unavailable; try again next interval
                    logger.warning("incident_auto_resolution_failed error=%s reason=%s", type(exc).__name__,
                                   (str(exc).strip().splitlines() or [""])[0][:200])
                self._stop.wait(self._interval_seconds)

        self._thread = threading.Thread(target=run, daemon=True, name="incident-lifecycle")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
