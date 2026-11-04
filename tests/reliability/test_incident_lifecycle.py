"""Quiet incidents that nobody has touched must close, or the open count only ever grows."""
import importlib
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get("POSTGRES_INTEGRATION") != "1",
    reason="Set POSTGRES_INTEGRATION=1 with a disposable PostgreSQL database",
)]

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
QUIET = 3600


@pytest.fixture
def lifecycle():
    from dotenv import dotenv_values
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL
    from sqlalchemy.orm import sessionmaker

    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(ROOT / "alert-engine"))
    models = importlib.import_module("app.db.models")
    module = importlib.import_module("app.incidents.lifecycle")

    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    schema = "lifecycle_" + uuid4().hex
    url = URL.create("postgresql+psycopg", username=env.get("PGUSER", env.get("POSTGRES_USER")),
                     password=env.get("PGPASSWORD", env.get("POSTGRES_PASSWORD")), host=env.get("PGHOST", "127.0.0.1"),
                     port=int(env.get("PGPORT", env.get("POSTGRES_PUBLISHED_PORT", "5432"))),
                     database=env.get("PGDATABASE", env.get("POSTGRES_DB")))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    models.Base.metadata.create_all(engine)
    factory = sessionmaker(engine)

    def add(incident_id, status, quiet_for, alert_status="open"):
        """An incident last updated `quiet_for` seconds before NOW, with one alert."""
        updated = NOW - timedelta(seconds=quiet_for)
        with factory() as session, session.begin():
            session.add(models.Incident(incident_id=incident_id, created_at=updated, updated_at=updated, severity="high",
                                        title=incident_id, description="d", related_alerts=[], entity_ids=[],
                                        status=status, raw_payload={}))
            session.flush()
            session.add(models.Alert(alert_id=f"alert-{incident_id}", timestamp=updated, severity="high", title="t",
                                     description="d", source_topic="t", correlation_id="c", incident_id=incident_id,
                                     explanation="e", recommended_action="r", status=alert_status, tags=[], raw_payload={}))

    def state():
        with factory() as session:
            incidents = {row.incident_id: row for row in session.query(models.Incident)}
            alerts = {row.incident_id: row for row in session.query(models.Alert)}
        return incidents, alerts

    yield module.IncidentLifecycleWorker, factory, add, state
    engine.dispose()
    with create_engine(url).begin() as connection:
        connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))


def test_only_quiet_untouched_incidents_are_resolved_with_their_open_alerts(lifecycle):
    worker_class, factory, add, state = lifecycle
    add("quiet-open", "open", QUIET + 60)
    add("recent-open", "open", QUIET - 60)
    add("quiet-acknowledged", "acknowledged", QUIET * 5)
    add("quiet-investigating", "investigating", QUIET * 5)
    add("quiet-resolved", "resolved", QUIET * 5)
    add("quiet-open-ack-alert", "open", QUIET + 60, alert_status="acknowledged")

    assert worker_class(factory, QUIET).resolve_stale_once(NOW) == 2

    incidents, alerts = state()
    assert {k: v.status for k, v in incidents.items()} == {
        "quiet-open": "resolved", "recent-open": "open", "quiet-acknowledged": "acknowledged",
        "quiet-investigating": "investigating", "quiet-resolved": "resolved", "quiet-open-ack-alert": "resolved"}
    assert incidents["quiet-open"].resolution_notes == "Auto-resolved: no new alerts for 60 minutes."
    assert incidents["recent-open"].resolution_notes is None
    assert alerts["quiet-open"].status == "resolved"
    assert alerts["recent-open"].status == "open" and alerts["quiet-acknowledged"].status == "open"
    assert alerts["quiet-open-ack-alert"].status == "acknowledged"  # an analyst's own decision on an alert is kept


def test_resolution_is_repeatable_and_can_be_disabled(lifecycle):
    worker_class, factory, add, state = lifecycle
    add("quiet-open", "open", QUIET + 60)
    assert worker_class(factory, 0).resolve_stale_once(NOW) == 0  # disabled
    assert state()[0]["quiet-open"].status == "open"
    worker = worker_class(factory, QUIET)
    assert worker.resolve_stale_once(NOW) == 1 and worker.resolve_stale_once(NOW) == 0
