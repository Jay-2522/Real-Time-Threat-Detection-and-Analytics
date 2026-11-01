"""A search index that is down must be probed, not hammered: one failed write per delay, not one per pending row."""
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from pipeline_runtime import durable
from pipeline_runtime.durable import TargetCircuit, target_unavailable


def _elastic_error(name, **attrs):
    return type(name, (Exception,), {"__module__": "elastic_transport", **attrs})("boom")


def test_only_unreachable_targets_are_classified_unavailable():
    assert target_unavailable(ConnectionRefusedError())
    assert target_unavailable(TimeoutError())
    assert target_unavailable(_elastic_error("ConnectionError"))
    assert target_unavailable(_elastic_error("ConnectionTimeout"))
    overloaded = Exception("overloaded"); overloaded.status_code = 503
    assert target_unavailable(overloaded)
    bad_row = Exception("mapper_parsing_exception"); bad_row.status_code = 400
    assert not target_unavailable(bad_row)  # one bad document must not block every other row
    assert not target_unavailable(ValueError("bad payload"))


def test_circuit_backs_off_caps_and_resets_on_recovery():
    now = [0.0]
    circuit = TargetCircuit(clock=lambda: now[0])
    assert not circuit.is_open()
    assert [circuit.record_failure() for _ in range(6)] == [5, 10, 20, 40, 60, 60]
    assert circuit.is_open()
    now[0] += 60
    assert not circuit.is_open()  # the next row probes the target
    assert circuit.record_success() is True and circuit.record_success() is False
    assert circuit.record_failure() == 5  # a recovered target starts again from the short delay


pytestmark_integration = pytest.mark.skipif(
    os.environ.get("POSTGRES_INTEGRATION") != "1",
    reason="Set POSTGRES_INTEGRATION=1 with a disposable PostgreSQL database",
)


@pytest.fixture
def outbox(monkeypatch):
    from dotenv import dotenv_values
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL
    from sqlalchemy.orm import sessionmaker

    env = {**dotenv_values(Path(__file__).resolve().parents[2] / ".env"), **os.environ}
    schema = "outbox_circuit_" + uuid4().hex
    url = URL.create("postgresql+psycopg", username=env.get("PGUSER", env.get("POSTGRES_USER")),
                     password=env.get("PGPASSWORD", env.get("POSTGRES_PASSWORD")), host=env.get("PGHOST", "127.0.0.1"),
                     port=int(env.get("PGPORT", env.get("POSTGRES_PUBLISHED_PORT", "5432"))),
                     database=env.get("PGDATABASE", env.get("POSTGRES_DB")))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    durable.Base.metadata.create_all(engine)
    factory = sessionmaker(engine)

    def seed(count, target="search", age_offset=0):
        base = datetime.now(UTC) - timedelta(hours=1)
        with factory() as session, session.begin():
            for index in range(count):
                session.add(durable.Outbox(service="test", key=f"k{age_offset + index:04d}", target=target, payload={},
                                           next_attempt=base + timedelta(seconds=age_offset + index)))

    def rows():
        with factory() as session:
            return session.query(durable.Outbox).order_by(durable.Outbox.key).all()

    yield factory, seed, rows
    engine.dispose()
    with create_engine(url).begin() as connection:
        connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))


@pytestmark_integration
def test_down_target_is_probed_once_not_once_per_pending_row_and_recovers(outbox):
    factory, seed, rows = outbox
    seed(30)
    calls = []

    def down(_key, _payload):
        calls.append(1)
        raise ConnectionRefusedError("connection refused")

    worker = durable.OutboxWorker(factory, "test", {"search": down})
    worker.drain_once(limit=50)
    worker.drain_once(limit=50)
    assert len(calls) == 1  # 30 pending rows, one probe, the circuit then holds the rest back
    assert all(row.attempts == 0 and not row.done for row in rows())  # the rows were not at fault

    delivered = []
    worker.handlers["search"] = lambda key, _payload: delivered.append(key)
    worker.circuits["search"].open_until = 0.0  # the probe delay has passed
    worker.drain_once(limit=50)
    assert len(delivered) == 30 and all(row.done for row in rows())
    assert worker.circuits["search"].failures == 0


@pytestmark_integration
def test_a_bad_row_is_retried_alone_and_does_not_open_the_circuit(outbox):
    factory, seed, rows = outbox
    seed(3)

    def handler(key, _payload):
        if key == "k0000":
            raise ValueError("mapper_parsing_exception")

    worker = durable.OutboxWorker(factory, "test", {"search": handler})
    worker.drain_once(limit=10)
    first, *others = rows()
    assert first.attempts == 1 and not first.done and first.next_attempt > datetime.now(UTC)
    assert all(row.done for row in others)
    assert not worker.circuits["search"].is_open()


@pytestmark_integration
def test_backlog_is_bounded_and_the_oldest_rows_are_dropped_visibly(outbox, monkeypatch):
    factory, seed, rows = outbox
    monkeypatch.setattr(durable, "MAX_PENDING", 10)
    seed(25)

    def down(_key, _payload):
        raise ConnectionRefusedError("connection refused")

    durable.OutboxWorker(factory, "test", {"search": down}).drain_once(limit=1)
    pending = [row for row in rows() if not row.done]
    dropped = [row for row in rows() if row.done]
    assert len(pending) == 10 and len(dropped) == 15
    assert max(row.key for row in dropped) < min(row.key for row in pending)  # oldest first
    assert all(row.last_error.startswith("dropped:") for row in dropped)
