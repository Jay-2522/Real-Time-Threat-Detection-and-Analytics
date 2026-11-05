"""Old pipeline records are pruned in chunks; alerts and recent records are kept; 0 disables pruning."""
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
NOW = datetime(2026, 1, 31, tzinfo=UTC)


@pytest.fixture
def retention():
    from dotenv import dotenv_values
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL
    from sqlalchemy.orm import sessionmaker

    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(ROOT / "storage-sink"))
    models = importlib.import_module("app.db.models")
    module = importlib.import_module("app.retention")
    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    schema = "retention_" + uuid4().hex
    url = URL.create("postgresql+psycopg", username=env.get("PGUSER", env.get("POSTGRES_USER")),
                     password=env.get("PGPASSWORD", env.get("POSTGRES_PASSWORD")), host=env.get("PGHOST", "127.0.0.1"),
                     port=int(env.get("PGPORT", env.get("POSTGRES_PUBLISHED_PORT", "5432"))),
                     database=env.get("PGDATABASE", env.get("POSTGRES_DB")))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    models.Base.metadata.create_all(engine)
    factory = sessionmaker(engine)

    def add(model, count, age_days, tag):
        when = NOW - timedelta(days=age_days)
        with factory() as session, session.begin():
            session.add_all(model(source_topic="t", raw_payload={}, timestamp=when, created_at=when) for _ in range(count))

    def count(model):
        with factory() as session:
            return session.query(model).count()

    yield module, models, factory, add, count
    engine.dispose()
    with create_engine(url).begin() as connection:
        connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))


def test_only_records_older_than_the_period_are_deleted_from_every_pipeline_table(retention, monkeypatch):
    module, models, factory, add, count = retention
    monkeypatch.setattr(module, "CHUNK", 3)  # force several passes over the backlog
    for model in module.TABLES:
        add(model, 7, age_days=40, tag="old")
        add(model, 2, age_days=5, tag="new")

    removed = module.RetentionWorker(factory, 30).prune_once(NOW)

    assert removed == {model.__tablename__: 7 for model in module.TABLES}
    assert all(count(model) == 2 for model in module.TABLES)
    assert module.RetentionWorker(factory, 30).prune_once(NOW) == {}  # repeatable


def test_retention_can_be_disabled(retention):
    module, models, factory, add, count = retention
    add(models.TelemetryEvent, 4, age_days=400, tag="old")
    assert module.RetentionWorker(factory, 0).prune_once(NOW) == {} and count(models.TelemetryEvent) == 4
