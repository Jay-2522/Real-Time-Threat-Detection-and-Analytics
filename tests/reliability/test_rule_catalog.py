"""The threat engine must report the rules it loaded to PostgreSQL, and keep that report exact."""
import importlib
import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get("POSTGRES_INTEGRATION") != "1",
    reason="Set POSTGRES_INTEGRATION=1 with a disposable PostgreSQL database",
)]

ROOT = Path(__file__).resolve().parents[2]


def rule(rule_id, enabled=True, name=None):
    return {"rule_id": rule_id, "name": name or rule_id.title(), "severity": "high", "enabled": enabled,
            "tactic": "Credential Access", "technique": "Brute Force", "mitre_id": "T1110", "description": "d"}


@pytest.fixture
def catalog():
    from dotenv import dotenv_values
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL
    from sqlalchemy.orm import sessionmaker

    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(ROOT / "threat-intelligence-engine"))
    models = importlib.import_module("app.db.models")
    repository = importlib.import_module("app.repositories.threat_repository").ThreatRepository()
    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    schema = "rule_catalog_" + uuid4().hex
    url = URL.create("postgresql+psycopg", username=env.get("PGUSER", env.get("POSTGRES_USER")),
                     password=env.get("PGPASSWORD", env.get("POSTGRES_PASSWORD")), host=env.get("PGHOST", "127.0.0.1"),
                     port=int(env.get("PGPORT", env.get("POSTGRES_PUBLISHED_PORT", "5432"))),
                     database=env.get("PGDATABASE", env.get("POSTGRES_DB")))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    models.Base.metadata.create_all(engine)
    factory = sessionmaker(engine)

    def sync(rules):
        with factory() as session, session.begin():
            repository.sync_rule_catalog(session, rules)

    def stored():
        with factory() as session:
            return {row.rule_id: row for row in session.query(models.DetectionRuleCatalog)}

    yield sync, stored
    engine.dispose()
    with create_engine(url).begin() as connection:
        connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))


def test_the_catalog_mirrors_the_rules_the_engine_just_loaded(catalog):
    sync, stored = catalog
    sync([rule("brute_force"), rule("geo_login", enabled=False), rule("beaconing")])
    assert {k: v.enabled for k, v in stored().items()} == {"brute_force": True, "geo_login": False, "beaconing": True}

    first_loaded = stored()["brute_force"].loaded_at
    sync([rule("brute_force", name="Renamed"), rule("geo_login", enabled=True)])  # a restart with an edited rule set
    after = stored()
    assert set(after) == {"brute_force", "geo_login"}  # beaconing was removed from disk, so it is gone from the report
    assert after["brute_force"].name == "Renamed" and after["geo_login"].enabled is True
    assert after["brute_force"].loaded_at >= first_loaded

    sync([])  # no rule files at all: the report is empty, not stale
    assert stored() == {}
