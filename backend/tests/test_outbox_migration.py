"""Redis outbox cleanup preserves the remaining Elasticsearch work."""
import os
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get("POSTGRES_INTEGRATION") != "1",
    reason="Set POSTGRES_INTEGRATION=1 with a disposable PostgreSQL database",
)]


def test_remove_redis_outbox_migration_is_repeatable():
    import psycopg
    from psycopg import sql
    from dotenv import dotenv_values

    root = Path(__file__).resolve().parents[2]
    env = {**dotenv_values(root / ".env"), **os.environ}
    schema = "outbox_test_" + uuid4().hex
    with psycopg.connect(
        host=env.get("PGHOST", "127.0.0.1"),
        port=env.get("PGPORT", env.get("POSTGRES_PUBLISHED_PORT", "5432")),
        dbname=env.get("PGDATABASE", env["POSTGRES_DB"]),
        user=env.get("PGUSER", env["POSTGRES_USER"]),
        password=env.get("PGPASSWORD", env["POSTGRES_PASSWORD"]),
        autocommit=True,
    ) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            db.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            db.execute("""CREATE TABLE pipeline_outbox (
                service text, key text, target text, payload jsonb, done boolean,
                PRIMARY KEY (service, key, target))""")
            db.execute("""INSERT INTO pipeline_outbox VALUES
                ('storage', 'pending', 'redis', '{}', false),
                ('alert', 'done', 'redis', '{}', true),
                ('storage', 'search', 'elasticsearch', '{}', false)""")
            migration = (root / "migrations/004_remove_redis_outbox.sql").read_text()
            db.execute(migration)
            db.execute(migration)
            assert db.execute(
                "SELECT service, key, target, done FROM pipeline_outbox ORDER BY key"
            ).fetchall() == [("storage", "search", "elasticsearch", False)]
        finally:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
