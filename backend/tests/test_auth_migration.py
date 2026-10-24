"""Additive PostgreSQL migration over a copy of representative existing records."""
import os
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get('POSTGRES_INTEGRATION') != '1',
    reason='Set POSTGRES_INTEGRATION=1 with a disposable PostgreSQL database',
)]


def test_auth_migration_preserves_history_and_constraints():
    import psycopg
    from psycopg import sql
    from dotenv import dotenv_values
    root = Path(__file__).resolve().parents[2]
    env = {**dotenv_values(root/'.env'), **os.environ}
    schema = 'auth_test_' + uuid4().hex
    with psycopg.connect(host=env.get('PGHOST', '127.0.0.1'), port=env.get('PGPORT', env.get('POSTGRES_PUBLISHED_PORT', '5432')),
                          dbname=env.get('PGDATABASE', env['POSTGRES_DB']), user=env.get('PGUSER', env['POSTGRES_USER']),
                          password=env.get('PGPASSWORD', env['POSTGRES_PASSWORD']), autocommit=True) as db:
        db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        try:
            db.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            before = {}
            for table in ('telemetry_events','anomaly_predictions','alerts','incidents'):
                db.execute(sql.SQL('CREATE TABLE {} AS SELECT * FROM public.{} LIMIT 5').format(sql.Identifier(table), sql.Identifier(table)))
                before[table] = db.execute(sql.SQL('SELECT row_to_json(t) FROM {} t').format(sql.Identifier(table))).fetchall()
            migration = (root/'migrations/003_authentication.sql').read_text()
            db.execute(migration)
            db.execute(migration)  # Repeat safely without reopening bootstrap.
            for table, rows in before.items():
                assert rows == db.execute(sql.SQL('SELECT row_to_json(t) FROM {} t').format(sql.Identifier(table))).fetchall()
            assert db.execute('SELECT count(*) FROM auth_users').fetchone()[0] == 0
            db.execute("INSERT INTO auth_users VALUES ('test','fixture','not-a-real-password-hash','administrator',true,1)")
            with pytest.raises(psycopg.errors.UniqueViolation):
                db.execute("INSERT INTO auth_users VALUES ('test2','fixture','hash','viewer',true,1)")
            with pytest.raises(psycopg.errors.CheckViolation):
                db.execute("INSERT INTO auth_users VALUES ('bad','invalid','hash','root',true,1)")
            with pytest.raises(psycopg.errors.ForeignKeyViolation):
                db.execute("INSERT INTO auth_sessions VALUES ('digest','missing',1,2,1,false)")
            db.execute('UPDATE auth_control SET bootstrapped=true')
            db.execute(migration)
            assert db.execute('SELECT bootstrapped FROM auth_control').fetchone()[0]
        finally:
            db.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
