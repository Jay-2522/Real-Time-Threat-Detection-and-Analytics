"""Explicit isolated-schema/service-role bootstrap; does not provision human accounts."""
import json
from pathlib import Path
from urllib.parse import quote
from sqlalchemy import create_engine, text
from elasticsearch import Elasticsearch
from app.db.models import Base

def main():
    values = json.loads(Path('/run/admin/bootstrap.json').read_text())['passwords']
    url=f"postgresql+psycopg://postgres:{quote(values['postgres'])}@postgres:5432/soc?sslmode=verify-full&sslrootcert=/run/admin/ca.crt"
    engine=create_engine(url, hide_parameters=True)
    Base.metadata.create_all(engine)
    with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as connection:
        for file in sorted(Path('/setup/migrations').glob('*.sql')):
            connection.exec_driver_sql(file.read_text())
    with engine.begin() as db:
        db.execute(text('REVOKE CREATE ON SCHEMA public FROM PUBLIC'))
        db.execute(text('REVOKE ALL ON DATABASE soc FROM PUBLIC'))
        for name in ('backend','storage','alerts','threat'):
            # Names are a fixed inventory; passwords use driver parameterization via set_config.
            if not db.execute(text('SELECT 1 FROM pg_roles WHERE rolname=:name'),{'name':name}).scalar():
                db.execute(text(f'CREATE ROLE "{name}" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION'))
                db.execute(text("SELECT set_config('soc.provision_password', :password, true)"),{'password':values[name]})
                db.execute(text(f"DO $$ BEGIN EXECUTE format('ALTER ROLE {name} PASSWORD %L', current_setting('soc.provision_password')); END $$"))
            db.execute(text(f'GRANT CONNECT ON DATABASE soc TO "{name}"'))
            db.execute(text(f'GRANT USAGE ON SCHEMA public TO "{name}"'))
        db.execute(text('GRANT SELECT ON ALL TABLES IN SCHEMA public TO backend'))
        db.execute(text('GRANT INSERT, UPDATE ON auth_users, auth_sessions, auth_control, auth_login_throttle, auth_audit TO backend'))
        db.execute(text('GRANT UPDATE ON alerts, incidents TO backend'))
        tables={
          'storage':['telemetry_events','analytics_metrics','risk_metrics','feature_snapshots','anomaly_predictions','deadletter_events'],
          'alerts':['alerts','incidents','incident_alert_links','alert_dedup_cache'],
          'threat':['threat_intel_events','ioc_matches','entity_profiles','detection_rule_hits','attack_timelines']}
        for name,names in tables.items():
            for table in names+['pipeline_receipts','pipeline_outbox','pipeline_quarantine']:
                if db.execute(text('SELECT to_regclass(:table)'),{'table':table}).scalar():
                    db.execute(text(f'GRANT SELECT, INSERT, UPDATE ON {table} TO "{name}"'))
            db.execute(text(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{name}"'))
        # The storage sink prunes pipeline records past RETENTION_DAYS; no other worker may delete.
        db.execute(text('GRANT DELETE ON '+', '.join(tables['storage'])+' TO storage'))
        for table in ('pipeline_receipts','pipeline_outbox','pipeline_quarantine'):
            db.execute(text(f'ALTER TABLE {table} ENABLE ROW LEVEL SECURITY'))
            for role,condition in (('backend','true'),('storage',"service = 'storage'"),('alerts',"service = 'alert'"),('threat',"service = 'threat'")):
                policy='soc_'+role
                if not db.execute(text('SELECT 1 FROM pg_policies WHERE tablename=:table AND policyname=:policy'),{'table':table,'policy':policy}).scalar():
                    db.execute(text(f'CREATE POLICY {policy} ON {table} TO {role} USING ({condition}) WITH CHECK ({condition})'))
    es=Elasticsearch('https://elasticsearch:9200',basic_auth=('elastic',values['elastic']),ca_certs='/run/admin/ca.crt',request_timeout=10)
    for name in ('backend','storage','alerts','threat'):
        patterns={'backend':['telemetry-*','analytics-*','ml-*','alerts','incidents','threat-*','ioc-*','entity-*','detection-*','attack-*'],
                  'storage':['telemetry-*','analytics-*','ml-*','deadletter-*'], 'alerts':['alerts','incidents'],
                  'threat':['threat-*','ioc-*','entity-*','detection-*','attack-*']}[name]
        es.security.put_role(name='soc_'+name,cluster=['monitor'],indices=[{'names':patterns,'privileges':['read','view_index_metadata'] if name=='backend' else ['read','write','create_index','view_index_metadata']}])
        es.security.put_user(username=name,password=values[name],roles=['soc_'+name])
    print('Schema and scoped service identities provisioned; no human accounts created.')

if __name__ == '__main__':
    try: main()
    except Exception: raise SystemExit('Infrastructure bootstrap failed; credential-bearing diagnostics withheld.')
