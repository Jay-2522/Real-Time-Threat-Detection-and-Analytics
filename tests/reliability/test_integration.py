import json
import os
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy import text, select, update
from sqlalchemy.orm import Session
from confluent_kafka import TopicPartition, Producer
from pipeline_runtime.durable import Receipt, Outbox, OutboxWorker, claim, enqueue
from pipeline_runtime.identity import logical_key, coordinate
from pipeline_runtime.kafka import publish_confirmed
from support import engine, storage, alerts, kafka_case, consumer, next_message, send, event, ROOT

pytestmark=[pytest.mark.integration, pytest.mark.skipif(os.environ.get('PIPELINE_INTEGRATION')!='1',reason='Set PIPELINE_INTEGRATION=1 for real local infrastructure tests')]


@pytest.fixture
def db():
    db=engine()
    with db.connect() as c: c.execute(text('SELECT 1 FROM pipeline_receipts LIMIT 0'))
    yield db
    db.dispose()


def count(db,table,column,value):
    with db.connect() as c: return c.scalar(text(f'SELECT count(*) FROM {table} WHERE {column}=:value'),{'value':value})


def test_real_database_disconnect_no_ack_then_retry(db):
    topic,group=kafka_case(); p=event(); send(topic,json.dumps(p).encode())
    c,loop=consumer(topic,group); msg=next_message(c)
    try:
        def fail(message):
            with Session(db) as session,session.begin():
                claim(session,'disconnect-test',logical_key(topic,p),p)
                pid=session.scalar(text('select pg_backend_pid()'))
                with db.connect().execution_options(isolation_level='AUTOCOMMIT') as killer:
                    killer.execute(text('select pg_terminate_backend(:pid)'),{'pid':pid})
                session.execute(text('SELECT 1'))
        with pytest.raises(Exception): loop.process(msg,fail)
        assert c.committed([TopicPartition(topic,0)])[0].offset < 0
        sink=storage(db)
        loop.process(msg,lambda m:sink._handle_message(m.topic(),m.value(),coordinate(m)))
        assert c.committed([TopicPartition(topic,0)])[0].offset==msg.offset()+1
        assert count(db,'telemetry_events','event_id',p['event_id'])==1
    finally: c.close()


def test_real_crash_restart_duplicate_delivery(db):
    topic,group=kafka_case();p=event();wire=json.dumps(p).encode();send(topic,wire)
    env={**os.environ,'TEST_TOPIC':topic,'TEST_GROUP':group,'TEST_MODE':'crash'}
    r=subprocess.run([sys.executable,str(Path(__file__).with_name('crash_worker.py'))],env=env,capture_output=True,timeout=40)
    assert r.returncode==77,r.stderr.decode()
    assert count(db,'telemetry_events','event_id',p['event_id'])==1
    c,loop=consumer(topic,group)
    try:
        assert c.committed([TopicPartition(topic,0)])[0].offset<0
        msg=next_message(c);sink=storage(db)
        loop.process(msg,lambda m:sink._handle_message(m.topic(),m.value(),coordinate(m)))
        send(topic,wire);duplicate=next_message(c)
        loop.process(duplicate,lambda m:sink._handle_message(m.topic(),m.value(),coordinate(m)))
        assert c.committed([TopicPartition(topic,0)])[0].offset==duplicate.offset()+1
        assert count(db,'telemetry_events','event_id',p['event_id'])==1
    finally:c.close()


def test_concurrent_alerts_and_replay_do_not_inflate_incident(db):
    consumer_obj=alerts(db);topic='ml.anomaly.predictions'
    entity='reliability-'+uuid.uuid4().hex
    p={'prediction_id':str(uuid.uuid4()),'entity_id':entity,'event_type':'feature_window','ml_risk_score':95,
       'timestamp':datetime.now(timezone.utc).isoformat()}
    wire=json.dumps(p).encode()
    with ThreadPoolExecutor(2) as pool: list(pool.map(lambda _:consumer_obj._handle_message(topic,wire),range(2)))
    consumer_obj._handle_message(topic,wire)
    assert count(db,'alerts','entity_id',entity)==1
    p2={**p,'prediction_id':str(uuid.uuid4())}
    consumer_obj._handle_message(topic,json.dumps(p2).encode())
    assert count(db,'alerts','entity_id',entity)==2
    with db.connect() as c:
        rows=c.execute(text("SELECT event_count FROM incidents WHERE entity_ids::jsonb @> CAST(:entities AS jsonb)"),{'entities':json.dumps([entity])}).all()
        assert rows==[(2,)]
        assert c.scalar(text('SELECT count(*) FROM incident_alert_links l JOIN alerts a ON a.alert_id=l.alert_id WHERE a.entity_id=:e'),{'e':entity})==2


def test_outbox_recovers_real_elasticsearch_failures(db):
    from elasticsearch import Elasticsearch
    key=uuid.uuid4().hex; namespace='integration-'+key[:12]
    with Session(db) as session,session.begin():
        assert claim(session,namespace,key,{'id':key})
        enqueue(session,namespace,key,'es',{'id':key})
    bad_es=Elasticsearch('http://127.0.0.1:1',request_timeout=.2,max_retries=0)
    handlers={'es':lambda k,p:bad_es.index(index='reliability-regression',id=k,document=p)}
    worker=OutboxWorker(lambda:Session(db),namespace,handlers);worker.drain_once()
    with Session(db) as session,session.begin():
        rows=session.scalars(select(Outbox).where(Outbox.service==namespace)).all()
        assert len(rows)==1 and all(not r.done and r.attempts==1 for r in rows)
        for r in rows:r.next_attempt=datetime.now(timezone.utc)-timedelta(seconds=1)
    es=Elasticsearch('http://localhost:9200')
    handlers['es']=lambda k,p:es.index(index='reliability-regression',id=k,document=p)
    worker.drain_once()
    # Repeat the effects as if the worker crashed before marking them done.
    handlers['es'](key,{'id':key})
    assert es.get(index='reliability-regression',id=key)['_source']=={'id':key}
    with Session(db) as session: assert all(r.done for r in session.scalars(select(Outbox).where(Outbox.service==namespace)))


def test_kafka_publish_failure_is_retryable_and_invalid_storage_is_durable(db):
    topic,group=kafka_case();send(topic,b'not-json')
    c,loop=consumer(topic,group);msg=next_message(c)
    bad=Producer({'bootstrap.servers':'127.0.0.1:1','delivery.timeout.ms':1000,'socket.timeout.ms':1000})
    try:
        with pytest.raises(RuntimeError): loop.process(msg,lambda m:publish_confirmed(bad,topic=topic,key='k',value=m.value(),timeout=3))
        assert c.committed([TopicPartition(topic,0)])[0].offset<0
        sink=storage(db)
        loop.process(msg,lambda m:sink._handle_message(m.topic(),m.value(),coordinate(m)))
        assert c.committed([TopicPartition(topic,0)])[0].offset==msg.offset()+1
        with db.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM deadletter_events WHERE raw_payload->>'source_topic'=:topic"),{'topic':topic})==1
    finally:c.close()


def test_actual_partition_revocation_replays_completed_database_write(db):
    topic,group=kafka_case();p=event();send(topic,json.dumps(p).encode())
    c,loop=consumer(topic,group);msg=next_message(c);sink=storage(db)
    try:
        def revoke_after_write(message):
            sink._handle_message(message.topic(),message.value(),coordinate(message))
            c.unsubscribe()
            c.poll(1)  # Serve the real revocation callback before acknowledgement.
        with pytest.raises(Exception):loop.process(msg,revoke_after_write)
        assert c.committed([TopicPartition(topic,0)])[0].offset<0
    finally:c.close()
    restarted,loop2=consumer(topic,group)
    try:
        replay=next_message(restarted)
        loop2.process(replay,lambda m:sink._handle_message(m.topic(),m.value(),coordinate(m)))
        assert restarted.committed([TopicPartition(topic,0)])[0].offset==replay.offset()+1
        assert count(db,'telemetry_events','event_id',p['event_id'])==1
    finally:restarted.close()


def test_distinct_feature_windows_and_payload_conflict(db):
    sink=storage(db);entity='192.0.2.212';run=uuid.uuid4().hex
    p={'entity_id':entity,'event_type':'feature_window','entity_type':'source_ip','feature_contract_version':'2.0',
       'window_start':'2026-01-01T00:00:00Z','window_end':'2026-01-01T00:01:00Z','source_event_ids':[run]}
    # Unique topic isolates this permanent test without deleting existing data.
    topic='analytics.feature.engineering.'+run
    for payload in [p,p,{**p,'window_start':'2026-01-01T00:00:30Z','window_end':'2026-01-01T00:01:30Z'}]:
        sink._handle_message(topic,json.dumps(payload).encode())
    assert count(db,'telemetry_events','source_topic',topic)==2
    sink._handle_message(topic,json.dumps({**p,'source_event_ids':['conflicting']}).encode())
    assert count(db,'telemetry_events','source_topic',topic)==2
    with db.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM deadletter_events WHERE raw_payload->>'source_topic'=:topic"),{'topic':topic})==1


def test_real_ml_delivery_duplicate_identity_and_dlq(db):
    from dotenv import dotenv_values
    import importlib
    from support import module
    settings_env=dotenv_values(ROOT/'.env')
    # Reuse the Phase 1 deterministic contract fixture, not a second contract definition.
    sys.path.insert(0,str(ROOT/'ml-models/tests'))
    from test_feature_windows import payload
    p=payload();p['entity_id']='192.0.2.213';p['source_event_ids']=[uuid.uuid4().hex for _ in range(12)]
    m=module('ml-models','app.main')
    output,group=kafka_case()
    settings=importlib.import_module('app.config.settings').MLSettings(
        ML_ARTIFACT_DIR=Path(settings_env.get('DEV_BIND_ROOT',str(ROOT)))/'ml-models/artifacts/contract-v2',
        ML_PREDICTION_TOPIC=output, deadletter_topic=output)
    artifacts=m.load_artifacts(settings.artifact_dir)
    inference=m.MLInferenceConsumer(settings,m.AnomalyPredictor(settings,artifacts))
    inference._producer=Producer({'bootstrap.servers':'127.0.0.1:9092','broker.address.family':'v4','delivery.timeout.ms':5000})
    input_topic='test-features-'+uuid.uuid4().hex
    wire=json.dumps(p).encode()
    inference._handle_message(input_topic,wire)
    inference._handle_message(input_topic,wire)
    # A different intentional sliding window must retain its own prediction identity.
    other={**p,'window_start':'2026-01-01T11:59:30Z','window_end':'2026-01-01T12:00:30Z'}
    inference._handle_message(input_topic,json.dumps(other).encode())
    inference._handle_message(input_topic,b'invalid',{'topic':input_topic,'partition':0,'offset':4})
    c,loop=consumer(output,group)
    try: results=[json.loads(next_message(c).value()) for _ in range(4)]
    finally:c.close()
    assert results[0]['prediction_id']==results[1]['prediction_id']
    assert results[0]['prediction_id']!=results[2]['prediction_id']
    assert results[0]['model_artifact_id'] and results[0]['features_used']['failed_login_rate']==.5
    assert results[3]['event_type']=='deadletter' and results[3]['raw_hex']==b'invalid'.hex()
    sink=storage(db)
    for result in results[:3]:sink._handle_message('ml.anomaly.predictions',json.dumps(result).encode())
    assert count(db,'anomaly_predictions','prediction_id',results[0]['prediction_id'])==1
    assert count(db,'anomaly_predictions','prediction_id',results[2]['prediction_id'])==1
