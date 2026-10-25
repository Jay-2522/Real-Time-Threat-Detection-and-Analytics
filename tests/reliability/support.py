import importlib
import os
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from dotenv import dotenv_values
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pipeline_runtime.kafka import CONSUMER_OPTIONS, DeliveryLoop, publish_confirmed

ROOT = Path(__file__).resolve().parents[2]


def module(service, name):
    for key in list(sys.modules):
        if key == "app" or key.startswith("app."):
            del sys.modules[key]
    sys.path.insert(0, str(ROOT / service))
    return importlib.import_module(name)


def engine():
    env = dotenv_values(ROOT / '.env')
    return create_engine(URL.create('postgresql+psycopg', username=env['POSTGRES_USER'], password=env['POSTGRES_PASSWORD'],
        host='127.0.0.1', port=int(env.get('POSTGRES_PUBLISHED_PORT',15432)), database=env['POSTGRES_DB']),
        pool_pre_ping=True, connect_args={'connect_timeout':3, 'options':'-c statement_timeout=10000 -c lock_timeout=5000'})


def storage(db):
    m = module('storage-sink', 'app.main')
    settings = m.get_settings()
    factory = sessionmaker(db)
    indexer = m.ElasticsearchIndexer(settings)
    return m.StorageSinkConsumer(settings, factory, m.PostgresStorageRepository(), indexer)


def alerts(db):
    m = module('alert-engine', 'app.main')
    settings = m.get_settings()
    service = m.AlertProcessingService(m.AlertRulesEngine(), m.IncidentCorrelator(settings),
        m.AlertRepository(), m.AlertIndexer(settings), settings.dedup_cooldown_seconds)
    return m.AlertEngineConsumer(settings, sessionmaker(db), service)


def kafka_case():
    name = 'reliability-' + uuid.uuid4().hex
    admin=AdminClient({'bootstrap.servers':'localhost:9092'})
    admin.create_topics([NewTopic(name, 1, 1)])[name].result(20)
    return name, name + '-group'


def consumer(topic, group):
    c = Consumer({'bootstrap.servers':'localhost:9092','group.id':group, **CONSUMER_OPTIONS, 'session.timeout.ms':6000})
    loop = DeliveryLoop(c, 'regression')
    loop.subscribe([topic])
    return c, loop


def next_message(c, timeout=25):
    until = time.monotonic()+timeout
    while time.monotonic()<until:
        message=c.poll(1)
        if message and not message.error():
            return message
    raise AssertionError('No Kafka message received within deadline')


def send(topic, value):
    p = Producer({'bootstrap.servers':'localhost:9092', 'delivery.timeout.ms':5000, 'enable.idempotence':True})
    publish_confirmed(p, topic=topic, key='test', value=value)


def event():
    from datetime import datetime, timezone
    return {'event_id':str(uuid.uuid4()), 'event_type':'login', 'source_ip':'192.0.2.211',
            'user_id':'reliability-'+uuid.uuid4().hex, 'timestamp':datetime.now(timezone.utc).isoformat(),
            'risk_score':91, 'login_success':False}
