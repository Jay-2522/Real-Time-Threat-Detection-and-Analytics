"""Crash/replay verification on a marked disposable hardened deployment only."""
import json
import os
from pathlib import Path
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from test_stack import compose, ROOT

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get('HARDENED_RECOVERY_INTEGRATION') != '1',
    reason='Requires a disposable hardened Kafka/Spark/worker deployment')]


def test_secure_streaming_crash_replay_and_durable_predictions():
    directory = Path(os.environ['HARDENED_SECRETS_DIR']).resolve()
    assert (directory / 'test-only').is_file()
    prefix = 'security-recovery-' + uuid.uuid4().hex[:12]
    checkpoint = '/opt/spark/checkpoints/' + prefix
    # Distinct entity identity prevents separate runs sharing ML window keys.
    entity_ip = '10.' + '.'.join(str(int(prefix[-6:][i:i+2], 16)) for i in (0, 2, 4))
    start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = start.replace(minute=start.minute // 5 * 5).isoformat()
    admin_mount = str(directory / 'kafka-admin') + ':/run/security:ro'

    def kafka(script):
        return compose('run', '--rm', '-T', '-v', admin_mount, '--entrypoint',
                       'python', 'telemetry-producer', '-', input=script, timeout=120)

    common = f'''
import json,time,uuid
from datetime import datetime,timedelta,timezone
from confluent_kafka import Producer,Consumer,TopicPartition
from confluent_kafka.admin import AdminClient,NewTopic
from infrastructure_runtime.security import kafka_options
options={{'bootstrap.servers':'kafka:9092',**kafka_options()}}
prefix={prefix!r}
'''
    kafka(common + '''
names=['login','api','network','anomaly','enriched','window','risk','features']
admin_client=AdminClient(options)
for future in admin_client.create_topics([NewTopic(prefix+'.'+n,1,1) for n in names]).values():future.result(20)
''')
    def seed(shift):
        return json.loads(kafka(common + f'''
p=Producer({{**options,'delivery.timeout.ms':10000}})
start=datetime.fromisoformat({start!r})+timedelta(minutes={shift})
ids=[];errors=[]
for kind,number in [('login',4),('api',4),('network',3),('anomaly',1)]:
 for i in range(number):
  event={{'event_id':str(uuid.uuid4()),'event_type':kind,'timestamp':(start+timedelta(seconds=10)).isoformat(),
   'source_ip':{entity_ip!r},'risk_score':80,'user_id':prefix,'country':'US','login_success':i%2==0,
   'endpoint':'/recovery','status_code':[200,404,500,503][i%4],'response_time_ms':100,
   'bytes_sent':100,'bytes_received':50,'destination_ip':'198.51.100.'+str(i%2+1),'anomaly_type':'recovery','severity':'high'}}
  ids.append(event['event_id'])
  p.produce(prefix+'.'+kind,value=json.dumps(event),on_delivery=lambda e,m:errors.append(e))
 marker={{**event,'event_id':str(uuid.uuid4()),'source_ip':'192.0.2.220','timestamp':(start+timedelta(minutes=10)).isoformat()}}
 p.produce(prefix+'.'+kind,value=json.dumps(marker),on_delivery=lambda e,m:errors.append(e))
assert p.flush(20)==0 and len(errors)==16 and not any(errors)
print(json.dumps(ids))
''').strip())

    # Driver and executors must retain the same scoped Spark keystore/password.
    # Grant only this disposable topic prefix, rather than mounting admin keys
    # into a driver whose executors use a different identity.
    compose('run','--rm','-T','--entrypoint','bash','kafka-init','-ec',
            'kafka-acls --bootstrap-server kafka:9092 --command-config /run/security/client.properties '
            '--add --allow-principal User:CN=spark --operation Read --operation Write '
            '--resource-pattern-type prefixed --topic '+prefix+'.', timeout=120)
    first = seed(0)
    driver = (ROOT / 'tests/spark_recovery/streaming_driver.py').read_text().replace(
        "'/checkpoint", repr(checkpoint)[:-1])
    # The same source/sinks and crash boundary as the legacy recovery regression,
    # with mTLS and the authenticated cluster's exact driver/worker image.
    def run(crash=False):
        compose('run', '--rm', '--use-aliases', '-T',
                '-e', 'RECOVERY_TEST_PREFIX=' + prefix,
                '-e', 'RECOVERY_CRASH=' + str(int(crash)),
                '--entrypoint', '/bin/bash', 'spark-driver', '-ec',
                'cat > /tmp/recovery.py; exec /opt/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/recovery.py',
                input=driver, timeout=600, expected_returncode=77 if crash else 0)

    run(True)
    def metadata():
        return json.loads(compose('run','--rm','-T','--entrypoint','python','spark-driver','-c',f'''
import json
from pathlib import Path
p=Path({checkpoint!r})/'features'
print(json.dumps({{'id':json.loads((p/'metadata').read_text())['id'],
                  'offset':(p/'offsets/1').exists(),'commit':(p/'commits/1').exists()}}))
''').strip())
    before = metadata()
    assert before['offset'] and not before['commit']
    run()
    after = metadata()
    assert after['commit'] and after['id'] == before['id']
    second = seed(20)
    run()
    data = json.loads(kafka(common + '''
result={}
for name in ['enriched','window','risk','features']:
 topic=prefix+'.'+name
 c=Consumer({**options,'group.id':prefix+'-read-'+name,'enable.auto.commit':False})
 end=c.get_watermark_offsets(TopicPartition(topic,0),timeout=10)[1]
 c.assign([TopicPartition(topic,0,0)])
 rows=[];deadline=time.monotonic()+30
 while len(rows)<end and time.monotonic()<deadline:
  m=c.poll(1)
  if m and not m.error():rows.append(json.loads(m.value()))
 c.close();assert len(rows)==end
 result[name]=rows
print(json.dumps(result))
''').strip())
    expected = first + second
    assert sorted(p['event_id'] for p in data['enriched'] if p['event_id'] in expected) == sorted(expected)
    starts = {datetime.fromisoformat(start), datetime.fromisoformat(start) + timedelta(minutes=20)}
    windows = [p for p in data['window'] if datetime.fromisoformat(p['window_start'].replace('Z','+00:00')) in starts]
    assert len(windows)==8 and sorted(p['event_count'] for p in windows)==[1,1,3,3,4,4,4,4]
    features = [p for p in data['features'] if p['entity_id']==entity_ip]
    assert len(features) == 6
    assert len({(p['entity_id'],p['window_start'],p['window_end']) for p in features}) == 4
    for p in features:
        assert p['feature_contract_version']=='2.0' and len(p['features'])==18
        assert p['features']['event_count']==12
        assert p['features']['failed_login_rate']==.5 and p['features']['api_5xx_rate']==.5
        assert sorted(p['source_event_ids']) in [sorted(first), sorted(second)]
    risks = [p for p in data['risk'] if p['entity_id']==entity_ip]
    assert len(risks)==4 and all(p['rolling_event_count']==12 for p in risks)
    # Real ML worker consumes the repeated feature windows; normal scoped storage
    # and alert workers must persist exactly four predictions/alerts.
    kafka(common + f'''
p=Producer({{**options,'delivery.timeout.ms':10000}});errors=[]
for row in {features!r}:
 p.produce('analytics.feature.engineering',value=json.dumps(row),on_delivery=lambda e,m:errors.append(e))
assert p.flush(20)==0 and len(errors)==6 and not any(errors)
''')
    compose('exec','-T','backend','python','-',timeout=150,input=f'''
import time,json,hashlib
from sqlalchemy import create_engine,text
from app.core.settings import settings
from infrastructure_runtime.security import elasticsearch_options
from elasticsearch import Elasticsearch
engine=create_engine(settings.database_url,hide_parameters=True)
es=Elasticsearch(settings.elasticsearch_host,**elasticsearch_options())
ids={[first[0],second[0]]!r}
deadline=time.monotonic()+120
while time.monotonic()<deadline:
 with engine.connect() as c:
  predictions=c.execute(text("SELECT prediction_id FROM anomaly_predictions WHERE CAST(raw_payload->'source_event_ids' AS jsonb) ?| CAST(:ids AS text[])"),{{'ids':ids}}).scalars().all()
  alerts=c.execute(text("SELECT alert_id FROM alerts WHERE raw_payload->>'prediction_id'=ANY(:ids)"),{{'ids':predictions}}).scalars().all() if predictions else []
 if len(predictions)==4 and len(alerts)==4:
  keys=[hashlib.sha256(json.dumps(['ml.anomaly.predictions',pid],separators=(',',':')).encode()).hexdigest() for pid in predictions]
  if all(es.exists(index='ml-predictions',id=key) for key in keys) and all(es.exists(index='alerts',id=aid) for aid in alerts):break
 time.sleep(2)
else:raise AssertionError('Replayed feature windows did not persist exactly four predictions/alerts in SQL and search')
print('Authenticated Spark crash/replay, contract 2.0, ML, SQL and search verified.')
''')


def test_scoped_kafka_no_ack_on_failure_and_process_crash():
    directory=Path(os.environ['HARDENED_SECRETS_DIR']).resolve()
    assert (directory/'test-only').is_file()
    topic='telemetry.security.'+uuid.uuid4().hex
    group='storage-security-'+uuid.uuid4().hex
    event_id=str(uuid.uuid4())
    common=f'''
import os,json,time
from datetime import datetime,timezone
from pathlib import Path
from confluent_kafka import Producer,Consumer,TopicPartition
from confluent_kafka.admin import AdminClient,NewTopic
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from app.main import StorageSinkConsumer,PostgresStorageRepository,ElasticsearchIndexer,get_settings
from infrastructure_runtime.security import kafka_options
from pipeline_runtime.kafka import CONSUMER_OPTIONS,DeliveryLoop,publish_confirmed
from pipeline_runtime.identity import coordinate
settings=get_settings();db=create_engine(settings.database_url,hide_parameters=True)
sink=StorageSinkConsumer(settings,sessionmaker(db),PostgresStorageRepository(),ElasticsearchIndexer(settings))
topic={topic!r};group={group!r}
options={{'bootstrap.servers':'kafka:9092',**kafka_options()}}
admin=json.loads(Path('/run/kafka-test/security.json').read_text())['kafka']
admin={{k:v.replace('/run/security/','/run/kafka-test/') if isinstance(v,str) else v for k,v in admin.items()}}
p=Producer({{'bootstrap.servers':'kafka:9092',**admin,'delivery.timeout.ms':10000}})
wire=json.dumps({{'event_id':{event_id!r},'event_type':'login','source_ip':'192.0.2.211',
 'timestamp':'2026-01-01T00:00:00Z','risk_score':91,'login_success':False}}).encode()
c=Consumer({{**options,**CONSUMER_OPTIONS,'group.id':group}})
loop=DeliveryLoop(c,'security-regression')
def next_message():
 # A crashed member retains its lease until Kafka's normal session timeout.
 deadline=time.monotonic()+75
 while time.monotonic()<deadline:
  m=c.poll(1)
  if m and not m.error():return m
 raise AssertionError('No authenticated Kafka delivery')
'''
    def run(script,exitcode=0):
        return compose('run','--rm','-T','-v',str(directory/'kafka-admin')+':/run/kafka-test:ro',
                '--entrypoint','python','storage-sink','-',input=common+script,
                expected_returncode=exitcode,timeout=180)
    run('''
admin_client=AdminClient({'bootstrap.servers':'kafka:9092',**admin})
for future in admin_client.create_topics([NewTopic(topic,1,1)]).values():future.result(20)
publish_confirmed(p,topic=topic,key='fixture',value=wire)
loop.subscribe([topic]);msg=next_message()
# Terminate this role's own connection inside a transaction: processing must fail
# without a Kafka acknowledgement or a committed partial receipt.
def disconnect(message):
 with db.begin() as connection:
  connection.execute(text('SELECT pg_terminate_backend(pg_backend_pid())'))
try:loop.process(msg,disconnect);raise AssertionError('Disconnected write acknowledged')
except Exception as exc:assert not isinstance(exc,AssertionError)
assert c.committed([TopicPartition(topic,0)])[0].offset<0
sink._handle_message(msg.topic(),msg.value(),coordinate(msg))
os._exit(77) # Real process death after durable SQL, before acknowledgement.
''',77)
    run(f'''
assert c.committed([TopicPartition(topic,0)])[0].offset<0
loop.subscribe([topic]);msg=next_message()
loop.process(msg,lambda m:sink._handle_message(m.topic(),m.value(),coordinate(m)))
assert c.committed([TopicPartition(topic,0)])[0].offset==msg.offset()+1
publish_confirmed(p,topic=topic,key='fixture',value=wire)
msg=next_message()
loop.process(msg,lambda m:sink._handle_message(m.topic(),m.value(),coordinate(m)))
assert c.committed([TopicPartition(topic,0)])[0].offset==msg.offset()+1
with db.connect() as connection:
 assert connection.scalar(text('SELECT count(*) FROM telemetry_events WHERE event_id=:id'),{{'id':{event_id!r}}})==1
c.close()
print('mTLS manual acknowledgement, database failure, process death and duplicate replay passed.')
''')
