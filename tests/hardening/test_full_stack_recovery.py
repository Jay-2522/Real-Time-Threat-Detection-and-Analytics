"""Real process failures on a fresh marked hardened deployment; no legacy endpoints."""
import json
import os
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pytest
from test_stack import compose, ROOT, PROJECT, stack

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get('HARDENED_FULL_STACK_RECOVERY') != '1',
    reason='Requires dedicated hardened recovery project, fresh state and all workers')]


def marked():
    directory = Path(os.environ['HARDENED_SECRETS_DIR']).resolve()
    assert (directory/'test-only').is_file()
    assert PROJECT.startswith('soc-phase4a'), 'Failure injection requires a dedicated recovery project'
    return directory


def remote(service, script, timeout=180):
    return compose('exec','-T',service,'python','-',input=script,timeout=timeout)


def test_pipeline_driver_and_dependency_recovery(stack):
    directory = marked()
    run = uuid.uuid4().hex
    ip = '192.0.2.181'
    start = datetime.now(timezone.utc).replace(second=0,microsecond=0)
    start = start.replace(minute=start.minute//5*5)
    # Expected BEFORE publication: 2 batches x (4 login + 4 API + 3 network + 1 anomaly),
    # two overlapping 60s feature/risk windows per batch; 4 type windows per batch.
    expected = {'source_events':24,'feature_windows':4,'risk_windows':4,'type_windows':8,
                'predictions':4,'enriched_events':24,'target_alerts':44,'target_incidents':4,'threat_inputs':46}
    ids = [[str(uuid.uuid5(uuid.NAMESPACE_URL,f'{run}:{batch}:{i}')) for i in range(12)] for batch in range(2)]
    common = f'''
import json,time,uuid
from datetime import datetime,timezone,timedelta
from confluent_kafka import Producer
from infrastructure_runtime.security import kafka_options
from pipeline_runtime.kafka import publish_confirmed
run={run!r};ip={ip!r};ids={ids!r};base=datetime.fromisoformat({start.isoformat()!r})
'''
    def seed(batch):
        script = common + f'''
p=Producer({{'bootstrap.servers':'kafka:9092',**kafka_options(),'delivery.timeout.ms':10000,'enable.idempotence':True}})
start=base+timedelta(minutes={batch*20});i=0;coordinates=[]
for kind,n in [('login',4),('api',4),('network',3),('anomaly',1)]:
 for j in range(n):
  row={{'event_id':ids[{batch}][i],'event_type':kind,'timestamp':(start+timedelta(seconds=10)).isoformat(),
   'source_ip':ip,'risk_score':80,'user_id':run,'country':'US','username':'phase4a','device_type':'desktop',
   'login_success':j%2==0,'endpoint':'/phase4a','method':'GET','status_code':[200,404,500,503][j%4],
   'response_time_ms':100,'request_size':64,'bytes_sent':100,'bytes_received':50,
   'destination_ip':'198.51.100.'+str(j%2+1),'protocol':'TCP','port':443,'packet_count':1,
   'anomaly_type':'phase4a','severity':'high','description':'Deterministic isolated recovery fixture'}}
  topic='telemetry.'+kind+'.events'
  p.produce(topic,key=row['event_id'],value=json.dumps(row),callback=lambda e,m:coordinates.append((str(e) if e else None,m.topic(),m.partition(),m.offset())))
  i+=1
 # Watermark advancement is explicit synthetic control input, excluded from the 24 target events.
 marker={{**row,'event_id':str(uuid.uuid5(uuid.NAMESPACE_URL,run+'-marker-'+str({batch})+'-'+kind)),'source_ip':'192.0.2.250',
         'timestamp':(start+timedelta(minutes=10)).isoformat()}}
 publish_confirmed(p,topic=topic,key=marker['event_id'],value=json.dumps(marker))
assert p.flush(20)==0 and len(coordinates)==12 and not any(c[0] for c in coordinates)
print(json.dumps(coordinates))
'''
        return json.loads(compose('run','--rm','-T','--entrypoint','python','telemetry-producer','-',input=script,timeout=90))

    def snapshot():
        return json.loads(remote('spark-driver', '''
import json
from pathlib import Path
result={}
for p in Path('/opt/spark/checkpoints').iterdir():
 if not p.is_dir() or not (p/'metadata').is_file():continue
 result[p.name]={'id':json.loads((p/'metadata').read_text())['id'],
   'commits':sorted(int(x.name) for x in (p/'commits').glob('*') if x.name.isdigit()),
   'offsets':sorted(int(x.name) for x in (p/'offsets').glob('*') if x.name.isdigit())}
print(json.dumps(result))
'''))

    def reconcile():
        return json.loads(remote('backend', f'''
import json,time
from sqlalchemy import create_engine,text
from app.core.settings import settings
engine=create_engine(settings.database_url,hide_parameters=True)
ids={sum(ids,[])!r}
with engine.connect() as db:
 predictions=db.execute(text("SELECT prediction_id,raw_payload FROM anomaly_predictions WHERE CAST(raw_payload->'source_event_ids' AS jsonb) ?| CAST(:ids AS text[])"),{{'ids':ids}}).all()
 enriched=db.scalar(text("SELECT count(*) FROM analytics_metrics WHERE source_topic='analytics.enriched.events' AND event_id=ANY(:ids)"),{{'ids':ids}})
print(json.dumps({{'predictions':[{{'id':p[0],'payload':p[1]}} for p in predictions],'enriched':enriched}}))
'''))

    def await_predictions(count, sources):
        deadline=time.monotonic()+300
        while time.monotonic()<deadline:
            result=reconcile()
            if len(result['predictions'])==count and result['enriched']==sources:return result
            time.sleep(3)
        raise AssertionError(f'Expected {count} predictions and {sources} enriched events')

    first_coordinates=seed(0)
    compose('up','-d','--no-build','spark-driver')
    first=await_predictions(2,12)
    before=snapshot();assert len(before)==4 and all(v['commits'] for v in before.values())
    # Actual driver process/container is killed; restart uses its current volume.
    compose('kill','-s','SIGKILL','spark-driver')
    compose('up','-d','--no-build','spark-driver')
    for _ in range(60):
        try:
            after=snapshot()
            if len(after)==4 and {k:v['id'] for k,v in after.items()}=={k:v['id'] for k,v in before.items()}:break
        except AssertionError:pass
        time.sleep(2)
    else:raise AssertionError('Driver checkpoint identities changed or failed to resume')

    # Dependency outages overlap the next input. SQL remains authoritative while
    # search is down; SQL failures must leave input offsets replayable.
    compose('stop','elasticsearch','postgres')
    try:
        second_coordinates=seed(1)
        time.sleep(5)
    finally:
        compose('start','postgres')
    compose('up','-d','--no-build','storage-sink','alert-engine','threat-intelligence-engine')
    final=await_predictions(4,24)
    pending=remote('backend', '''
from sqlalchemy import create_engine,text
from app.core.settings import settings
with create_engine(settings.database_url,hide_parameters=True).connect() as db:
 assert db.scalar(text('SELECT count(*) FROM pipeline_outbox WHERE NOT done'))>0
print('Durable outbox remains pending during external outage')
''')
    compose('kill','-s','SIGKILL','storage-sink','alert-engine','threat-intelligence-engine')
    compose('start','elasticsearch')
    compose('up','-d','--no-build','storage-sink','alert-engine','threat-intelligence-engine')
    remote('backend', f'''
import json,time
from sqlalchemy import create_engine,text
from elasticsearch import Elasticsearch
from app.core.settings import settings
from infrastructure_runtime.security import elasticsearch_options
import hashlib
def logical_key(topic,payload):
 return hashlib.sha256(json.dumps([topic,str(payload['prediction_id'])],sort_keys=True,separators=(',',':')).encode()).hexdigest()
engine=create_engine(settings.database_url,hide_parameters=True)
es=Elasticsearch(settings.elasticsearch_host,**elasticsearch_options())
ids={sum(ids,[])!r};deadline=time.monotonic()+300
while time.monotonic()<deadline:
 with engine.connect() as db:
  rows=db.execute(text("SELECT prediction_id,raw_payload FROM anomaly_predictions WHERE CAST(raw_payload->'source_event_ids' AS jsonb) ?| CAST(:ids AS text[])"),{{'ids':ids}}).all()
  pending=db.scalar(text('SELECT count(*) FROM pipeline_outbox WHERE NOT done'))
  alerts=db.execute(text('SELECT alert_id,incident_id FROM alerts WHERE entity_id=:ip'),{{'ip':'192.0.2.181'}}).all()
 try:
  if len(rows)==4 and pending==0 and len(alerts)==44 and all(es.exists(index='alerts',id=a[0]) and es.exists(index='incidents',id=a[1]) for a in alerts) and all(es.exists(index='ml-predictions',id=logical_key('ml.anomaly.predictions',row[1])) for row in rows):break
 except Exception:pass
 time.sleep(2)
else:raise AssertionError('Database/search/outbox failed to converge')
print('Replay-safe SQL/search convergence verified')
''',timeout=330)
    observed=json.loads(remote('backend', f'''
import json
from sqlalchemy import create_engine,text
from app.core.settings import settings
ids={sum(ids,[])!r};ip={ip!r}
starts={[start.isoformat(),(start+timedelta(minutes=20)).isoformat()]!r}
expected_features={{'event_count':12,'login_event_count':4,'api_event_count':4,'network_event_count':3,'anomaly_event_count':1,
 'failed_login_rate':.5,'failed_auth_count':2,'requests_per_minute':4,'avg_api_response_time':100,'endpoint_error_rate':.75,
 'api_5xx_rate':.5,'avg_network_bytes':150,'high_risk_network_event_count':3,'anomaly_rate':1/12,
 'risk_score_moving_average':80,'high_risk_event_count':12,'distinct_login_country_count':1,'unique_destination_ip_count':2}}
with create_engine(settings.database_url,hide_parameters=True).connect() as db:
 features=db.execute(text("SELECT raw_payload FROM feature_snapshots WHERE raw_payload->>'entity_id'=:ip"),{{'ip':ip}}).scalars().all()
 assert len(features)==4
 for row in features:
  assert row['feature_contract_version']=='2.0' and row['features']==expected_features
  assert sorted(row['source_event_ids']) in {[sorted(group) for group in ids]!r}
 risks=db.execute(text("SELECT raw_payload FROM risk_metrics WHERE raw_payload->>'entity_id'=:ip"),{{'ip':ip}}).scalars().all()
 assert len(risks)==4 and all(r['rolling_event_count']==12 for r in risks)
 windows=db.execute(text("SELECT raw_payload FROM analytics_metrics WHERE source_topic='analytics.window.metrics'")).scalars().all()
 from datetime import datetime
 windows=[r for r in windows if datetime.fromisoformat(r['window_start'].replace('Z','+00:00')).isoformat() in starts]
 assert len(windows)==8 and sorted(r['event_count'] for r in windows)==[1,1,3,3,4,4,4,4]
 predictions=db.execute(text("SELECT raw_payload FROM anomaly_predictions WHERE CAST(raw_payload->'source_event_ids' AS jsonb) ?| CAST(:ids AS text[])"),{{'ids':ids}}).scalars().all()
 prediction_ids=[r['prediction_id'] for r in predictions]
 alerts=db.execute(text("SELECT alert_id,incident_id FROM alerts WHERE entity_id=:ip"),{{'ip':ip}}).all()
 assert len(alerts)==44 and len({{r[0] for r in alerts}})==44
 assert len({{r[1] for r in alerts}})==4
 # Receipt cardinality proves duplicates did not reapply threat SQL side effects.
 threat=db.scalar(text("SELECT count(*) FROM threat_intel_events WHERE raw_payload->>'event_id'=ANY(:ids) OR raw_payload->>'entity_id'=:ip"),{{'ids':ids,'ip':ip}})
 assert threat==46
 print(json.dumps({{'features':4,'risk':4,'type_windows':8,'predictions':len(predictions),'alerts':[r[0] for r in alerts],
                    'incidents':sorted({{r[1] for r in alerts}}),'threat_inputs':threat}}))
''',timeout=120))
    import httpx,ssl
    _,password=stack
    with httpx.Client(base_url='https://localhost:8443',verify=ssl.create_default_context(cafile=directory/'web/ca.crt')) as client:
        assert client.get('/api/v1/alerts/recent').status_code==401
        response=client.post('/api/v1/auth/login',json={'username':'test-analyst','password':password},headers={'Origin':'https://localhost:8443','X-Requested-With':'SOC'})
        assert response.status_code==200
        rows=client.get('/api/v1/alerts/recent?limit=200').json()
        assert set(observed['alerts']) <= {r['alert_id'] for r in rows}
    # Keep deterministic evidence local; no generated source IDs/checkpoints enter Git.
    evidence=ROOT/'.codex-logs/phase4a'
    evidence.mkdir(parents=True,exist_ok=True)
    (evidence/('full-stack-reconciliation-'+PROJECT+'.json')).write_text(json.dumps({
        'run':run,'expected':expected,'source_ids':ids,'coordinates':first_coordinates+second_coordinates,
        'before':before,'after':snapshot(),'observed_predictions':final['predictions'],'observed':observed},indent=2))


def test_separate_worker_loss_reexecutes_task_with_shared_state():
    directory=marked()
    from concurrent.futures import ThreadPoolExecutor
    prefix='worker-recovery-'+uuid.uuid4().hex[:10]
    checkpoint='/opt/spark/checkpoints/'+prefix
    # Independent topics make the test deterministic even if the full application is running.
    admin=str(directory/'kafka-admin')+':/run/security:ro'
    compose('run','--rm','-T','-v',admin,'--entrypoint','python','telemetry-producer','-',input=f'''
import json,uuid
from datetime import datetime,timezone,timedelta
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient,NewTopic,AclBinding,ResourceType,ResourcePatternType,AclOperation,AclPermissionType
from infrastructure_runtime.security import kafka_options
options={{'bootstrap.servers':'kafka:9092',**kafka_options()}}
a=AdminClient(options);prefix={prefix!r}
for future in a.create_topics([NewTopic(prefix+'.'+n,1,1) for n in ['login','api','network','anomaly','enriched','window','risk','features']]).values():future.result(20)
acls=[AclBinding(ResourceType.TOPIC,prefix+'.',ResourcePatternType.PREFIXED,'User:CN=spark','*',op,AclPermissionType.ALLOW) for op in [AclOperation.READ,AclOperation.WRITE]]
for future in a.create_acls(acls).values():future.result(20)
p=Producer(options);start=datetime.now(timezone.utc).replace(second=10,microsecond=0)
for kind in ['login','api','network','anomaly']:
 for shift in [0,10]:
  row={{'event_id':str(uuid.uuid4()),'event_type':kind,'timestamp':(start+timedelta(minutes=shift)).isoformat(),
   'source_ip':'192.0.2.182','risk_score':80,'user_id':prefix,'country':'US','login_success':False,
   'endpoint':'/worker','status_code':500,'response_time_ms':100,'bytes_sent':100,'bytes_received':50,
   'destination_ip':'198.51.100.1','anomaly_type':'worker','severity':'high'}}
  p.produce(prefix+'.'+kind,value=json.dumps(row))
assert p.flush(20)==0
''',timeout=120)
    driver=(ROOT/'tests/spark_recovery/streaming_driver.py').read_text().replace("'/checkpoint",repr(checkpoint)[:-1])
    # Test-only task stalls after a real stateful batch is evaluated, providing an
    # observable window for killing the separate worker. No application sleep hooks.
    driver=driver.replace("            wire.write.format('kafka')", f'''            if name == 'features':
                def witness(rows):
                    import os,socket,time,json
                    from pathlib import Path
                    from pyspark import TaskContext
                    context=TaskContext.get()
                    root=Path({checkpoint!r})
                    rows=list(rows)
                    if rows:
                        key=f'attempt-{{batch_id}}-{{context.stageId()}}-{{context.partitionId()}}-{{context.taskAttemptId()}}-{{os.getpid()}}'
                        (root/key).write_text(json.dumps({{'host':socket.gethostname(),'batch':batch_id,'stage':context.stageId(),'partition':context.partitionId(),'attempt':context.attemptNumber()}}))
                    if rows and not (root/'worker-killed').exists():
                        (root/'worker-active').write_text(socket.gethostname())
                        time.sleep(30)
                    yield from rows
                wire=spark.createDataFrame(wire.rdd.mapPartitions(witness),wire.schema)
            wire.write.format('kafka')''')
    def run():
        compose('run','--rm','--use-aliases','-T','-e','RECOVERY_TEST_PREFIX='+prefix,
                '--entrypoint','bash','spark-driver','-ec',
                'cat >/tmp/worker-recovery.py; exec /opt/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/worker-recovery.py',input=driver,timeout=600)
    # Stop the ordinary driver to reserve this small VM's worker for the fault test.
    compose('stop','spark-driver')
    before=compose('ps','-q','spark-worker').strip()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(run)
            for _ in range(120):
                result=compose('exec','-T','spark-worker','python','-c',f'from pathlib import Path;print((Path({checkpoint!r})/"worker-active").exists())').strip()
                if result=='True':break
                if future.done():future.result();raise AssertionError('No separate-worker task witness')
                time.sleep(1)
            else:raise AssertionError('Task did not reach observable worker boundary')
            witness_host=compose('exec','-T','spark-worker','python','-c',f'from pathlib import Path;print((Path({checkpoint!r})/"worker-active").read_text())').strip()
            assert before.startswith(witness_host), 'Task witness did not run in the separate worker'
            compose('kill','-s','SIGKILL','spark-worker')
            compose('up','-d','--no-build','--force-recreate','spark-worker')
            after=compose('ps','-q','spark-worker').strip();assert after!=before
            # The checkpoint volume survived recreation; release the retried task.
            compose('exec','-T','spark-worker','python','-c',f'from pathlib import Path;p=Path({checkpoint!r});assert (p/"worker-active").exists();(p/"worker-killed").write_text("test-only")')
            future.result(timeout=480)
        evidence=json.loads(compose('exec','-T','spark-worker','python','-c',f'''
import json
from pathlib import Path
p=Path({checkpoint!r});attempts=[f.name for f in p.glob('attempt-*')]
assert len(attempts)>=2
rows=[json.loads(f.read_text()) for f in p.glob('attempt-*')]
assert any(r['attempt']>0 for r in rows) or any(a['batch']==b['batch'] and a['partition']==b['partition'] and a['host']!=b['host'] for a in rows for b in rows), 'No task or stage re-execution across worker replacement'
assert len({{r['host'] for r in rows}})>=2
assert all((p/name/'metadata').is_file() and any((p/name/'commits').glob('[0-9]*')) for name in ['enriched','window','risk','features'])
print(json.dumps({{'attempts':rows,'queries':{{name:json.loads((p/name/'metadata').read_text())['id'] for name in ['enriched','window','risk','features']}}}}))
'''))
        # Resume those same four queries after publishing new records; query UUIDs
        # and durable state must survive a second separate-driver application too.
        compose('run','--rm','-T','-v',admin,'--entrypoint','python','telemetry-producer','-',input=f'''
import json,uuid
from datetime import datetime,timezone,timedelta
from confluent_kafka import Producer
from infrastructure_runtime.security import kafka_options
p=Producer({{'bootstrap.servers':'kafka:9092',**kafka_options()}})
start=datetime.now(timezone.utc).replace(second=10,microsecond=0)+timedelta(minutes=20)
for kind in ['login','api','network','anomaly']:
 for shift in [0,10]:
  row={{'event_id':str(uuid.uuid4()),'event_type':kind,'timestamp':(start+timedelta(minutes=shift)).isoformat(),
   'source_ip':'192.0.2.182','risk_score':80,'user_id':{prefix!r},'country':'US','login_success':False,
   'endpoint':'/worker','status_code':500,'response_time_ms':100,'bytes_sent':100,'bytes_received':50,
   'destination_ip':'198.51.100.1','anomaly_type':'worker','severity':'high'}}
  p.produce({prefix!r}+'.'+kind,value=json.dumps(row))
assert p.flush(20)==0
''')
        run()
        resumed=json.loads(compose('exec','-T','spark-worker','python','-c',f'''
import json
from pathlib import Path
p=Path({checkpoint!r})
print(json.dumps({{name:json.loads((p/name/'metadata').read_text())['id'] for name in ['enriched','window','risk','features']}}))
'''))
        assert resumed==evidence['queries']
        compose('run','--rm','-T','-v',admin,'--entrypoint','python','telemetry-producer','-',input=f'''
import json,time,uuid
from confluent_kafka import Consumer,TopicPartition
from infrastructure_runtime.security import kafka_options
options={{'bootstrap.servers':'kafka:9092',**kafka_options()}}
def rows(name):
 c=Consumer({{**options,'group.id':'admin-'+uuid.uuid4().hex,'enable.auto.commit':False}})
 topic={prefix!r}+'.'+name;end=c.get_watermark_offsets(TopicPartition(topic,0),timeout=10)[1]
 c.assign([TopicPartition(topic,0,0)]);result=[];deadline=time.monotonic()+30
 while len(result)<end and time.monotonic()<deadline:
  m=c.poll(1)
  if m and not m.error():result.append(json.loads(m.value()))
 c.close();assert len(result)==end;return result
source={{r['event_id'] for name in ['login','api','network','anomaly'] for r in rows(name)}}
assert len(source)==16
assert {{r['event_id'] for r in rows('enriched')}}==source
features=rows('features')
assert len({{(r['entity_id'],r['window_start'],r['window_end']) for r in features}})==6
assert all(r['feature_contract_version']=='2.0' and len(r['features'])==18 and r['features']['event_count']==4 for r in features)
print('Worker loss, task retry, all four continued queries and new-event lineage verified')
''',timeout=120)
        (ROOT/'.codex-logs/phase4a/worker-recovery.json').write_text(json.dumps(evidence,indent=2))
    finally:
        compose('up','-d','--no-build','spark-worker','spark-driver')


def test_publish_confirmation_failure_and_broker_reconnection():
    marked()
    from concurrent.futures import ThreadPoolExecutor
    key=uuid.uuid4().hex
    # Telemetry validator identity may publish only the existing DLQ.
    script=f'''
import json,time
from pathlib import Path
from confluent_kafka import Producer
from infrastructure_runtime.security import kafka_options
from pipeline_runtime.kafka import publish_confirmed
key={key!r};root=Path('/tmp')
p=Producer({{'bootstrap.servers':'kafka:9092',**kafka_options(),'delivery.timeout.ms':3000}})
p.list_topics(timeout=10)
(root/(key+'-ready')).write_text('ready')
while not (root/(key+'-disconnected')).exists():time.sleep(.1)
row={{'event_id':key,'event_type':'deadletter','timestamp':'2026-10-18T00:00:00Z','source_topic':'phase4a-confirmation','reason':'synthetic outage','payload':{{'test':key}}}}
wire=json.dumps(row)
try:
 publish_confirmed(p,topic='telemetry.deadletter.events',key=key,value=wire,timeout=5)
 raise AssertionError('Publication was falsely confirmed during paused broker')
except RuntimeError:pass
(root/(key+'-failed')).write_text('unconfirmed')
while not (root/(key+'-reconnected')).exists():time.sleep(.1)
publish_confirmed(p,topic='telemetry.deadletter.events',key=key,value=wire,timeout=15)
publish_confirmed(p,topic='telemetry.deadletter.events',key=key,value=wire,timeout=15)
print('Real failed confirmation, reconnection and duplicate redelivery verified')
'''
    def flag(suffix,write=False):
        return remote('telemetry-producer',f'from pathlib import Path;p=Path("/tmp/{key}-{suffix}");'+('p.write_text("test")' if write else 'print(p.exists())')).strip()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(remote,'telemetry-producer',script,120)
        for _ in range(40):
            if flag('ready')=='True':break
            time.sleep(.5)
        else:raise AssertionError('Producer did not connect before outage')
        compose('pause','kafka')
        try:
            flag('disconnected',True)
            for _ in range(20):
                if flag('failed')=='True':break
                time.sleep(.5)
            else:raise AssertionError('Failed publication was not observed')
        finally:compose('unpause','kafka')
        flag('reconnected',True)
        future.result(timeout=60)
    remote('backend',f'''
import time
from sqlalchemy import create_engine,text
from app.core.settings import settings
engine=create_engine(settings.database_url,hide_parameters=True)
for _ in range(60):
 with engine.connect() as db:
  count=db.scalar(text('SELECT count(*) FROM deadletter_events WHERE event_id=:id'),{{'id':{key!r}}})
 if count==1:break
 time.sleep(1)
else:raise AssertionError('Confirmed redelivery did not persist')
assert count==1
''',timeout=75)


def test_reconciled_source_offsets_and_replay_safe_cache():
    marked()
    evidence=json.loads((ROOT/'.codex-logs/phase4a'/('full-stack-reconciliation-'+PROJECT+'.json')).read_text())
    ids=sum(evidence['source_ids'],[])
    observed=evidence['observed']
    # Validate the persisted raw inputs as well as transformed products.
    remote('backend',f'''
from sqlalchemy import create_engine,text
from app.core.settings import settings
with create_engine(settings.database_url,hide_parameters=True).connect() as db:
 assert db.scalar(text('SELECT count(*) FROM telemetry_events WHERE event_id=ANY(:ids)'),{{'ids':{ids!r}}})==24
 assert db.scalar(text('SELECT count(*) FROM pipeline_outbox WHERE NOT done'))==0
''')
    required={}
    for _,topic,partition,offset in evidence['coordinates']:
        required.setdefault(topic,{})[str(partition)]=max(required.get(topic,{}).get(str(partition),0),offset+2)
    # Each source also has one explicitly recorded watermark-control record per batch.
    offsets=json.loads(remote('spark-driver','''
import json
from pathlib import Path
result={}
for root in Path('/opt/spark/checkpoints').iterdir():
 if not (root/'metadata').is_file():continue
 logs=[p for p in (root/'offsets').glob('*') if p.name.isdigit()]
 last=max(logs,key=lambda p:int(p.name));sources=last.read_text().splitlines()[2:]
 result[root.name]=[json.loads(x) for x in sources if x!='-']
print(json.dumps(result))
'''))
    assert len(offsets)==4
    for rows in offsets.values():
        merged={topic:parts for row in rows for topic,parts in row.items()}
        assert merged==required

def test_duplicate_enriched_delivery_keeps_threat_alert_and_incident_effects_unique():
    directory=marked()
    evidence=json.loads((ROOT/'.codex-logs/phase4a'/('full-stack-reconciliation-'+PROJECT+'.json')).read_text())
    event_id=evidence['source_ids'][0][0]
    payload=json.loads(remote('backend',f'''
import json
from sqlalchemy import create_engine,text
from app.core.settings import settings
with create_engine(settings.database_url,hide_parameters=True).connect() as db:
 row=db.execute(text("SELECT raw_payload FROM analytics_metrics WHERE source_topic='analytics.enriched.events' AND event_id=:id"),{{'id':{event_id!r}}}).scalar_one()
 print(json.dumps(row))
'''))
    # Authorized test-only redelivery of the same Spark product; no raw-event inflation.
    compose('run','--rm','-T','-v',str(directory/'kafka-admin')+':/run/security:ro','--entrypoint','python','telemetry-producer','-',input=f'''
import json,time
from confluent_kafka import Producer,ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient
from infrastructure_runtime.security import kafka_options
from pipeline_runtime.kafka import publish_confirmed
options={{'bootstrap.servers':'kafka:9092',**kafka_options()}}
p=Producer(options);row={payload!r};offsets=[]
for _ in range(3):p.produce('analytics.enriched.events',key=row['event_id'],value=json.dumps(row),callback=lambda e,m:offsets.append((e,m.offset())))
assert p.flush(20)==0 and len(offsets)==3 and not any(x[0] for x in offsets)
end=max(x[1] for x in offsets)+1;a=AdminClient(options)
for group in ['storage-hardened','alerts-hardened','threat-hardened']:
 deadline=time.monotonic()+60
 while time.monotonic()<deadline:
  rows=a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)]).get(group).result(10).topic_partitions
  if any(x.topic=='analytics.enriched.events' and x.offset>=end for x in rows):break
  time.sleep(1)
 else:raise AssertionError('Redelivery not durably acknowledged by '+group)
''',timeout=210)
    remote('backend',f'''
from sqlalchemy import create_engine,text
from app.core.settings import settings
ids={sum(evidence['source_ids'],[])!r};ip='192.0.2.181'
with create_engine(settings.database_url,hide_parameters=True).connect() as db:
 assert db.scalar(text("SELECT count(*) FROM analytics_metrics WHERE source_topic='analytics.enriched.events' AND event_id=ANY(:ids)"),{{'ids':ids}})==24
 assert db.scalar(text('SELECT count(*) FROM alerts WHERE entity_id=:ip'),{{'ip':ip}})==44
 assert db.scalar(text("SELECT count(*) FROM threat_intel_events WHERE raw_payload->>'event_id'=ANY(:ids) OR raw_payload->>'entity_id'=:ip"),{{'ids':ids,'ip':ip}})==46
 threat_ids=db.execute(text("SELECT event_id FROM threat_intel_events WHERE raw_payload->>'event_id'=ANY(:ids) OR raw_payload->>'entity_id'=:ip"),{{'ids':ids,'ip':ip}}).scalars().all()
from elasticsearch import Elasticsearch
from infrastructure_runtime.security import elasticsearch_options
es=Elasticsearch(settings.elasticsearch_host,**elasticsearch_options())
assert len(threat_ids)==46 and all(es.exists(index='threat-intel-events',id=key+':threat-intel-events') for key in threat_ids)
''')
