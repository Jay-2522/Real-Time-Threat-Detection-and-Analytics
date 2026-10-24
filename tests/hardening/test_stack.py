"""Opt-in tests exclusively against a freshly provisioned, marked isolated stack."""
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import ssl
import subprocess
import time
import tempfile

import httpx
import pytest
from scripts.setup_local import compose_command

ROOT=Path(__file__).resolve().parents[2]
PROJECT=os.environ.get('HARDENED_PROJECT','soc-hardened')
pytestmark=[pytest.mark.integration,pytest.mark.skipif(os.environ.get('HARDENED_INTEGRATION')!='1',reason='Requires isolated hardened deployment')]


def compose(*args,input=None,timeout=90,expected_returncode=0):
    files=['-f',str(ROOT/'deploy/hardened/compose.yml')]
    if os.environ.get('HARDENED_COMPOSE_OVERRIDE'):
        files+=['-f',os.environ['HARDENED_COMPOSE_OVERRIDE']]
    result=subprocess.run([*compose_command(),'-p',PROJECT,*files,*args],input=input,text=True,capture_output=True,timeout=timeout)
    if result.returncode!=expected_returncode:
        # Keep diagnostics private and ignored; never expose credentials in pytest output.
        directory=ROOT/'.codex-logs/hardening'
        directory.mkdir(parents=True,exist_ok=True)
        with tempfile.NamedTemporaryFile(mode='w',dir=directory,prefix='failure-',suffix='.log',delete=False) as log:
            log.write(result.stdout+'\n'+result.stderr)
        raise AssertionError('Isolated deployment command failed; private diagnostics: '+log.name)
    return result.stdout


@pytest.fixture(scope='module')
def stack():
    directory=Path(os.environ['HARDENED_SECRETS_DIR'])
    assert (directory/'test-only').is_file(), 'Refusing to run fixtures/outages against an unmarked deployment'
    assert directory.resolve()!=ROOT/'.secrets'
    # Isolated accounts only; no real administrator is provisioned by deployment setup.
    password=secrets.token_urlsafe(32)
    seed=directory/'admin/test-users.json'
    if seed.exists(): seed.chmod(0o600)
    seed.write_text(json.dumps({'password':password}));seed.chmod(0o444)
    compose('run','--rm','-T','--entrypoint','python','bootstrap','-',input='''
import json,time
from pathlib import Path
from urllib.parse import quote
from datetime import datetime,timezone
from sqlalchemy import create_engine,select
from sqlalchemy.orm import Session
from app.auth.models import User
from app.auth.service import create_user
from app.db.models import Incident,Alert
p=json.loads(Path('/run/admin/bootstrap.json').read_text())['passwords']['postgres']
e=create_engine(f'postgresql+psycopg://postgres:{quote(p)}@postgres:5432/soc?sslmode=verify-full&sslrootcert=/run/admin/ca.crt',hide_parameters=True)
password=json.loads(Path('/run/admin/test-users.json').read_text())['password']
with Session(e) as db,db.begin():
 for role in ('administrator','analyst','viewer'):
  user=db.scalar(select(User).where(User.username=='test-'+role))
  if user:
   from app.auth.service import password_hash
   user.password_hash=password_hash(password)
  else:create_user(db,'test-'+role,password,role,initial=role=='administrator' and not db.scalar(select(User.id).limit(1)))
 now=datetime.now(timezone.utc)
 incident=db.scalar(select(Incident).where(Incident.incident_id=='hardened-test'))
 if incident:incident.status='open'
 else:db.add(Incident(incident_id='hardened-test',created_at=now,updated_at=now,severity='high',title='Isolated hardened incident',description='Security fixture',related_alerts=[],entity_ids=['fixture'],status='open',event_count=1,escalation_level=1,raw_payload={}))
 if not db.scalar(select(Alert).where(Alert.alert_id=='hardened-test')):
  db.add(Alert(alert_id='hardened-test',timestamp=now,created_at=now,severity='high',title='Isolated hardened alert',description='Security fixture',source_topic='test',correlation_id='test',explanation='Fixture',recommended_action='Investigate',status='open',tags=[],raw_payload={}))
print('Isolated account and investigation fixtures ready.')
''')
    yield directory,password


def test_tls_proxy_assets_and_authentication(stack):
    directory,password=stack
    with pytest.raises(httpx.ConnectError): httpx.get('https://localhost:8443/api/v1/health/live')
    with httpx.Client(base_url='https://localhost:8443',verify=ssl.create_default_context(cafile=directory/'web/ca.crt')) as client:
        assert client.get('/').status_code==200
        assert 'Content-Security-Policy' in client.get('/').headers
        assert client.get('/api/v1/alerts/recent').status_code==401
        response=client.post('/api/v1/auth/login',json={'username':'test-analyst','password':password},headers={'Origin':'https://localhost:8443','X-Requested-With':'SOC'})
        assert response.status_code==200 and 'Secure' in response.headers['set-cookie'] and '__Host-soc_session=' in response.headers['set-cookie']
        assert client.get('/api/v1/dashboard/overview').status_code==200
        assert client.get('/api/v1/alerts/recent').status_code==200
        headers={'Origin':'https://localhost:8443','X-CSRF-Token':response.json()['csrf_token']}
        assert client.patch('/api/v1/incidents/hardened-test/status',json={'status':'investigating'},headers=headers).status_code==200
        assert client.post('/api/v1/auth/logout',headers=headers).status_code==204
        assert client.get('/api/v1/alerts/recent').status_code==401


def test_service_tls_authentication_and_authorization(stack):
    compose('exec','-T','backend','python','-',input='''
import ssl,json,urllib.request,urllib.error
import redis
from elasticsearch import Elasticsearch,AuthorizationException
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
from app.core.settings import settings
from infrastructure_runtime.security import redis_options,elasticsearch_options
try:
 redis.Redis(host='redis',port=6379,ssl=True,ssl_ca_certs='/run/security/ca.crt',ssl_check_hostname=True).ping()
 raise AssertionError('Anonymous Redis access succeeded')
except redis.exceptions.AuthenticationError:pass
r=redis.Redis.from_url(settings.redis_url,**redis_options())
assert r.ping()
try:r.flushall();raise AssertionError('Read identity executed FLUSHALL')
except redis.exceptions.NoPermissionError:pass
ctx=ssl.create_default_context(cafile='/run/security/ca.crt')
try:urllib.request.urlopen('https://elasticsearch:9200',context=ctx);raise AssertionError('Anonymous Elasticsearch access succeeded')
except urllib.error.HTTPError as exc:assert exc.code==401
es=Elasticsearch(settings.elasticsearch_host,**elasticsearch_options())
assert es.info()['version']['number']
try:es.index(index='alerts',id='denied-write',document={'secret':'not-written'});raise AssertionError('Read identity wrote an index')
except AuthorizationException:pass
with create_engine(settings.database_url,hide_parameters=True).connect() as db:
 assert db.scalar(text('SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()'))
 assert db.scalar(text('SELECT current_user'))=='backend'
 try:db.execute(text('CREATE TABLE forbidden_table (id int)'));raise AssertionError('Runtime identity has DDL privileges')
 except SQLAlchemyError:db.rollback()
try:
 with create_engine(settings.database_url.replace('sslmode=verify-full','sslmode=disable'),hide_parameters=True).connect():pass
 raise AssertionError('Plaintext database connection succeeded')
except SQLAlchemyError:pass
print('Redis ACL, Elasticsearch RBAC, PostgreSQL least privilege and verified TLS passed.')
''')


def test_isolated_readiness_outage_preserves_liveness(stack):
    directory,_=stack
    with httpx.Client(base_url='https://localhost:8443',verify=ssl.create_default_context(cafile=directory/'web/ca.crt'),timeout=8) as client:
        assert client.get('/api/v1/health/ready').status_code==200
        compose('stop','postgres')
        try:
            assert client.get('/api/v1/health/ready').status_code==503
            assert client.get('/api/v1/health/live').json()=={'status':'alive'}
        finally:compose('start','postgres')
        for _ in range(30):
            if client.get('/api/v1/health/ready').status_code==200:break
            time.sleep(.5)
        else:raise AssertionError('Readiness did not recover')


def test_network_paths_and_published_ports(stack):
    config=json.loads(compose('--profile','*','config','--format','json'))
    for service in config['services']:
        ids=compose('ps','-q',service).strip()
        if not ids:continue
        details=json.loads(subprocess.check_output(['docker','inspect',ids],text=True))[0]
        bindings=details['HostConfig']['PortBindings'] or {}
        assert (service=='web') == bool(bindings)
    pg=json.loads(subprocess.check_output(['docker','inspect',compose('ps','-q','postgres').strip()],text=True))[0]
    address=next(iter(pg['NetworkSettings']['Networks'].values()))['IPAddress']
    probe=subprocess.run(['docker','run','--rm','--network',PROJECT+'_web','--cap-drop=ALL','--security-opt=no-new-privileges','soc-hardened-backend','python','-c',
        f"import socket;s=socket.socket();s.settimeout(2);assert s.connect_ex(('{address}',5432))!=0"],capture_output=True,timeout=15)
    assert probe.returncode==0


def test_browser_secure_cookie_websocket_workflow(stack):
    from playwright.sync_api import sync_playwright,expect
    directory,password=stack
    pem=subprocess.check_output(['openssl','x509','-in',str(directory/'web/server.crt'),'-pubkey','-noout'])
    public_der=subprocess.check_output(['openssl','pkey','-pubin','-outform','DER'],input=pem)
    pin=base64.b64encode(hashlib.sha256(public_der).digest()).decode()
    with sync_playwright() as p:
        # Pin only this isolated leaf key; do not alter the host trust store or ignore all TLS errors.
        browser=p.chromium.launch(args=['--ignore-certificate-errors-spki-list='+pin])
        page=browser.new_page()
        frames=[]
        page.on('websocket',lambda ws:ws.on('framereceived',lambda payload:frames.append(payload)))
        page.goto('https://localhost:8443')
        page.get_by_label('Username',exact=True).fill('test-analyst')
        page.get_by_label('Password',exact=True).fill(password)
        page.get_by_role('button',name='Sign in',exact=True).click()
        expect(page.get_by_role('button',name='Sign out')).to_be_visible()
        page.get_by_role('link',name='Alerts',exact=True).first.click()
        expect(page.get_by_text('Isolated hardened alert',exact=True).first).to_be_visible()
        page.get_by_role('link',name='Incidents',exact=True).first.click()
        page.get_by_label('incidents hardened-test status').first.select_option('resolved')
        expect(page.get_by_text('Status saved.',exact=True).first).to_be_visible()
        assert frames, 'Authenticated WebSocket did not stream through HTTPS proxy'
        assert all(c['secure'] and c['httpOnly'] for c in page.context.cookies() if c['name']=='__Host-soc_session')
        page.get_by_role('button',name='Sign out').click()
        expect(page.get_by_role('button',name='Sign in',exact=True)).to_be_visible()
        browser.close()


def test_monitoring_requires_identity_and_runtime_is_nonroot(stack):
    compose('exec','-T','backend','python','-',input="""
import os,base64,urllib.request,urllib.error
from infrastructure_runtime.security import section
assert os.geteuid()!=0
try:
 urllib.request.urlopen('http://prometheus:9090/api/v1/query?query=up',timeout=5)
 raise AssertionError('Anonymous metrics access succeeded')
except urllib.error.HTTPError as exc:assert exc.code==401
credentials=section('prometheus')
request=urllib.request.Request('http://prometheus:9090/api/v1/query?query=up')
request.add_header('Authorization','Basic '+base64.b64encode((credentials['username']+':'+credentials['password']).encode()).decode())
assert urllib.request.urlopen(request,timeout=5).status==200
try:open('/app/forbidden','w');raise AssertionError('Runtime root filesystem writable')
except OSError:pass
""")


@pytest.mark.skipif(os.environ.get('HARDENED_WORKERS_INTEGRATION')!='1',reason='Requires isolated secured workers')
@pytest.mark.parametrize('service,role,index,namespace',[
    ('storage-sink','storage','deadletter-events','storage'),
    ('alert-engine','alerts','alerts','alert'),
    ('threat-intelligence-engine','threat','attack-timelines','threat')])
def test_worker_scoped_clients_and_cache_projection(stack,service,role,index,namespace):
    compose('exec','-T',service,'python','-',input=f"""
import os,uuid
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
from elasticsearch import Elasticsearch,AuthorizationException
import redis
from app.config.settings import get_settings
from infrastructure_runtime.security import redis_options,elasticsearch_options
assert os.geteuid()==10001
settings=get_settings()
with create_engine(settings.database_url,hide_parameters=True).connect() as db:
 assert db.scalar(text('SELECT current_user'))=={role!r}
 assert db.scalar(text('SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()'))
 try:db.execute(text('SELECT password_hash FROM auth_users'));raise AssertionError('Worker read authentication secrets')
 except SQLAlchemyError:db.rollback()
 try:
  db.execute(text("INSERT INTO pipeline_receipts(service,key,fingerprint) VALUES ('forbidden',:key,'probe')"),{{'key':uuid.uuid4().hex}})
  raise AssertionError('Worker wrote another pipeline namespace')
 except SQLAlchemyError:db.rollback()
es=Elasticsearch('https://elasticsearch:9200',**elasticsearch_options('https://elasticsearch:9200'))
key='security-'+uuid.uuid4().hex
assert es.index(index={index!r},id=key,document={{'security_probe':True}})['result']=='created'
try:es.index(index='forbidden-security-index',id=key,document={{}});raise AssertionError('Worker wrote outside its index scope')
except AuthorizationException:pass
r=redis.Redis(host='redis',port=6379,ssl=True,**redis_options())
assert r.ping()
if {namespace!r} in ('storage','alert'):
 from pipeline_runtime.cache import cache_once
 namespace={namespace!r}
 latest=('latest:alert:' if namespace=='alert' else 'recent:high-risk:')+key
 counters=[('counters:alerts:entity:' if namespace=='alert' else 'counters:events:')+key]
 cache_once(r,namespace,key,latest,'2026-10-17T00:00:00Z',{{'security_probe':True,'timestamp':'2026-10-17T00:00:00Z','alert_id':key}},counters,list_key='live:critical-alerts' if namespace=='alert' else '')
 cache_once(r,namespace,key,latest,'2026-10-17T00:00:00Z',{{'security_probe':True}},counters)
 assert int(r.get(counters[0]))==1 and r.get(latest)
if {namespace!r}=='threat':
 r.setex('ioc:security:'+key,60,'probe')
 try:r.setex('latest:alert:'+key,60,'forbidden');raise AssertionError('Threat worker changed alert cache')
 except redis.exceptions.NoPermissionError:pass
print('Scoped SQL, search and replay-safe cache permissions passed.')
""")
