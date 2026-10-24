import importlib
import json
import os
from pathlib import Path
import subprocess
import pytest
import yaml
from scripts.setup_local import compose_command

ROOT=Path(__file__).resolve().parents[2]
COMPOSE=ROOT/'deploy/hardened/compose.yml'


def test_hardened_compose_requires_secret_directory():
    env={k:v for k,v in os.environ.items() if k!='HARDENED_SECRETS_DIR'}
    result=subprocess.run([*compose_command(),'-f',str(COMPOSE),'config','--quiet'],env=env,capture_output=True,text=True)
    assert result.returncode != 0 and 'HARDENED_SECRETS_DIR' in result.stderr


def test_hardened_runtime_has_no_development_overrides():
    config=yaml.safe_load(COMPOSE.read_text())
    for name,service in config['services'].items():
        assert 'env_file' not in service and not service.get('privileged')
        if name!='web': assert not service.get('ports')
        assert service.get('cap_drop') == ['ALL']
        if name!='permissions': assert not str(service.get('user','')).startswith('0:')
        assert '--reload' not in str(service.get('command'))
        assert not any('/app:' in v or 'checkpoints' in v and 'streaming-analytics' in v for v in service.get('volumes',[]))
    assert config['services']['backend']['environment']['AUTH_LOCAL_HTTP']=='false'
    assert config['services']['backend']['environment']['ENVIRONMENT']=='production'
    assert config['services']['web']['ports']==['127.0.0.1:8443:8443']
    assert all(v.get('internal') for k,v in config['networks'].items() if k!='edge')
    assert all('edge' not in v.get('networks',{}) for k,v in config['services'].items() if k!='web')
    assert all(not v or 'name' not in v for v in config['volumes'].values())


def test_production_transport_settings_fail_closed(tmp_path,monkeypatch):
    from infrastructure_runtime import security
    security.configuration.cache_clear()
    monkeypatch.setenv('ENVIRONMENT','production')
    monkeypatch.delenv('SERVICE_SECURITY_FILE',raising=False)
    with pytest.raises(RuntimeError,match='required'): security.configuration()
    path=tmp_path/'credentials.json';path.write_text(json.dumps({'database_url':'postgresql+psycopg://user:password@db/soc?sslmode=disable','kafka':{'security.protocol':'PLAINTEXT'}}))
    monkeypatch.setenv('SERVICE_SECURITY_FILE',str(path));security.configuration.cache_clear()
    with pytest.raises(RuntimeError,match='verified TLS'): security.database_url('unused')
    with pytest.raises(RuntimeError,match='mutually authenticated'): security.kafka_options()
    with pytest.raises(RuntimeError,match='Missing required'): security.redis_options()
    with pytest.raises(RuntimeError,match='HTTPS'): security.elasticsearch_options('http://search:9200')
    security.configuration.cache_clear()
