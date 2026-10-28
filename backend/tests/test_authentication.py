"""Security boundaries use real authentication and isolated persisted accounts."""
import secrets
import time
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, update
from starlette.websockets import WebSocketDisconnect
from test_api_contracts import load_backend_app

ORIGIN = {'Origin': 'http://localhost:5173', 'X-Requested-With': 'SOC'}


@pytest.fixture
def auth(tmp_path):
    app = load_backend_app(tmp_path)
    from app.auth.models import User
    from app.auth.service import create_user
    from app.db.models import Alert, Incident, Base
    from app.db.session import SessionLocal, engine
    Base.metadata.create_all(engine)
    password = secrets.token_urlsafe(24)
    ids = {}
    with SessionLocal() as db, db.begin():
        for role in ('administrator', 'analyst', 'viewer'):
            ids[role] = create_user(db, role, password, role, initial=role == 'administrator', actor=ids.get('administrator')).id
        now = datetime.now(timezone.utc)
        db.add(Incident(incident_id='test-incident', created_at=now, updated_at=now, severity='high',
                        title='Isolated incident', description='Security regression fixture', related_alerts=['test-alert'], entity_ids=['fixture'],
                        status='open', event_count=1, escalation_level=1, raw_payload={}))
        db.add(Alert(alert_id='test-alert', timestamp=now, created_at=now, severity='high', title='Isolated alert',
                     description='Security regression fixture', source_topic='test', correlation_id='test-rule:fixture', explanation='Fixture',
                     entity_id='fixture', ml_risk_score=82.0, incident_id='test-incident',
                     recommended_action='Investigate', status='open', tags=[], raw_payload={}))
    with TestClient(app, base_url='http://localhost') as client:
        yield app, client, SessionLocal, password, ids


def login(auth, role='viewer'):
    _, client, _, password, _ = auth
    response = client.post('/api/v1/auth/login', json={'username':role, 'password':password}, headers=ORIGIN)
    assert response.status_code == 200
    client.headers.update({**ORIGIN, 'X-CSRF-Token': response.json()['csrf_token']})
    return response


def test_password_session_fixation_logout_and_audit(auth):
    from app.auth.models import User, LoginSession, Audit
    from app.auth.service import digest
    _, client, factory, password, _ = auth
    first = login(auth)
    old = client.cookies.get('soc_session_local')
    second = login(auth)
    token = client.cookies.get('soc_session_local')
    assert old != token
    assert 'HttpOnly' in second.headers['set-cookie'] and 'SameSite=strict' in second.headers['set-cookie']
    with factory() as db:
        assert db.get(LoginSession, digest(old)).revoked
        assert db.get(LoginSession, digest(token)).token_hash != token
        assert db.scalar(select(User).where(User.username == 'viewer')).password_hash.startswith('$argon2id$')
    assert client.post('/api/v1/auth/logout').status_code == 204
    client.cookies.set('soc_session_local', token)
    assert client.get('/api/v1/alerts/recent').status_code == 401
    with factory() as db:
        actions = [a.action for a in db.scalars(select(Audit))]
        assert 'login' in actions and 'logout' in actions and 'initial_administrator_created' in actions
        assert password not in str([a.__dict__ for a in db.scalars(select(Audit))])


def test_all_routes_deny_anonymous_and_policy_is_explicit(auth):
    from app.auth.policy import permission
    app, client, *_ = auth
    for route in app.routes:
        for method in getattr(route, 'methods', set()) - {'HEAD', 'OPTIONS'}:
            if route.path in ('/api/v1/health', '/api/v1/health/live', '/api/v1/health/ready', '/api/v1/auth/login', '/metrics'): continue
            path = route.path.replace('{entity_id}', 'fixture').replace('{incident_id}', 'fixture').replace('{alert_id}', 'fixture').replace('{rule_id}', 'fixture').replace('{user_id}', 'fixture')
            assert client.request(method, path).status_code == 401, (method, path)
            assert permission(path, method, '/api/v1') is not None, (method, path)
    login(auth)
    assert client.get('/api/v1/unclassified').status_code == 403
    assert client.post('/api/v1/auth/register', json={}).status_code == 403


def test_viewer_reads_but_cannot_mutate_or_escalate(auth):
    _, client, factory, _, ids = auth
    login(auth)
    for path in ('alerts/recent', 'incidents/recent', 'dashboard/overview', 'threat-intel/overview', 'predictions/recent', 'monitoring/overview'):
        assert client.get('/api/v1/' + path).status_code == 200
    for path in ('alerts/test-alert/status', 'incidents/test-incident/status'):
        assert client.patch('/api/v1/' + path, json={'status':'resolved'}).status_code == 403
    assert client.get('/api/v1/auth/users').status_code == 403
    assert client.patch('/api/v1/auth/users/' + ids['viewer'], json={'role':'administrator', 'active':True}).status_code == 403
    from app.auth.models import Audit
    from app.db.models import Incident
    with factory() as db:
        assert db.scalar(select(Incident)).status == 'open'
        assert db.scalar(select(Audit).where(Audit.outcome == 'forbidden'))


def test_analyst_changes_are_audited_and_csrf_enforced(auth):
    _, client, factory, *_ = auth
    response = login(auth, 'analyst')
    for bad in ('', 'invalid'):
        assert client.patch('/api/v1/incidents/test-incident/status', json={'status':'resolved'}, headers={'X-CSRF-Token':bad}).status_code == 403
    assert client.patch('/api/v1/incidents/test-incident/status', json={'status':'resolved'}, headers={'Origin':'https://evil.example'}).status_code == 403
    for kind, identifier in [('alerts','test-alert'), ('incidents','test-incident')]:
        result = client.patch(f'/api/v1/{kind}/{identifier}/status', json={'status':'investigating'})
        assert result.status_code == 200 and result.json()['status'] == 'investigating'
    assert client.post('/api/v1/auth/users', json={}).status_code == 403
    from app.auth.models import Audit
    with factory() as db:
        assert {'alert_status_changed','incident_status_changed','modification_denied'} <= {a.action for a in db.scalars(select(Audit))}


def test_administration_revocation_provisioning_and_self_protection(auth):
    from app.auth.service import create_user
    from app.auth.models import LoginSession, User
    _, client, factory, password, ids = auth
    login(auth, 'administrator')
    assert client.get('/api/v1/auth/users').status_code == 200
    assert client.patch('/api/v1/auth/users/' + ids['administrator'], json={'role':'viewer','active':False}).status_code == 403
    # Even deleting all users outside the application cannot reopen initial provisioning.
    with factory() as db, pytest.raises(ValueError):
        create_user(db, 'another-admin', password, 'administrator', initial=True)
    response = client.post('/api/v1/auth/users', json={'username':'new-viewer','password':password,'role':'viewer'})
    assert response.status_code == 201 and 'password' not in response.text
    assert client.post('/api/v1/auth/users', json={'username':'bad-role','password':password,'role':'root'}).status_code == 422
    login(auth, 'analyst')
    token = client.cookies.get('soc_session_local')
    login(auth, 'administrator')
    assert client.patch('/api/v1/auth/users/' + ids['analyst'], json={'role':'viewer','active':False}).status_code == 200
    assert client.get('/api/v1/auth/audit').status_code == 200
    client.cookies.clear(); client.cookies.set('soc_session_local', token)
    assert client.get('/api/v1/auth/me').status_code == 401
    assert client.post('/api/v1/auth/login', json={'username':'analyst','password':password}, headers=ORIGIN).status_code == 401


@pytest.mark.parametrize('reason', ['absolute', 'idle', 'revoked', 'disabled'])
def test_invalid_sessions(auth, reason):
    from app.auth.models import LoginSession, User
    _, client, factory, _, ids = auth
    login(auth)
    with factory() as db, db.begin():
        if reason == 'disabled': db.execute(update(User).where(User.id == ids['viewer']).values(active=False))
        else:
            values = {'absolute': {'expires_at':0}, 'idle': {'last_seen':0}, 'revoked': {'revoked':True}}[reason]
            db.execute(update(LoginSession).values(**values))
    assert client.get('/api/v1/auth/me').status_code == 401


@pytest.mark.parametrize('channel', ['dashboard', 'alerts', 'threat-intel'])
def test_websocket_auth_origin_stream_and_active_revocation(auth, channel):
    from app.auth.models import LoginSession
    _, client, factory, *_ = auth
    path = 'ws://localhost/api/v1/ws/' + channel
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect(path, headers=ORIGIN): pass
    login(auth)
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect(path, headers={'Origin':'https://evil.example'}): pass
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect(path, headers={'Origin':''}): pass
    with client.websocket_connect(path, headers=ORIGIN) as socket:
        assert isinstance(socket.receive_json(), dict)
        with factory() as db, db.begin(): db.execute(update(LoginSession).values(revoked=True))
        with pytest.raises(WebSocketDisconnect) as exc: socket.receive_json()
        assert exc.value.code == 4401


def test_login_abuse_origin_validation_and_no_password_echo(auth):
    _, client, factory, password, _ = auth
    payload = {'username':'viewer', 'password':password}
    assert client.post('/api/v1/auth/login', json=payload).status_code == 403
    assert client.post('/api/v1/auth/login', json=payload, headers={**ORIGIN,'Origin':'https://evil.example'}).status_code == 403
    preflight = client.options('/api/v1/alerts/recent', headers={'Origin':'https://evil.example', 'Access-Control-Request-Method':'GET'})
    assert preflight.status_code == 400 and 'access-control-allow-origin' not in preflight.headers
    response = client.post('/api/v1/auth/login', json={**payload,'password':password*10}, headers=ORIGIN)
    assert response.status_code == 422 and password not in response.text
    for _ in range(10):
        assert client.post('/api/v1/auth/login', json={**payload,'password':'wrong'}, headers=ORIGIN).status_code == 401
    assert client.post('/api/v1/auth/login', json=payload, headers=ORIGIN).status_code == 429
    # A fresh process/client still observes shared persisted admission state.
    with TestClient(auth[0], base_url='http://localhost') as other:
        assert other.post('/api/v1/auth/login', json=payload, headers=ORIGIN).status_code == 429


def test_production_cookie_and_config_defaults(auth):
    from app.core.settings import Settings, settings
    from pydantic import ValidationError
    assert Settings(_env_file=None, auth_local_http=False).session_cookie == '__Host-soc_session'
    with pytest.raises(RuntimeError, match='Production requires'): Settings(_env_file=None, environment='production', auth_local_http=True)
    with pytest.raises(ValidationError): Settings(_env_file=None, BACKEND_CORS_ORIGINS='*')
    settings.auth_local_http = False
    _, client, _, password, _ = auth
    assert client.post('/api/v1/auth/login', json={'username':'viewer','password':password}, headers=ORIGIN).status_code == 400
    with TestClient(auth[0], base_url='https://localhost') as secure:
        response = secure.post('/api/v1/auth/login', json={'username':'viewer','password':password}, headers=ORIGIN)
        assert response.status_code == 200
        assert '__Host-soc_session=' in response.headers['set-cookie'] and 'Secure' in response.headers['set-cookie']


def test_status_and_audit_are_atomic_and_stale_roles_rejected(auth, monkeypatch):
    from app.api.v1.endpoints import alerts
    from app.auth.models import User
    from app.auth.service import authorize_write
    from app.db.models import Incident
    from fastapi import HTTPException
    _, client, factory, _, ids = auth
    login(auth, 'analyst')
    from app.auth.middleware import lookup
    identity = lookup(client.cookies.get('soc_session_local'))
    def failed_audit(*args, **kwargs): raise RuntimeError('Simulated audit outage')
    monkeypatch.setattr(alerts, 'audit', failed_audit)
    with TestClient(auth[0], base_url='http://localhost', raise_server_exceptions=False) as other:
        other.cookies.update(client.cookies)
        other.headers.update(client.headers)
        assert other.patch('/api/v1/incidents/test-incident/status', json={'status':'resolved'}).status_code == 500
    with factory() as db:
        assert db.scalar(select(Incident)).status == 'open'
    with factory() as db, db.begin(): db.execute(update(User).where(User.id == ids['analyst']).values(role='viewer'))
    with factory() as db, pytest.raises(HTTPException): authorize_write(db, identity, 'analyst')


def test_concurrent_initial_provisioning_is_one_time(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    load_backend_app(tmp_path)
    from app.auth.service import create_user
    from app.auth.models import User
    from app.db.session import SessionLocal
    password = secrets.token_urlsafe(24)
    def provision(name):
        try:
            with SessionLocal() as db, db.begin(): create_user(db, name, password, 'administrator', initial=True)
            return True
        except ValueError: return False
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(provision, ['first-admin','second-admin'])) == [False, True]
    with SessionLocal() as db:
        assert len(list(db.scalars(select(User)))) == 1
