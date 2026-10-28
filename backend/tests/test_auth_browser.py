"""Real browser + real API, isolated DB/accounts; no mocked authentication responses."""
import os
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
from test_authentication import auth  # isolated database/account fixture

pytestmark = pytest.mark.skipif(os.environ.get('AUTH_BROWSER_INTEGRATION') != '1', reason='Enable isolated Chromium integration explicitly')


def test_browser_workflow_viewer_denial_and_expiration(auth):
    import uvicorn
    from playwright.sync_api import sync_playwright, expect
    from sqlalchemy import update
    from app.auth.models import LoginSession
    from app.core.settings import settings
    app, _, factory, password, _ = auth
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level='error', access_log=False))
    thread = threading.Thread(target=server.run, kwargs={'sockets':[listener]}, daemon=True)
    thread.start()
    root = Path(__file__).resolve().parents[2]
    process = subprocess.Popen(['npm','run','dev','--','--host','127.0.0.1','--port','5174','--strictPort'],
        cwd=root/'frontend', env={**os.environ,'VITE_API_BASE_URL':f'http://localhost:{port}/api/v1'},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        import urllib.request
        for _ in range(100):
            try:
                urllib.request.urlopen('http://localhost:5174', timeout=1)
                if server.started: break
            except OSError: pass
            time.sleep(.1)
        else: raise AssertionError('Isolated API/frontend failed to start')
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            page = context.new_page()
            page.goto('http://localhost:5174/incidents')
            expect(page.get_by_role('heading', name='Sign in to Streaming Security Analytics (local demo)')).to_be_visible()
            assert page.request.get(f'http://localhost:{port}/api/v1/alerts/recent').status == 401
            def sign_in(role):
                page.get_by_label('Username', exact=True).fill(role)
                page.get_by_label('Password', exact=True).fill(password)
                page.get_by_role('button', name='Sign in', exact=True).click()
                expect(page.get_by_role('button', name='Sign out')).to_be_visible()
            sign_in('analyst')
            page.get_by_role('link', name='Overview', exact=True).first.click()
            expect(page.get_by_role('heading', name='Streaming Security Analytics (local demo)').first).to_be_visible()
            page.get_by_role('link', name='Alerts', exact=True).first.click()
            expect(page.get_by_text('Isolated alert', exact=True).first).to_be_visible()
            page.get_by_role('link', name='Incidents', exact=True).first.click()
            selector = page.get_by_label('incidents test-incident status').first
            selector.select_option('investigating')
            expect(page.get_by_text('Status saved.', exact=True).first).to_be_visible()
            assert page.request.get(f'http://localhost:{port}/api/v1/incidents/test-incident').json()['status'] == 'investigating'
            cookies = context.cookies()
            assert any(c['name'] == 'soc_session_local' and c['httpOnly'] and c['sameSite'] == 'Strict' for c in cookies)
            assert page.evaluate('Object.keys(localStorage).length') == 0
            page.get_by_role('button', name='Sign out').click()
            expect(page.get_by_role('button', name='Sign in', exact=True)).to_be_visible()
            assert page.request.get(f'http://localhost:{port}/api/v1/incidents/recent').status == 401
            page.reload()
            expect(page.get_by_role('button', name='Sign in', exact=True)).to_be_visible()
            sign_in('viewer')
            expect(page.get_by_text('Isolated incident', exact=True).first).to_be_visible()
            assert page.get_by_role('combobox').count() == 0
            # Direct requests cannot bypass the UI's hidden controls.
            me = page.request.get(f'http://localhost:{port}/api/v1/auth/me').json()
            response = page.request.patch(f'http://localhost:{port}/api/v1/incidents/test-incident/status',
                data={'status':'resolved'}, headers={'Origin':'http://localhost:5174','X-CSRF-Token':me['csrf_token']})
            assert response.status == 403
            assert page.evaluate("""async () => {
                const {httpClient} = await import('/src/api/httpClient.ts');
                try { await httpClient.get('/auth/users'); return 200; }
                catch (error) { return error.response.status; }
            }""") == 403
            expect(page.get_by_text('You do not have permission to perform this action.')).to_be_visible()
            # Active WebSocket is revoked at the next send, clearing the entire protected UI.
            with factory() as db, db.begin(): db.execute(update(LoginSession).values(expires_at=0))
            expect(page.get_by_role('button', name='Sign in', exact=True)).to_be_visible(timeout=15000)
            expect(page.get_by_text('Isolated incident', exact=True)).to_have_count(0)
            assert page.request.get(f'http://localhost:{port}/api/v1/alerts/recent').status == 401
            assert page.evaluate("""async () => {
                const {dashboardService} = await import('/src/services/dashboardService.ts');
                try { await dashboardService.overview(); return 'synthetic-success'; }
                catch (error) { return error.response.status; }
            }""") == 401
            browser.close()
    finally:
        process.terminate(); process.wait(timeout=10)
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
