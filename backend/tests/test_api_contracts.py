import importlib
import os
import sys
import secrets
from pathlib import Path

from fastapi.testclient import TestClient


def load_backend_app(tmp_path: Path):
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    os.environ["DATABASE_URL"] = f"sqlite+pysqlite:///{tmp_path / 'test.db'}"
    os.environ["BACKEND_CORS_ORIGINS"] = "http://localhost:5173,http://localhost:5174"
    os.environ["PROMETHEUS_URL"] = "http://localhost:9090"
    os.environ["AUTH_LOCAL_HTTP"] = "true"
    token_file = tmp_path / "metrics-token"
    token_file.write_text(secrets.token_urlsafe(32))
    os.environ["METRICS_TOKEN_FILE"] = str(token_file)
    app = importlib.import_module("app.main").app
    from app.auth.models import AuthBase, Control
    from app.db.session import engine, SessionLocal
    AuthBase.metadata.create_all(engine)
    with SessionLocal() as db, db.begin():
        db.add(Control(id=1, generation=0, bootstrapped=False))
    return app


def test_health_endpoint_and_security_headers(tmp_path):
    app = load_backend_app(tmp_path)
    with TestClient(app) as client:
        response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "alive"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"


def test_dashboard_alert_monitoring_and_threat_api_surfaces(tmp_path):
    app = load_backend_app(tmp_path)
    from app.auth.service import create_user
    from app.db.session import SessionLocal
    password = secrets.token_urlsafe(24)
    with SessionLocal() as db, db.begin():
        create_user(db, "test-admin", password, "administrator", initial=True)
    with TestClient(app, base_url="http://localhost") as client:
        assert client.post("/api/v1/auth/login", json={"username":"test-admin", "password":password},
                           headers={"Origin":"http://localhost:5173", "X-Requested-With":"SOC"}).status_code == 200
        assert client.get("/api/v1/dashboard/overview").status_code == 200
        assert client.get("/api/v1/alerts/stats").status_code == 200
        assert client.get("/api/v1/incidents/recent").status_code == 200
        assert client.get("/api/v1/monitoring/overview").status_code == 200
        assert client.get("/api/v1/threat-intel/overview").status_code == 200
        assert client.get("/api/v1/mitre/tactics").status_code == 200
        assert client.get("/metrics").status_code == 401
        token = Path(os.environ["METRICS_TOKEN_FILE"]).read_text()
        assert client.get("/metrics", headers={"Authorization":f"Bearer {token}"}).status_code == 200


def test_dashboard_live_records_keep_the_persisted_source_topic(tmp_path):
    load_backend_app(tmp_path)
    from app.api.v1.endpoints.dashboard import _live_record

    record = type("Record", (), {"raw_payload": {"event_type": "feature_window"}, "source_topic": "ml.anomaly.predictions"})()

    assert _live_record(record) == {
        "event_type": "feature_window",
        "source_topic": "ml.anomaly.predictions",
    }
