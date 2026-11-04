"""Dashboard numbers must agree across endpoints, trends must span time, and health must be measured."""
import secrets
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from test_api_contracts import load_backend_app

HEADERS = {"Origin": "http://localhost:5173", "X-Requested-With": "SOC"}


def naive_utc(seconds_ago=0.0):
    """SQLite, which these tests use, stores naive UTC datetimes."""
    return (datetime.now(UTC) - timedelta(seconds=seconds_ago)).replace(tzinfo=None)


@pytest.fixture
def stack(tmp_path):
    app = load_backend_app(tmp_path)
    from app.auth.service import create_user
    from app.db import models
    from app.db.session import SessionLocal

    password = secrets.token_urlsafe(24)
    with SessionLocal() as db, db.begin():
        create_user(db, "dashboard-admin", password, "administrator", initial=True)
    with TestClient(app, base_url="http://localhost") as client:
        assert client.post("/api/v1/auth/login", json={"username": "dashboard-admin", "password": password},
                           headers=HEADERS).status_code == 200
        yield client, SessionLocal, models


def stored(models, model, count, *, age_seconds=0.0, event_age=None, score=None, tag="r"):
    """`count` records persisted `age_seconds` ago whose event time is `event_age` seconds ago."""
    rows = []
    created, happened = naive_utc(age_seconds), naive_utc(age_seconds if event_age is None else event_age)  # one instant per group
    for index in range(count):
        fields = dict(source_topic="test", raw_payload={}, created_at=created, timestamp=happened)
        if model is models.TelemetryEvent:
            fields.update(event_id=f"{tag}-{age_seconds}-{event_age}-{index}", event_type="login", risk_score=score or 1.0)
        elif model is models.AnomalyPrediction:
            fields.update(prediction_id=f"{tag}-{age_seconds}-{event_age}-{index}", ml_risk_score=score if score is not None else 50.0,
                          raw_payload={"is_anomaly": False})
        rows.append(model(**fields))
    return rows


def add(session_factory, *rows):
    with session_factory() as db, db.begin():
        db.add_all(rows)


def test_the_overview_monitoring_and_analytics_endpoints_agree_on_every_shared_number(stack):
    client, session, models = stack
    now = naive_utc()
    incidents = [models.Incident(incident_id=f"i-{status}", created_at=now, updated_at=now, severity="high", title="t",
                                 description="d", related_alerts=[], entity_ids=[], status=status, event_count=1,
                                 escalation_level=1, raw_payload={}) for status in ("open", "acknowledged", "investigating", "resolved")]
    add(session, *stored(models, models.TelemetryEvent, 3), *stored(models, models.AnalyticsMetric, 2),
        *stored(models, models.RiskMetric, 1), *stored(models, models.FeatureSnapshot, 2),
        *stored(models, models.AnomalyPrediction, 2), *incidents)

    overview = client.get("/api/v1/dashboard/overview").json()
    monitoring = client.get("/api/v1/monitoring/overview").json()
    pipeline = client.get("/api/v1/monitoring/pipeline").json()
    summary = client.get("/api/v1/analytics/summary").json()
    alerts = client.get("/api/v1/alerts/stats").json()

    assert overview["total_events"] == monitoring["total_events"] == pipeline["telemetry_events"] == summary["telemetry_events"] == 3
    assert overview["total_records"] == 3 + 2 + 1 + 2  # telemetry plus what Spark derived from it
    assert overview["total_predictions"] == monitoring["total_predictions"] == pipeline["anomaly_predictions"] == 2
    # open means not yet resolved, everywhere: open, acknowledged and investigating
    assert monitoring["open_incidents"] == alerts["open_incidents"] == 3
    assert client.get("/api/v1/incidents/open").json().__len__() == 3


def test_trends_cover_the_last_hour_by_minute_instead_of_the_last_few_hundred_rows(stack):
    client, session, models = stack
    # 250 predictions in the latest minute used to push every earlier minute out of a 200-row window.
    add(session, *stored(models, models.AnomalyPrediction, 250, age_seconds=5, score=80.0, tag="now"),
        *stored(models, models.AnomalyPrediction, 3, age_seconds=600, score=20.0, tag="ten"),
        *stored(models, models.AnomalyPrediction, 2, age_seconds=1800, score=40.0, tag="half"),
        *stored(models, models.AnomalyPrediction, 5, age_seconds=3 * 3600, score=99.0, tag="old"),  # outside the window
        *stored(models, models.TelemetryEvent, 300, age_seconds=5, tag="now"),
        *stored(models, models.TelemetryEvent, 4, age_seconds=1500, tag="earlier"),
        *stored(models, models.TelemetryEvent, 9, age_seconds=7200, tag="old"))

    risk = client.get("/api/v1/dashboard/risk-trends").json()
    volume = client.get("/api/v1/dashboard/event-volume").json()

    # three minutes with data, oldest first, each with its own mean and count; the 3 hour old rows are excluded
    assert [(point["value"], point["secondary_value"]) for point in risk] == [(40.0, 2), (20.0, 3), (80.0, 250)]
    assert [(point["value"]) for point in volume] == [4.0, 300.0]  # the 9 events from two hours ago are not in the hour


def stages_at(stack, **ages):
    """pipeline_stages with each stage's newest/oldest persisted age given in seconds (None = never)."""
    client, session, models = stack
    from app.services.pipeline_health import pipeline_stages
    rows = []
    for key, model in (("ingest", models.TelemetryEvent), ("spark", models.FeatureSnapshot), ("ml", models.AnomalyPrediction)):
        if ages.get(key) is not None:
            if ages.get(key + "_first") is not None:  # rows are inserted in time order, as the pipeline does
                rows += stored(models, model, 1, age_seconds=ages[key + "_first"], tag=key + "-first")
            rows += stored(models, model, 1, age_seconds=ages[key], tag=key)
    add(session, *rows)
    with session() as db:
        return {stage.name: stage for stage in pipeline_stages(db, datetime.now(UTC))}


@pytest.mark.parametrize("ages,expected", [
    ({}, ("waiting", "waiting", "waiting")),
    ({"ingest": 5, "spark": 40, "ml": 50}, ("active", "active", "active")),
    ({"ingest": 5, "ingest_first": 1500}, ("active", "degraded", "waiting")),  # input for 25 min, no Spark output at all
    ({"ingest": 5, "ingest_first": 60}, ("active", "waiting", "waiting")),  # just started: Spark needs about 3 minutes
    ({"ingest": 5, "spark": 20, "ml": 1500}, ("active", "active", "degraded")),  # Spark feeds ML but ML went quiet
    ({"ingest": 5, "spark": 1500, "ml": 1500}, ("active", "degraded", "idle")),  # Spark quiet while input arrives
    ({"ingest": 1800, "spark": 1800, "ml": 1800}, ("idle", "idle", "idle")),  # nothing arrives: producer stopped
])
def test_pipeline_stage_status_is_measured_from_when_each_stage_last_stored_data(stack, ages, expected):
    stages = stages_at(stack, **ages)
    assert tuple(stages[name].status for name in ("Kafka", "Spark", "ML Inference")) == expected


def test_system_health_reports_measured_details_not_fixed_text(stack):
    client, session, models = stack
    add(session, *stored(models, models.TelemetryEvent, 1, age_seconds=7), *stored(models, models.FeatureSnapshot, 1, age_seconds=30),
        *stored(models, models.AnomalyPrediction, 1, age_seconds=45))
    components = {item["name"]: item for item in client.get("/api/v1/dashboard/system-health").json()["components"]}
    assert components["Kafka"]["status"] == "active" and "7 s ago" in components["Kafka"]["detail"]
    assert components["Spark"]["status"] == "active" and "30 s ago" in components["Spark"]["detail"]
    assert components["ML Inference"]["status"] == "active" and "45 s ago" in components["ML Inference"]["detail"]
    assert "when running" not in components["Kafka"]["detail"]


def test_monitoring_judges_kafka_and_spark_from_data_and_does_not_blame_spark_for_an_unreachable_prometheus(stack):
    client, session, models = stack
    add(session, *stored(models, models.TelemetryEvent, 1, age_seconds=7), *stored(models, models.FeatureSnapshot, 1, age_seconds=30))
    services = {item["name"]: item for item in client.get("/api/v1/monitoring/services").json()}
    assert services["Kafka"]["status"] == "healthy" and "7 s ago" in services["Kafka"]["detail"]
    # Prometheus is unreachable in this test, which says nothing against Spark itself.
    assert services["Spark"]["status"] == "healthy" and "Prometheus is unavailable" in services["Spark"]["detail"]
    assert "Frontend" not in services  # the backend cannot measure it, so it no longer claims it is healthy
