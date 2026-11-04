"""List endpoints page with offset, leave out stored payloads the pages never show, and compress."""
from datetime import timedelta

from test_dashboard_truth import add, naive_utc, stack, stored  # noqa: F401  (stack is a fixture)


def seed(models, session):
    now = naive_utc()
    big = {"blob": "x" * 4000}
    alerts = [models.Alert(alert_id=f"a-{i:03d}", timestamp=now - timedelta(seconds=i), severity="high", title="t", description="d",
                           source_topic="t", correlation_id="c", explanation="e", recommended_action="r", status="open", tags=[],
                           raw_payload=big, created_at=now - timedelta(seconds=i)) for i in range(7)]
    incidents = [models.Incident(incident_id=f"i-{i:03d}", created_at=now, updated_at=now - timedelta(seconds=i), severity="high",
                                 title="t", description="d", related_alerts=[], entity_ids=[], status="open", event_count=1,
                                 escalation_level=1, raw_payload=big) for i in range(7)]
    add(session, *alerts, *incidents, *stored(models, models.TelemetryEvent, 7))


def ids(rows, key):
    return [row[key] for row in rows]


def test_lists_page_without_gaps_or_repeats(stack):
    client, session, models = stack
    seed(models, session)
    for path, key in (("/api/v1/alerts/recent", "alert_id"), ("/api/v1/alerts/high", "alert_id"),
                      ("/api/v1/incidents/recent", "incident_id"), ("/api/v1/incidents/open", "incident_id"),
                      ("/api/v1/events/recent", "id")):
        everything = ids(client.get(path, params={"limit": 50}).json(), key)
        paged = []
        for offset in (0, 3, 6):
            paged += ids(client.get(path, params={"limit": 3, "offset": offset}).json(), key)
        assert len(everything) == 7 and paged == everything, path
        assert client.get(path, params={"offset": -1}).status_code == 422


def test_alert_and_incident_lists_omit_the_stored_payload_but_details_keep_it(stack):
    client, session, models = stack
    seed(models, session)
    for path in ("/api/v1/alerts/recent", "/api/v1/incidents/recent", "/api/v1/incidents/open"):
        rows = client.get(path).json()
        assert rows and all("raw_payload" not in row for row in rows), path
    assert "blob" in client.get("/api/v1/incidents/i-000").json()["raw_payload"]


def test_large_responses_are_compressed(stack):
    client, session, models = stack
    seed(models, session)
    response = client.get("/api/v1/events/recent", headers={"Accept-Encoding": "gzip"})
    assert response.headers.get("content-encoding") == "gzip"
    assert len(response.json()) == 7
