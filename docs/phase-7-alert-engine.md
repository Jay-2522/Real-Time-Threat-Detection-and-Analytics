# Phase 7: Real-Time Alert Engine And Incident Intelligence

Phase 7 transforms the platform into a local AI-assisted SOC system. It adds a real-time alert engine, incident correlation, PostgreSQL-backed cooldown deduplication, severity escalation, alert and incident persistence, Elasticsearch indexing, FastAPI alert APIs, alert WebSocket streaming, and dashboard SOC views.

Everything remains free and local. No PagerDuty, Splunk Cloud, Datadog, cloud SIEM, paid APIs, or managed infrastructure is used.

## Architecture

```text
Kafka + Spark + ML Predictions
    -> Alert Rules Engine
    -> PostgreSQL Cooldown Deduplication
    -> Incident Correlation Engine
    -> PostgreSQL alerts/incidents
    -> Elasticsearch alert/incident indexes
    -> FastAPI APIs + WebSocket
    -> React SOC Dashboard
```

## Alert Engine Service

Container:

- `streaming-analytics-alert-engine`

Consumed topics:

- `ml.anomaly.predictions`
- `analytics.risk.metrics`
- `analytics.enriched.events`

Rules detect:

- High anomaly score.
- High ML risk score.
- Repeated failed logins.
- API abuse patterns.
- Suspicious IP frequency.
- Network spike anomalies.
- Repeated high-risk activity.
- High anomaly/risk windows.

## Severity Levels

- `info`
- `low`
- `medium`
- `high`
- `critical`

Escalation behavior:

- Repeated medium alerts in the same correlation group escalate to high.
- Repeated high alerts in the same correlation group escalate to critical.

## Deduplication

The default cooldown is 120 seconds. Repeated alerts with the same correlation key and severity during the cooldown window are suppressed and counted.

## Incident Correlation

Alerts are grouped into incidents using:

- Entity ID.
- Rule/correlation ID.
- Active incident time window.
- Severity clustering.

Each incident tracks:

- Related alert IDs.
- Entity IDs.
- Event count.
- Escalation level.
- Timeline in `raw_payload.timeline`.
- Status and resolution notes.

## PostgreSQL Tables

Phase 7 adds:

- `alerts`
- `incidents`
- `incident_alert_links`
- `alert_dedup_cache`

Indexes exist for:

- severity
- timestamp/updated time
- incident ID
- entity ID
- status

## Elasticsearch Indexes

The alert engine initializes:

- `alerts`
- `incidents`

Alert documents include title, description, explanation, recommended action, tags, severity, status, entity, and source topic.

## FastAPI Alert APIs

- `GET /api/v1/alerts/recent`
- `GET /api/v1/alerts/high`
- `GET /api/v1/alerts/critical`
- `GET /api/v1/incidents/recent`
- `GET /api/v1/incidents/{incident_id}`
- `GET /api/v1/incidents/open`
- `GET /api/v1/alerts/stats`
- `GET /api/v1/alerts/search?q=...`
- `PATCH /api/v1/incidents/{incident_id}/status`
- `PATCH /api/v1/alerts/{alert_id}/status`

WebSocket:

- `WS /api/v1/ws/alerts`

The WebSocket streams:

- recent alerts
- critical alerts
- recent incidents
- severity counters
- aggregate alert stats

## Dashboard Integration

New dashboard routes:

- `/alerts`
- `/incidents`

UI components include:

- Critical alert banner.
- Live SOC feed.
- Alert stats counters.
- Alerts table.
- Incident cards.
- Severity badges.
- WebSocket with polling fallback.

## Running Locally

```bash
docker compose up --build -d
docker logs -f streaming-analytics-alert-engine
```

Expected logs:

- Kafka connected.
- Elasticsearch connected or optional warning.
- Alerts generated.
- Incidents created.
- Deduplication active.

## Verification

Alert APIs:

```text
http://localhost:8000/api/v1/alerts/recent
http://localhost:8000/api/v1/incidents/recent
http://localhost:8000/api/v1/alerts/stats
```

Dashboard:

```text
http://localhost:5173/alerts
http://localhost:5173/incidents
```

PostgreSQL:

```sql
SELECT COUNT(*) FROM alerts;
SELECT COUNT(*) FROM incidents;
SELECT COUNT(*) FROM incident_alert_links;
```

Elasticsearch:

```text
http://localhost:9200/_cat/indices?v
```

## Troubleshooting

If no alerts appear, verify `ml.anomaly.predictions` is receiving records and the alert engine logs show Kafka consumption.

If alerts appear but incidents do not, check for PostgreSQL errors in alert-engine logs.

If many duplicate events appear, inspect the PostgreSQL alert history and cooldown query logs.

If the dashboard alert stream falls back, the REST APIs continue polling until `WS /api/v1/ws/alerts` reconnects.

