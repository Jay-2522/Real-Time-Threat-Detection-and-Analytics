# Backend API

The backend is the FastAPI service layer for the Real-Time Streaming Analytics System. It exposes health checks and Phase 5 query APIs over locally persisted PostgreSQL and Elasticsearch data.

## Query APIs

- `GET /api/v1/analytics/summary`
- `GET /api/v1/events/recent`
- `GET /api/v1/predictions/recent`
- `GET /api/v1/predictions/{entity_id}`
- `GET /api/v1/risks/high`
- `GET /api/v1/search/events?q=...`
- `GET /api/v1/dashboard/overview`
- `GET /api/v1/dashboard/system-health`
- `GET /api/v1/dashboard/risk-trends`
- `GET /api/v1/dashboard/event-volume`
- `GET /api/v1/dashboard/severity-distribution`
- `GET /api/v1/dashboard/top-entities`
- `WS /api/v1/ws/dashboard`
- `GET /api/v1/alerts/recent`
- `GET /api/v1/alerts/high`
- `GET /api/v1/alerts/critical`
- `GET /api/v1/incidents/recent`
- `GET /api/v1/incidents/{incident_id}`
- `GET /api/v1/incidents/open`
- `GET /api/v1/alerts/stats`
- `GET /api/v1/alerts/search?q=...`
- `PATCH /api/v1/alerts/{alert_id}/status`
- `PATCH /api/v1/incidents/{incident_id}/status`
- `WS /api/v1/ws/alerts`
- `GET /api/v1/monitoring/overview`
- `GET /api/v1/monitoring/services`
- `GET /api/v1/monitoring/pipeline`
- `GET /api/v1/monitoring/errors`
- `GET /api/v1/monitoring/metrics-summary`
- `GET /metrics`
- `GET /api/v1/threat-intel/overview`
- `GET /api/v1/threat-intel/iocs`
- `GET /api/v1/threat-intel/entities`
- `GET /api/v1/threat-intel/mitre`
- `GET /api/v1/threat-intel/attack-timeline`
- `GET /api/v1/threat-intel/risk-heatmap`
- `GET /api/v1/threat-intel/threat-graph`
- `GET /api/v1/detections/rules`
- `GET /api/v1/detections/rule-stats`
- `GET /api/v1/entities/high-risk`
- `GET /api/v1/mitre/tactics`
- `WS /api/v1/ws/threat-intel`

## Data Sources

- PostgreSQL stores structured telemetry, metrics, features, predictions, and dead letters.
- Elasticsearch backs the three search endpoints (`/search/events`, `/alerts/search`, `/threat-intel/search`).
  When it cannot answer, they return HTTP 503 with `Retry-After`, and HTTP 400 for a query that cannot be parsed;
  an empty list always means a search that ran and found nothing, or that nothing has been indexed yet. Search
  results show an alert or incident as it was when last indexed, so a later status change appears in the
  PostgreSQL-backed lists first.
- Prometheus backs monitoring summaries when available.
- Threat intelligence tables back IOC, MITRE, detection rule, entity, and timeline APIs.
- Existing health endpoint remains available at `GET /api/v1/health`.

## Local Development

```bash
uvicorn app.main:app --reload
```

When running through Docker Compose, the backend uses:

```text
DATABASE_URL=postgresql+psycopg://streaming_user:streaming_password@streaming-analytics-postgres:5432/streaming_analytics
ELASTICSEARCH_HOST=http://streaming-analytics-elasticsearch:9200
PROMETHEUS_URL=http://streaming-analytics-prometheus:9090
```
