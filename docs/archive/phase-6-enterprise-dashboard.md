# Phase 6: Enterprise Real-Time Dashboard

Phase 6 adds a polished local enterprise dashboard for the streaming analytics platform. It visualizes persisted telemetry, Spark analytics, ML predictions, high-risk activity, search results, and system health using only local FastAPI APIs and free open-source frontend libraries.

No paid APIs, SaaS dashboards, cloud services, or paid UI libraries are used.

## Architecture

```text
PostgreSQL + Elasticsearch
    -> FastAPI dashboard APIs + WebSocket
    -> React + TypeScript + Tailwind + Recharts
    -> Real-time AI analytics interface
```

## Backend APIs

Dashboard endpoints:

- `GET /api/v1/dashboard/overview`
- `GET /api/v1/dashboard/system-health`
- `GET /api/v1/dashboard/risk-trends`
- `GET /api/v1/dashboard/event-volume`
- `GET /api/v1/dashboard/severity-distribution`
- `GET /api/v1/dashboard/event-type-distribution`
- `GET /api/v1/dashboard/top-entities`

Existing query endpoints used by the dashboard:

- `GET /api/v1/events/recent`
- `GET /api/v1/predictions/recent`
- `GET /api/v1/risks/high`
- `GET /api/v1/search/events?q=...`

WebSocket:

- `WS /api/v1/ws/dashboard`
- Local frontend URL: `ws://localhost:8000/api/v1/ws/dashboard`

The WebSocket emits:

- latest overview metrics
- flattened metrics: `system_status`, `total_events`, `total_predictions`, `anomaly_count`, `high_risk_count`, `avg_risk_score`
- latest predictions
- recent events
- high-risk events
- event counters
- timestamp

The frontend includes polling fallback for API data if the WebSocket is unavailable.

## Frontend Routes

- `/` - Overview
- `/live` - Live Stream
- `/risk` - Risk Analytics
- `/predictions` - ML Predictions
- `/search` - Search
- `/health` - System Health

## Dashboard Views

### Overview

Shows total events, predictions, anomalies, high-risk count, average risk score, system status, trend charts, volume charts, severity distribution, top entities, and recent events.

### Live Stream

Shows recent telemetry and latest ML prediction activity with live WebSocket status and polling refresh.

### Risk Analytics

Shows risk trend line chart, event volume area chart, severity distribution chart, top risky entities chart, and high-risk entity table.

### ML Predictions

Shows recent predictions and high-risk prediction queue. Rows include expandable raw payloads with features and explanations.

### Search

Uses the backend Elasticsearch-backed search API to inspect telemetry, analytics, and prediction documents.

### System Health

Shows backend, PostgreSQL, Elasticsearch, Kafka observation state, Spark persistence state, and ML prediction persistence state.

## Frontend Stack

- React
- TypeScript
- Tailwind CSS
- React Router
- Axios
- Recharts
- Lucide React
- Browser WebSocket API

## Running Locally

Start the platform:

```bash
docker compose up --build -d
```

Open the dashboard:

```text
http://localhost:5173
```

Verify backend dashboard APIs:

```text
http://localhost:8000/api/v1/dashboard/overview
http://localhost:8000/api/v1/dashboard/system-health
http://localhost:8000/api/v1/predictions/recent
http://localhost:8000/api/v1/events/recent
```

Verify supporting UIs:

- Spark Job UI: `http://localhost:4040`
- Elasticsearch health: `http://localhost:9200/_cluster/health`

## Performance Notes

- Recent events are limited to 50 records.
- Recent predictions are limited to 50 records.
- High-risk records are limited to 25 by default.
- Chart APIs aggregate bounded recent records.
- Frontend polling intervals are between 5 and 15 seconds.

## Troubleshooting

If charts show fallback data, confirm that Phase 5 storage sink is running and records exist in PostgreSQL.

If search returns no results, confirm Elasticsearch is reachable and storage sink indexing has initialized indexes.

If WebSocket shows fallback, dashboard pages continue polling REST APIs.
