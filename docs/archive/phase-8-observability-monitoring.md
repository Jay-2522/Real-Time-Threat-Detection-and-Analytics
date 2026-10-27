# Phase 8 - Observability, Monitoring, And System Health

Phase 8 adds a fully local observability layer for the Real-Time Streaming Analytics System. The stack uses only free Docker Compose services and open-source libraries.

## Architecture

```text
Python services
    -> /metrics endpoints
    -> Prometheus
    -> Grafana dashboards
    -> FastAPI monitoring APIs
    -> React observability UI
```

## Local Services

| Service | Container | URL |
| --- | --- | --- |
| Prometheus | `streaming-analytics-prometheus` | `http://localhost:9090` |
| Grafana (optional) | `streaming-analytics-grafana` | `http://localhost:3000` |
| Backend metrics | `streaming-analytics-backend` | `http://localhost:8000/metrics` |
| ML metrics | `streaming-analytics-ml-inference` | `http://localhost:9101/metrics` |
| Storage sink metrics | `streaming-analytics-storage-sink` | `http://localhost:9102/metrics` |
| Alert engine metrics | `streaming-analytics-alert-engine` | `http://localhost:9103/metrics` |
| Producer metrics | `streaming-analytics-producer` | `http://localhost:9104/metrics` |

Start the optional Grafana dashboards with `./scripts/start_full.sh --dashboards`.
The admin password is the generated `GRAFANA_ADMIN_PASSWORD` in the ignored local `.env` file.

```text
admin / <GRAFANA_ADMIN_PASSWORD>
```

## Prometheus Targets

Prometheus is configured in `monitoring/prometheus/prometheus.yml` and scrapes:

- `backend`
- `ml-inference`
- `storage-sink`
- `alert-engine`
- `telemetry-producer`
- `prometheus`

Open target status at:

```text
http://localhost:9090/targets
```

## Metrics

Backend:

- `http_requests_total`
- `http_request_duration_seconds`
- `active_websocket_connections`
- `dashboard_websocket_messages_total`
- `alert_websocket_messages_total`

ML inference:

- `ml_predictions_total`
- `ml_anomalies_total`
- `ml_prediction_errors_total`
- `ml_inference_latency_seconds`
- `ml_model_loaded_status`

Storage sink:

- `records_persisted_total`
- `postgres_write_errors_total`
- `elasticsearch_index_errors_total`
- `storage_sink_lag_estimate`

Alert engine:

- `alerts_generated_total`
- `incidents_created_total`
- `alert_deduplications_total`
- `critical_alerts_total`
- `alert_engine_errors_total`

Kafka telemetry services:

- `events_published_total`
- `events_consumed_total`
- `producer_errors_total`
- `consumer_errors_total`

## Grafana Dashboards

Dashboards are provisioned automatically from `monitoring/grafana/dashboards/`:

- System Overview
- Streaming Pipeline
- ML Inference
- SOC Alert Engine

## Backend Monitoring APIs

- `GET /api/v1/monitoring/overview`
- `GET /api/v1/monitoring/services`
- `GET /api/v1/monitoring/pipeline`
- `GET /api/v1/monitoring/errors`
- `GET /api/v1/monitoring/metrics-summary`

The APIs query Prometheus when available and gracefully degrade if Prometheus is still starting. PostgreSQL and Elasticsearch checks are also optional-safe where appropriate.

## Frontend

New route:

```text
http://localhost:5173/observability
```

The Observability page shows:

- service health grid
- Prometheus target status
- pipeline record counts
- key metric counters
- Grafana and Prometheus links
- error and dead-letter summaries

The System Health page also includes Prometheus/Grafana-aware service checks.

## Verification

Start the stack:

```bash
docker compose up --build -d
```

Verify containers:

```bash
docker ps --filter "name=streaming-analytics"
```

Verify metrics:

```bash
curl http://localhost:8000/metrics
curl http://localhost:9101/metrics
curl http://localhost:9102/metrics
curl http://localhost:9103/metrics
curl http://localhost:9104/metrics
```

Verify monitoring APIs:

```bash
curl http://localhost:8000/api/v1/monitoring/overview
curl http://localhost:8000/api/v1/monitoring/services
curl http://localhost:8000/api/v1/monitoring/metrics-summary
```

Open:

- `http://localhost:9090`
- `http://localhost:9090/targets`
- `http://localhost:3000`
- `http://localhost:5173/observability`
- `http://localhost:5173/health`

## Troubleshooting

If Prometheus targets show `DOWN`, wait for the application containers to finish startup and refresh `/targets`.

If `/api/v1/monitoring/metrics-summary` reports Prometheus unavailable, verify the backend environment has:

```text
PROMETHEUS_URL=http://streaming-analytics-prometheus:9090
```

If a metrics endpoint is unavailable on localhost, check that the service container is running and that its metrics port is exposed in `docker-compose.yml`.

Grafana dashboards are provisioned at container startup. If a dashboard is missing, restart Grafana:

```bash
docker compose restart grafana
```
