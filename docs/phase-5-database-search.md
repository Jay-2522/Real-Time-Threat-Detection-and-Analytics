# Phase 5: Database And Search Architecture

Phase 5 adds local-only persistence, search, and query APIs. It uses free Docker Compose services only: PostgreSQL, Elasticsearch, and a Python storage sink that consumes Kafka streams and writes durable/queryable records.

No AWS, managed databases, managed Elasticsearch, paid Kafka, or cloud services are used.

## Architecture

```text
Kafka telemetry, analytics, and ML topics
    -> storage-sink Kafka consumer
    -> PostgreSQL structured records
    -> Elasticsearch searchable documents
    -> FastAPI query APIs
```

## Local Services

| Service | Container | Port | Purpose |
| --- | --- | --- | --- |
| `postgres` | `streaming-analytics-postgres` | `5432` | Structured analytics and prediction storage |
| `elasticsearch` | `streaming-analytics-elasticsearch` | `9200` | Local full-text/event search |
| `storage-sink` | `streaming-analytics-storage-sink` | none | Kafka persistence worker |

Elasticsearch runs in single-node mode with `xpack.security.enabled=false` and a 512 MB heap for local development.

## Kafka Inputs

The storage sink consumes:

- `telemetry.login.events`
- `telemetry.api.events`
- `telemetry.network.events`
- `telemetry.anomaly.events`
- `analytics.enriched.events`
- `analytics.window.metrics`
- `analytics.risk.metrics`
- `analytics.feature.engineering`
- `ml.anomaly.predictions`

Malformed sink records are persisted to `deadletter_events` and can also continue through the existing Kafka dead-letter topic.

## PostgreSQL Schema

Tables:

- `telemetry_events`
- `analytics_metrics`
- `risk_metrics`
- `feature_snapshots`
- `anomaly_predictions`
- `deadletter_events`

Common columns:

- `id`
- `event_id` or `prediction_id`
- `event_type`
- `entity_id`
- `source_topic`
- `timestamp`
- `risk_score` or `ml_risk_score`
- `severity`
- `raw_payload` as JSON/JSONB
- `created_at`

Indexes are created for:

- `timestamp`
- `event_type`
- `entity_id`
- `severity`
- `source_topic`

Local table creation is automatic in both the storage sink and backend startup paths.

## Elasticsearch Indexes

Indexes:

- `telemetry-events`
- `analytics-events`
- `ml-predictions`
- `deadletter-events`

Documents include:

- `timestamp`
- `event_type`
- `entity_id`
- `source_ip`
- `risk_score`
- `ml_risk_score`
- `severity`
- `explanation`
- `source_topic`
- `raw_payload`

Elasticsearch is optional-safe. If unavailable, PostgreSQL persistence continues and the sink logs a warning.

## FastAPI Query APIs

Endpoints:

- `GET /api/v1/analytics/summary`
- `GET /api/v1/events/recent`
- `GET /api/v1/predictions/recent`
- `GET /api/v1/predictions/{entity_id}`
- `GET /api/v1/risks/high`
- `GET /api/v1/search/events?q=...`

PostgreSQL backs summaries, recent records, predictions, and high-risk queries. Elasticsearch backs search and returns an empty result set if search is temporarily unavailable.

## Configuration

```text
POSTGRES_HOST=streaming-analytics-postgres
POSTGRES_PORT=5432
POSTGRES_DB=streaming_analytics
POSTGRES_USER=streaming_user
POSTGRES_PASSWORD=streaming_password
DATABASE_URL=postgresql+psycopg://streaming_user:streaming_password@streaming-analytics-postgres:5432/streaming_analytics
ELASTICSEARCH_HOST=http://streaming-analytics-elasticsearch:9200
STORAGE_SINK_LOG_LEVEL=INFO
```

## Running Locally

Start from a clean local state:

```bash
docker compose down -v
docker compose up --build -d
docker ps
```

Storage sink logs:

```bash
docker logs -f streaming-analytics-storage-sink
```

Expected log signals:

- Kafka connected.
- PostgreSQL connected and schema initialized.
- Elasticsearch connected or optional warning.
- Records persisted.

## PostgreSQL Verification

Open `psql`:

```bash
docker exec -it streaming-analytics-postgres psql -U streaming_user -d streaming_analytics
```

Run:

```sql
SELECT COUNT(*) FROM anomaly_predictions;
SELECT COUNT(*) FROM telemetry_events;
SELECT COUNT(*) FROM analytics_metrics;
```

## Elasticsearch Verification

```bash
curl http://localhost:9200/_cluster/health
curl http://localhost:9200/_cat/indices?v
```

## API Verification

Open:

- `http://localhost:8000/api/v1/analytics/summary`
- `http://localhost:8000/api/v1/events/recent`
- `http://localhost:8000/api/v1/predictions/recent`
- `http://localhost:8000/api/v1/risks/high`
- `http://localhost:8000/api/v1/search/events?q=login`

## Troubleshooting

If backend APIs return empty lists, confirm the storage sink is running and Kafka topics have data.

If Elasticsearch is yellow in single-node mode, that is acceptable for local development with one node.

If Elasticsearch consumes too much memory, stop the stack and lower `ES_JAVA_OPTS` in `docker-compose.yml`.

If storage sink logs Elasticsearch warnings, PostgreSQL should still continue receiving records and the outbox will retry indexing.

