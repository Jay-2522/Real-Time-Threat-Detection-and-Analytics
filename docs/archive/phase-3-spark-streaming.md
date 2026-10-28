# Phase 3: Spark Structured Streaming Pipeline

Phase 3 adds a production-style Spark Structured Streaming analytics layer on top of the Kafka telemetry foundation. The Spark job consumes real-time cybersecurity telemetry, normalizes and enriches events, computes event-time window metrics, engineers streaming features, and publishes analytics output back into Kafka.

This phase intentionally does not implement ML models, persistence stores, dashboard visualizations, or Kubernetes. It establishes the distributed analytics processing foundation those later phases will build on.

## Streaming Architecture

```text
Telemetry Producer
    -> telemetry.* Kafka topics
    -> Spark Structured Streaming
    -> parsing, validation, normalization
    -> enrichment and risk scoring features
    -> event-time aggregations
    -> analytics.* Kafka topics
```

## Docker Services

Phase 3 extends `docker-compose.yml` with:

| Service | Container | Purpose |
| --- | --- | --- |
| `spark-streaming` | `streaming-analytics-spark-streaming` | PySpark Structured Streaming job using two local worker threads |

Spark UI endpoints:

- Spark Driver / Jobs UI: `http://localhost:4040`

## Kafka Inputs

Spark consumes these telemetry topics:

| Topic | Purpose |
| --- | --- |
| `telemetry.login.events` | Login success, failure, user, country, device, and risk telemetry |
| `telemetry.api.events` | API endpoint, method, status, latency, payload size, and risk telemetry |
| `telemetry.network.events` | Network protocol, byte volume, port, packet, and risk telemetry |
| `telemetry.anomaly.events` | Synthetic anomaly signals produced by the telemetry generator |

The Spark readers use the internal Docker broker address: `streaming-analytics-kafka:29092`.

## Analytics Outputs

Spark publishes these derived topics:

| Topic | Contents |
| --- | --- |
| `analytics.enriched.events` | Per-event normalized telemetry with risk level, normalized risk score, source metadata, and processing timestamp |
| `analytics.window.metrics` | Five-minute tumbling window counts, high-risk counts, risk moving averages, latency metrics, traffic metrics, and anomaly frequency |
| `analytics.risk.metrics` | One-minute sliding source-IP risk metrics, rolling event counts, high-risk counts, and event-type diversity |
| `analytics.feature.engineering` | Real-time ML-ready feature rows such as failed login rate, endpoint error rate, request rate, suspicious IP frequency, geo variance, and network byte averages |

Local replication factor is `1` because the Compose stack runs a single Kafka broker.

## Spark Job Layout

```text
spark-jobs/
  app/
    config/              Runtime settings and topic constants
    schemas/             Spark StructType telemetry schemas
    streaming/           Kafka source readers
    transformations/     JSON parsing, cleansing, normalization, enrichment
    aggregations/        Event-time window metrics
    feature_engineering/ ML-ready streaming feature builders
    sinks/               Kafka sink writers
    utils/               Spark session, logging, and query monitoring
    main.py              Structured Streaming orchestration entrypoint
  Dockerfile
  requirements.txt
  README.md
```

## Transformations

The Spark pipeline performs:

- JSON parsing from Kafka message values.
- Schema-specific parsing for login, API, network, and anomaly events.
- Event-time conversion from telemetry timestamps.
- Risk-score validation and normalization.
- Risk-level enrichment: `low`, `medium`, `high`, `critical`.
- Null handling and invalid event filtering.
- Source topic, partition, offset, Kafka timestamp, and processing timestamp enrichment.

Malformed or schema-incompatible events are filtered out of analytics outputs. Storage-sink publishes malformed Kafka records to the dead-letter topic while persisting their quarantine records.

## Windowing And Features

Event-time processing uses watermarks configured by `SPARK_WATERMARK_DELAY`.

Implemented metrics include:

- Five-minute tumbling event counts.
- One-minute sliding risk metrics.
- High-risk event counts.
- Failed login rates.
- Average API response time.
- Requests per minute.
- Suspicious IP frequency.
- Average network bytes.
- Anomaly frequency and anomaly rate.
- Risk-score moving average.
- Geo-login variance using streaming-safe approximate distinct counts.
- Endpoint error rate.

Structured Streaming does not support exact distinct aggregations on streaming DataFrames, so cardinality features use `approx_count_distinct`, which is the appropriate scalable primitive for local and production streaming jobs.

## Checkpointing And Recovery

The Spark driver writes checkpoints under:

```text
/opt/spark/checkpoints
```

Docker Compose mounts that path to the named volume:

```text
streaming-analytics-spark-checkpoints
```

Each streaming query has a separate checkpoint directory. This allows Spark to recover offsets and state after driver restarts.

## Configuration

Relevant environment variables:

| Variable | Default |
| --- | --- |
| `SPARK_KAFKA_BOOTSTRAP_SERVERS` | `streaming-analytics-kafka:29092` |
| `SPARK_CHECKPOINT_ROOT` | `/opt/spark/checkpoints` |
| `SPARK_STARTING_OFFSETS` | `latest` |
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | `5000` |
| `SPARK_TRIGGER_PROCESSING_TIME` | `15 seconds` |
| `SPARK_WATERMARK_DELAY` | `2 minutes` |
| `SPARK_LOG_LEVEL` | `WARN` |

## Running Locally

Start the full stack:

```bash
docker compose up --build -d
```

Check container status:

```bash
docker ps --filter "name=streaming-analytics"
```

Inspect Spark logs:

```bash
docker logs streaming-analytics-spark-streaming --tail 200
```

List Kafka topics:

```bash
docker exec streaming-analytics-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Sample enriched analytics output:

```bash
docker exec streaming-analytics-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic analytics.enriched.events \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 30000
```

Aggregate topics use event-time watermarks, so `analytics.window.metrics`, `analytics.risk.metrics`, and `analytics.feature.engineering` can take a few minutes to emit after startup.

## Troubleshooting

If the Spark driver repeatedly restarts, inspect the current driver logs:

```bash
docker logs --since 5m streaming-analytics-spark-streaming
```

If Kafka topics are missing, recreate topic initialization:

```bash
docker compose up kafka-init
```

If Spark appears idle, verify the producer is generating telemetry:

```bash
docker logs streaming-analytics-producer --tail 100
```

If aggregate topics are empty immediately after startup, wait for watermark advancement. Enriched events should appear first; aggregate topics emit after event-time windows close.

For a clean streaming replay in local development, stop the stack and remove the Spark checkpoint volume:

```bash
docker compose down
docker volume rm streaming-analytics-spark-checkpoints
docker compose up --build -d
```

