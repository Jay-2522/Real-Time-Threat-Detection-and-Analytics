# Phase 2 Kafka Streaming Foundation

Phase 2 adds a local Kafka-based telemetry pipeline for validating real-time event flow before Spark, ML, storage, and dashboard modules are introduced.

## Architecture

```text
Synthetic Event Generator
        |
        v
Kafka Producer
        |
        v
Kafka Topics
        |
        v
Kafka Consumer
        |
        v
Validated Streaming Telemetry
```

## Services

| Container | Purpose |
| --- | --- |
| `streaming-analytics-zookeeper` | Coordinates the local Kafka broker. |
| `streaming-analytics-kafka` | Kafka broker with internal and external listeners. |
| `streaming-analytics-kafka-init` | Creates Phase 2 topics at startup. |
| `streaming-analytics-producer` | Generates and publishes validated telemetry events. |

## Kafka Connectivity

- Docker services use `streaming-analytics-kafka:29092`.
- Local development tools use `localhost:9092`.

## Topics

| Topic | Partitions | Replication | Purpose |
| --- | ---: | ---: | --- |
| `telemetry.login.events` | 3 | 1 | Login attempts, identity metadata, and authentication risk. |
| `telemetry.api.events` | 3 | 1 | API request telemetry and service response behavior. |
| `telemetry.network.events` | 3 | 1 | Network flow metadata and traffic risk signals. |
| `telemetry.anomaly.events` | 2 | 1 | Synthetic anomaly events injected for future detection workflows. |
| `telemetry.deadletter.events` | 1 | 1 | Invalid, malformed, or unprocessable telemetry messages. |

## Event Contracts

All generated events are validated with Pydantic before publishing. The runtime supports:

- UUID event ids.
- UTC timestamps.
- Discriminated event types.
- Strict schemas with extra fields rejected.
- JSON serialization for Kafka payloads.
- Storage-sink dead-letter publication for invalid Kafka records.

## Run Locally

```bash
docker compose up --build -d
```

Inspect running containers:

```bash
docker ps
```

Inspect producer output:

```bash
docker logs streaming-analytics-producer --tail 100
```

List topics from the Kafka container:

```bash
docker exec streaming-analytics-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Configuration

Producer settings:

- `KAFKA_BOOTSTRAP_SERVERS`
- `EVENT_RATE_PER_SECOND`
- `ANOMALY_RATE`
- `PRODUCER_LOG_LEVEL`

## Troubleshooting

If the producer container restarts, check Kafka readiness first:

```bash
docker logs streaming-analytics-kafka --tail 100
docker logs streaming-analytics-kafka-init
```

If local tools cannot connect to Kafka, use `localhost:9092`. Docker containers should use `streaming-analytics-kafka:29092`.

## Phase Boundary

This phase intentionally does not implement Spark Structured Streaming, ML models, database persistence, Elasticsearch indexing, or dashboard integration. It establishes the streaming substrate those phases will consume.
