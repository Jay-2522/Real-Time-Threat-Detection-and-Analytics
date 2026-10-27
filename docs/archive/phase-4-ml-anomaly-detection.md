# Phase 4: Machine Learning And Anomaly Detection

Phase 4 adds a production-style ML anomaly detection layer to the streaming analytics platform. The service trains an Isolation Forest model on synthetic cybersecurity feature data, persists model artifacts, loads them in a real-time inference worker, consumes analytics events from Kafka, and publishes validated ML prediction events.

This phase intentionally does not add database storage, Elasticsearch, dashboard ML visualizations, MLflow, Kubernetes, or cloud deployment.

## Architecture

```text
analytics.* Kafka topics
    -> ML inference consumer
    -> feature parser and safe default mapper
    -> StandardScaler
    -> Isolation Forest anomaly detector
    -> risk scoring and severity assignment
    -> ml.anomaly.predictions Kafka topic
```

## Services

| Service | Container | Purpose |
| --- | --- | --- |
| `ml-inference` | `streaming-analytics-ml-inference` | Long-running real-time ML inference worker |

The inference container auto-bootstraps model artifacts for local development if they are missing.

## Kafka Topics

Input topics:

- `analytics.enriched.events`
- `analytics.window.metrics`
- `analytics.risk.metrics`
- `analytics.feature.engineering`

Output topic:

- `ml.anomaly.predictions`

Dead-letter topic:

- `telemetry.deadletter.events`

`ml.anomaly.predictions` is created automatically by the Compose `kafka-init` service with three partitions and local replication factor `1`.

## Feature Contract

The model is trained and served on this feature set:

- `failed_login_rate`
- `requests_per_minute`
- `avg_api_response_time`
- `endpoint_error_rate`
- `suspicious_ip_frequency`
- `avg_network_bytes`
- `anomaly_rate`
- `risk_score_moving_average`
- `geo_login_variance`
- `high_risk_event_count`
- `failed_auth_count`
- `api_5xx_rate`
- `network_bytes_spike_score`
- `unique_ip_count`
- `session_activity_score`

Streaming analytics messages do not always contain all features. The inference service maps available fields and fills missing values with conservative defaults from the feature contract.

## Training Pipeline

The offline training pipeline creates realistic synthetic security feature data, fits a scaler, trains an Isolation Forest model, evaluates predictions against generated anomaly labels, and saves artifacts.

Artifacts are written to `ml-models/artifacts/`:

- `isolation_forest.joblib`
- `feature_scaler.joblib`
- `feature_metadata.json`
- `training_metrics.json`

Run training manually:

```bash
docker compose run --rm ml-inference python -m app.training.train_model
```

The inference runtime also trains a default local model automatically when artifacts are missing and `ML_AUTO_BOOTSTRAP_MODEL=true`.

## Prediction Schema

Prediction events published to `ml.anomaly.predictions` include:

- `prediction_id`
- `source_event_id`
- `timestamp`
- `model_name`
- `model_version`
- `event_type`
- `entity_id`
- `anomaly_score`
- `is_anomaly`
- `ml_risk_score`
- `confidence_score`
- `severity`
- `explanation`
- `features_used`

Severity values are:

- `low`
- `medium`
- `high`
- `critical`

## Configuration

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `streaming-analytics-kafka:29092` |
| `ML_CONSUMER_GROUP` | `streaming-analytics-ml-inference` |
| `ML_LOG_LEVEL` | `INFO` |
| `ML_ARTIFACT_DIR` | `/app/artifacts` |
| `ML_MODEL_NAME` | `isolation_forest_anomaly_detector` |
| `ML_MODEL_VERSION` | `0.1.0` |
| `ML_PREDICTION_TOPIC` | `ml.anomaly.predictions` |
| `ML_AUTO_BOOTSTRAP_MODEL` | `true` |
| `ML_TRAINING_SAMPLE_SIZE` | `12000` |
| `ML_CONTAMINATION` | `0.06` |

## Running Locally

Start the full platform:

```bash
docker compose up --build -d
```

Start or rebuild only ML inference:

```bash
docker compose up --build -d ml-inference
```

Inspect inference logs:

```bash
docker logs -f streaming-analytics-ml-inference
```

Verify prediction topic creation:

```bash
docker exec streaming-analytics-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Sample predictions:

```bash
docker exec streaming-analytics-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ml.anomaly.predictions \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 30000
```

## Operational Notes

- Inference uses `auto.offset.reset=earliest` so local development can score existing analytics messages after startup.
- The model artifact directory is bind-mounted from `./ml-models/artifacts` to `/app/artifacts`.
- Unparseable analytics messages are published to `telemetry.deadletter.events`.
- The prediction producer uses idempotent Kafka delivery settings.
- Graceful shutdown closes the Kafka consumer and flushes pending prediction/dead-letter messages.

## Troubleshooting

If artifacts are missing and auto-bootstrap is disabled, run:

```bash
docker compose run --rm ml-inference python -m app.training.train_model
```

If no predictions are produced, confirm Spark analytics topics are receiving events:

```bash
docker exec streaming-analytics-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic analytics.enriched.events \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 30000
```

If Kafka connection retries continue, check that `streaming-analytics-kafka` is healthy and that `KAFKA_BOOTSTRAP_SERVERS` is set to `streaming-analytics-kafka:29092` inside Docker.

