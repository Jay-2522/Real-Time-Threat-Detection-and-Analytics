# ML Anomaly Detection Service

This package contains the Phase 4 machine learning runtime for the Real-Time Streaming Analytics System. It provides offline model training and real-time Kafka inference for anomaly detection over Spark analytics streams.

## Responsibilities

- Generate synthetic cybersecurity feature training data.
- Train an Isolation Forest anomaly detection model.
- Persist model, scaler, metadata, and metrics artifacts.
- Load artifacts in a long-running inference worker.
- Consume analytics events from Kafka.
- Map incoming analytics messages into the ML feature contract.
- Publish prediction events to `ml.anomaly.predictions`.
- Send malformed messages to `telemetry.deadletter.events`.

## Layout

```text
ml-models/
  app/
    config/      Settings and topic constants
    data/        Synthetic training data generation
    features/    Feature definitions and streaming feature mapping
    models/      Artifact persistence and loading
    training/    Offline training pipeline
    inference/   Prediction logic and risk scoring
    schemas/     Pydantic prediction and dead-letter schemas
    producers/   Reserved producer package boundary
    consumers/   Kafka inference consumer
    utils/       Logging and retry helpers
    main.py      Runtime entrypoint
  artifacts/contract-v2/  Generated production-contract model artifacts
  notebooks/     Reserved exploratory workspace
```

## Training

Run the training pipeline:

```bash
docker compose run --rm ml-inference python -m app.training.train_model
```

Generated artifacts:

- `artifacts/contract-v2/isolation_forest.joblib`
- `artifacts/contract-v2/feature_scaler.joblib`
- `artifacts/contract-v2/feature_metadata.json`
- `artifacts/contract-v2/training_metrics.json`

## Inference

Start inference:

```bash
docker compose up --build -d ml-inference
```

Tail logs:

```bash
docker logs -f streaming-analytics-ml-inference
```

Read predictions:

```bash
docker exec streaming-analytics-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ml.anomaly.predictions \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 30000
```

If artifacts are missing, the service trains a default model during startup when `ML_AUTO_BOOTSTRAP_MODEL=true`.

