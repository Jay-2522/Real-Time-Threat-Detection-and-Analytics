# ML Anomaly Detection Service

This package contains the Phase 4 machine learning runtime for the Real-Time Streaming Analytics System. It provides offline model training and real-time Kafka inference for anomaly detection over Spark analytics streams.

## Responsibilities

- Simulate benign and attack traffic with the shared traffic model and window it like Spark does.
- Train an Isolation Forest on benign windows, calibrate its alert threshold, and evaluate it on held-out windows.
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

### How the model is trained and judged

Training uses `pipeline_runtime/traffic_model.py`, the same model that drives the telemetry producer, so the
model is trained on the traffic it later scores. Three independent simulations are used: one to fit the
Isolation Forest on benign windows only, one to set the anomaly threshold so that only
`ML_TARGET_FALSE_POSITIVE_RATE` (default 0.2%) of unseen benign windows are flagged, and one held-out run for
evaluation. The result is written to `training_metrics.json` (`evaluation`): benign false-positive rate and the
detection rate per attack scenario. A model trained on an older traffic profile is retrained automatically on
the next start when `ML_AUTO_BOOTSTRAP_MODEL` is true.

These figures are measured on synthetic data and are not real-world accuracy; the CIC-IDS-2017 results in the
repository README are the independent check. The model covers volume and behaviour attacks (brute force, API
abuse, exfiltration, part of lateral movement). Indicator-based attacks (DNS beaconing on port 53, logins from
watchlist countries) are caught by the YAML detection rules, not the model.

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

