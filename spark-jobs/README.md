# Spark Structured Streaming Jobs

This package contains the Phase 3 real-time analytics engine for the Real-Time Streaming Analytics System. It runs as the `streaming-analytics-spark-streaming` container and submits a PySpark Structured Streaming application to the local Spark standalone cluster.

## Responsibilities

- Read telemetry from Kafka topics.
- Parse and normalize JSON events with strongly typed Spark schemas.
- Enrich events with risk-level, normalized risk score, source metadata, and processing timestamps.
- Build event-time window aggregations.
- Produce ML-ready streaming feature rows.
- Write analytics streams back to Kafka.
- Maintain checkpoint state for restart-safe processing.

## Package Layout

```text
app/
  config/              Settings and Kafka topic constants
  schemas/             StructType definitions for telemetry events
  streaming/           Kafka source readers
  transformations/     Parsing, cleansing, normalization, and enrichment
  aggregations/        Windowed metric builders
  feature_engineering/ Streaming feature builders
  sinks/               Kafka sink writers
  utils/               Spark session, logging, and query monitor utilities
  main.py              Application entrypoint
```

## Runtime

The Docker image is based on `apache/spark:3.5.3` and submits the application with:

```bash
/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --jars /opt/spark/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.3.jar,... \
  app/main.py
```

The Kafka connector jars are cached in the image at build time.

## Configuration

| Environment variable | Purpose |
| --- | --- |
| `SPARK_KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap server list for Spark sources and sinks |
| `SPARK_CHECKPOINT_ROOT` | Root path for streaming checkpoints |
| `SPARK_STARTING_OFFSETS` | Kafka source offset strategy |
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | Backpressure control for source reads |
| `SPARK_TRIGGER_PROCESSING_TIME` | Micro-batch trigger interval |
| `SPARK_WATERMARK_DELAY` | Event-time watermark delay |
| `SPARK_LOG_LEVEL` | Spark runtime log level |

## Local Commands

Build and start only the Spark streaming service after the cluster and Kafka are running:

```bash
docker compose up --build -d spark-streaming
```

Inspect logs:

```bash
docker logs streaming-analytics-spark-streaming --tail 200
```

Sample analytics output:

```bash
docker exec streaming-analytics-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic analytics.enriched.events \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 30000
```

