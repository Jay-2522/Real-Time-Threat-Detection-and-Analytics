# Kafka Telemetry Producer

Python runtime for Phase 2 synthetic telemetry production.

## Responsibilities

- Generate telemetry from a stable synthetic company (users, workstations, servers) plus labelled attack scenarios.
- Validate all outbound events with Pydantic schemas.
- Publish events to Kafka topics with retry-safe producer configuration.

## Runtime

```bash
python -m app.main producer
```

Follow only new lines appended to a live Cowrie JSON log:

```bash
python -m app.main cowrie-live --file /path/to/cowrie.json
```

Replay a saved log once:

```bash
python -m app.main cowrie-replay --file /path/to/cowrie.json --time-shift
```

Use `--time-shift` for historical logs when Spark is already running or has
seen newer events. Spark's event-time watermark treats records sufficiently
older than the newest observed timestamp as late and can drop them from windowed
results. Time-shifting moves the earliest supported Cowrie event to the current
time and keeps every original gap, so a ten-second burst remains a ten-second
burst. The actual Cowrie time remains in `original_timestamp`. Omit the option
for live ingestion or when historical event times must drive the windows.

## Generated traffic

Traffic comes from `pipeline_runtime/traffic_model.py`, which the ML service also trains on. Benign users keep a
fixed office IP and country, so source-IP windows hold several events. Attack scenarios interleave with it:
brute force, API abuse, lateral movement, data exfiltration, DNS beaconing and a login from a high-risk country.
Each scenario is built to satisfy the matching detection rule or the model's window features, and benign
traffic never satisfies a detection rule. The data is synthetic and says nothing about real attackers.

## Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker list. |
| `EVENT_RATE_PER_SECOND` | `5` | Producer event generation rate. |
| `ANOMALY_RATE` | `0.05` | Long-run fraction of events that belong to an attack scenario. |
| `PRODUCER_LOG_LEVEL` | `INFO` | Producer logging level. |

## Topics

- `telemetry.login.events`
- `telemetry.api.events`
- `telemetry.network.events`
- `telemetry.anomaly.events`
- `telemetry.deadletter.events`
