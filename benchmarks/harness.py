"""Clean-volume orchestration for a single benchmark run."""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from dotenv import dotenv_values

from benchmarks.loadgen import LoadConfig, make_producer, run_load
from benchmarks.measurements import RuntimeSampler, TopicCapture, parse_datetime, summarize, window_latency_parts


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker-compose", "-f", str(ROOT / "docker-compose.yml"), "-f", str(ROOT / "benchmarks/compose.yml")]


@dataclass(frozen=True)
class RunConfig:
    rate: float
    duration: float
    repetition: int
    source_ips: int = 100
    out_of_order_fraction: float = 0.0
    watermark_seconds: int = 120
    drain_timeout_seconds: int = 240


def _environment(config: RunConfig) -> dict[str, str]:
    environment = os.environ.copy()
    environment["DEV_BIND_ROOT"] = str(ROOT)
    environment["SYNTHETIC_PRODUCER_ENABLED"] = "false"
    environment["BENCHMARK_WATERMARK"] = f"{config.watermark_seconds} seconds"
    return environment


def _run(command: list[str], environment: dict[str, str], *, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, env=environment, check=True, text=True, capture_output=capture)


def _wait_healthy(environment: dict[str, str], timeout_seconds: int = 600) -> float:
    started = time.monotonic()
    while time.monotonic() - started < timeout_seconds:
        ids = _run(COMPOSE + ["ps", "--status", "running", "-q"], environment, capture=True).stdout.split()
        states = []
        if ids:
            states = subprocess.run(
                ["docker", "inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}", *ids],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.split()
        if len(ids) == 15 and states.count("healthy") == 15:
            return time.monotonic() - started
        if "unhealthy" in states:
            raise RuntimeError(f"Stack became unhealthy: {states}")
        time.sleep(2)
    raise TimeoutError("Stack did not reach 15/15 healthy services")


def _current_lag() -> dict[str, int]:
    output = subprocess.run(
        [
            "docker",
            "exec",
            "streaming-analytics-kafka",
            "kafka-consumer-groups",
            "--bootstrap-server",
            "localhost:9092",
            "--all-groups",
            "--describe",
        ],
        check=False,
        capture_output=True,
        text=True,
    ).stdout
    from benchmarks.measurements import parse_consumer_lag

    return parse_consumer_lag(output)


def _wait_for_drain(timeout_seconds: int, capture: TopicCapture, expected_events: int) -> tuple[float, bool]:
    started = time.monotonic()
    zero_observations = 0
    while time.monotonic() - started < timeout_seconds:
        lag = _current_lag()
        relevant = {group: value for group, value in lag.items() if not group.startswith("benchmark-capture-")}
        spark_caught_up = len(capture.enriched) >= expected_events
        if relevant and all(value == 0 for value in relevant.values()) and spark_caught_up:
            zero_observations += 1
            if zero_observations >= 3:
                return time.monotonic() - started, True
        else:
            zero_observations = 0
        time.sleep(5)
    return time.monotonic() - started, False


def _database_connection() -> psycopg.Connection:
    values = {**dotenv_values(ROOT / ".env"), **os.environ}
    port = subprocess.run(
        ["docker", "port", "streaming-analytics-postgres", "5432/tcp"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()[0].rsplit(":", 1)[1]
    password = subprocess.run(
        ["docker", "exec", "streaming-analytics-postgres", "printenv", "POSTGRES_PASSWORD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return psycopg.connect(
        host="127.0.0.1",
        port=port,
        dbname=values.get("POSTGRES_DB", "streaming_analytics"),
        user=values.get("POSTGRES_USER", "streaming_user"),
        password=password,
    )


def _database_summary(capture: TopicCapture, sent: int, watermark_seconds: int) -> dict[str, Any]:
    with _database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*), count(DISTINCT event_id) FROM telemetry_events")
            persisted, distinct = cursor.fetchone()
            cursor.execute("SELECT source_topic, source_event_id, raw_payload, created_at FROM alerts")
            alert_rows = cursor.fetchall()

    direct = []
    windowed = []
    deliberate = []
    processing = []
    for source_topic, source_event_id, payload, created_at in alert_rows:
        created_ms = int(created_at.timestamp() * 1000)
        if source_topic == "analytics.enriched.events" and source_event_id in capture.enriched:
            direct.append((created_ms - capture.enriched[source_event_id]) / 1000)
        if source_topic == "ml.anomaly.predictions":
            ids = tuple(sorted(str(item) for item in payload.get("source_event_ids") or []))
            feature = capture.features.get(ids)
            if not feature:
                continue
            feature_timestamp, feature_payload = feature
            windowed.append((created_ms - feature_timestamp) / 1000)
            if feature_payload.get("first_event_at") and feature_payload.get("window_end"):
                total, expected_wait, overhead = window_latency_parts(
                    parse_datetime(feature_payload["first_event_at"]),
                    parse_datetime(feature_payload["window_end"]),
                    created_at,
                    watermark_seconds,
                )
                deliberate.append(expected_wait)
                processing.append(overhead)
    return {
        "events_sent": sent,
        "telemetry_rows": persisted,
        "distinct_telemetry_rows": distinct,
        "lost": max(0, sent - distinct),
        "duplicates": max(0, persisted - distinct),
        "direct_enriched_to_alert_seconds": summarize(direct),
        "window_feature_to_alert_seconds": summarize(windowed),
        "window_total_seconds": summarize(
            deliberate[index] + processing[index] for index in range(min(len(deliberate), len(processing)))
        ),
        "window_deliberate_seconds": summarize(deliberate),
        "window_processing_seconds": summarize(processing),
    }


def run_once(config: RunConfig) -> dict[str, Any]:
    environment = _environment(config)
    run_id = f"bench-{int(config.rate)}-{config.repetition}-{uuid4().hex[:10]}"
    _run(COMPOSE + ["down", "-v", "--remove-orphans"], environment)
    started = time.monotonic()
    _run(COMPOSE + ["up", "--build", "-d"], environment)
    healthy_wait = _wait_healthy(environment)
    startup_seconds = time.monotonic() - started

    capture = TopicCapture("127.0.0.1:9092", f"benchmark-capture-{run_id}")
    sampler = RuntimeSampler(COMPOSE)
    capture.start()
    sampler.start()
    time.sleep(2)
    load = run_load(
        LoadConfig(
            rate=config.rate,
            duration=config.duration,
            source_ips=config.source_ips,
            out_of_order_fraction=config.out_of_order_fraction,
            run_id=run_id,
        ),
        make_producer("127.0.0.1:9092"),
    )
    drain_seconds, drained = _wait_for_drain(config.drain_timeout_seconds, capture, load.delivered)
    time.sleep(5)
    sampler.stop()
    capture.stop()
    resources, lag = sampler.summary()
    database = _database_summary(capture, load.delivered, config.watermark_seconds)
    return {
        "config": asdict(config),
        "run_id": run_id,
        "startup_seconds": startup_seconds,
        "healthy_poll_seconds": healthy_wait,
        "load": asdict(load),
        "drain_seconds": drain_seconds,
        "drained": drained,
        "database": database,
        "kafka_latency_seconds": capture.kafka_latency_summary(),
        "consumer_lag": lag,
        "resources": resources,
        "capture_errors": len(capture.errors),
    }
