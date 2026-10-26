"""Measurement primitives shared by the benchmark orchestrator and tests."""

from __future__ import annotations

import json
import math
import re
import subprocess
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

from confluent_kafka import Consumer, KafkaError


RAW_TOPICS = {
    "telemetry.login.events",
    "telemetry.api.events",
    "telemetry.network.events",
}
CAPTURE_TOPICS = sorted(
    RAW_TOPICS
    | {
        "analytics.enriched.events",
        "analytics.feature.engineering",
        "ml.anomaly.predictions",
    }
)


def percentile(values: Iterable[float], quantile: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize(values: Iterable[float]) -> dict[str, float | int | None]:
    materialized = list(values)
    return {
        "samples": len(materialized),
        "median": percentile(materialized, 0.5),
        "p95": percentile(materialized, 0.95),
        "min": min(materialized) if materialized else None,
        "max": max(materialized) if materialized else None,
    }


def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def window_latency_parts(
    first_event_at: datetime,
    window_end: datetime,
    alert_created_at: datetime,
    watermark_seconds: float,
) -> tuple[float, float, float]:
    """Split observed window latency into deliberate event-time wait and overhead."""

    total = (alert_created_at - first_event_at).total_seconds()
    deliberate = max(0.0, (window_end - first_event_at).total_seconds() + watermark_seconds)
    return total, deliberate, total - deliberate


def parse_memory_mib(value: str) -> float:
    match = re.match(r"\s*([0-9.]+)([KMGT]?i?B)", value)
    if not match:
        raise ValueError(f"Unrecognized memory value: {value}")
    amount = float(match.group(1))
    factor = {"B": 1 / 1_048_576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "TiB": 1_048_576}
    return amount * factor[match.group(2)]


def parse_consumer_lag(output: str) -> dict[str, int]:
    totals: dict[str, int] = defaultdict(int)
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 6 or fields[0] == "GROUP":
            continue
        try:
            lag = int(fields[5])
        except ValueError:
            continue
        totals[fields[0]] += lag
    return dict(totals)


def fingerprint(payload: dict[str, Any]) -> tuple[str, ...]:
    return tuple(sorted(str(item) for item in payload.get("source_event_ids") or []))


@dataclass
class TopicCapture:
    bootstrap_servers: str
    group_id: str
    raw: dict[str, tuple[int, float]] = field(default_factory=dict)
    enriched: dict[str, int] = field(default_factory=dict)
    features: dict[tuple[str, ...], tuple[int, dict[str, Any]]] = field(default_factory=dict)
    predictions: dict[tuple[str, ...], tuple[int, dict[str, Any]]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: threading.Thread | None = field(default=None, init=False)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._consume, name="benchmark-topic-capture", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=15)

    def _consume(self) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe(CAPTURE_TOPICS)
        try:
            while not self._stop.is_set():
                message = consumer.poll(0.2)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        self.errors.append(str(message.error()))
                    continue
                try:
                    payload = json.loads(message.value())
                    timestamp_ms = int(message.timestamp()[1])
                    topic = message.topic()
                    if topic in RAW_TOPICS:
                        self.raw[str(payload["event_id"])] = (
                            timestamp_ms,
                            parse_datetime(str(payload["timestamp"])).timestamp(),
                        )
                    elif topic == "analytics.enriched.events":
                        self.enriched[str(payload["event_id"])] = timestamp_ms
                    elif topic == "analytics.feature.engineering":
                        self.features[fingerprint(payload)] = (timestamp_ms, payload)
                    elif topic == "ml.anomaly.predictions":
                        self.predictions[fingerprint(payload)] = (timestamp_ms, payload)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                    self.errors.append(f"{message.topic()}:{message.partition()}:{message.offset()}:{error}")
        finally:
            consumer.close()

    def kafka_latency_summary(self) -> dict[str, dict[str, float | int | None]]:
        raw_to_enriched = [
            (timestamp - self.raw[event_id][0]) / 1000
            for event_id, timestamp in self.enriched.items()
            if event_id in self.raw
        ]
        enriched_to_feature = []
        feature_to_prediction = []
        for ids, (feature_timestamp, _payload) in self.features.items():
            source_timestamps = [self.enriched[event_id] for event_id in ids if event_id in self.enriched]
            if source_timestamps:
                enriched_to_feature.append((feature_timestamp - max(source_timestamps)) / 1000)
            prediction = self.predictions.get(ids)
            if prediction:
                feature_to_prediction.append((prediction[0] - feature_timestamp) / 1000)
        return {
            "raw_to_enriched": summarize(raw_to_enriched),
            "enriched_to_feature": summarize(enriched_to_feature),
            "feature_to_prediction": summarize(feature_to_prediction),
        }

    def watermark_coverage(self, watermark_seconds: int) -> dict[str, int | float | None]:
        if not self.raw:
            return {
                "out_of_order_events": 0,
                "eligible_event_window_memberships": 0,
                "observed_event_window_memberships": 0,
                "missing_event_window_memberships": 0,
                "missing_membership_rate": None,
            }
        max_event_epoch = max(event_epoch for _broker, event_epoch in self.raw.values())
        watermark = max_event_epoch - watermark_seconds
        expected = set()
        out_of_order = 0
        for event_id, (broker_ms, event_epoch) in self.raw.items():
            out_of_order += int(broker_ms / 1000 - event_epoch > 1.0)
            latest = int(event_epoch // 30) * 30
            for start in (latest - 30, latest):
                if start + 60 <= watermark:
                    expected.add((event_id, start))
        observed = set()
        for _ids, (_timestamp, payload) in self.features.items():
            start = int(parse_datetime(payload["window_start"]).timestamp())
            observed.update((str(event_id), start) for event_id in payload.get("source_event_ids") or [])
        matched = expected & observed
        missing = len(expected - observed)
        return {
            "out_of_order_events": out_of_order,
            "eligible_event_window_memberships": len(expected),
            "observed_event_window_memberships": len(matched),
            "missing_event_window_memberships": missing,
            "missing_membership_rate": missing / len(expected) if expected else None,
        }


@dataclass
class RuntimeSampler:
    compose: list[str]
    interval_seconds: float = 5.0
    fixed_elapsed_seconds: float = 60.0
    resource_samples: dict[str, list[tuple[float, float]]] = field(default_factory=lambda: defaultdict(list))
    lag_samples: list[tuple[float, dict[str, int]]] = field(default_factory=list)
    snapshots: list[tuple[float, dict[str, tuple[float, float]]]] = field(default_factory=list)
    fixed_snapshot: tuple[float, dict[str, tuple[float, float]]] | None = None
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: threading.Thread | None = field(default=None, init=False)
    _fixed_thread: threading.Thread | None = field(default=None, init=False)
    _started: float | None = field(default=None, init=False)

    def start(self) -> None:
        self._started = time.monotonic()
        self._thread = threading.Thread(target=self._sample, name="benchmark-runtime-sampler", daemon=True)
        self._fixed_thread = threading.Thread(target=self._sample_fixed, name="benchmark-fixed-memory-sampler", daemon=True)
        self._thread.start()
        self._fixed_thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval_seconds + 5)
        if self._fixed_thread:
            self._fixed_thread.join(timeout=5)

    def _sample(self) -> None:
        started = self._started or time.monotonic()
        next_sample = started
        while not self._stop.is_set():
            try:
                sample_started = time.monotonic() - started
                output = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
                snapshot = {}
                for line in output.splitlines():
                    row = json.loads(line)
                    cpu = float(row["CPUPerc"].rstrip("%"))
                    memory = parse_memory_mib(row["MemUsage"])
                    self.resource_samples[row["Name"]].append((cpu, memory))
                    if row["Name"].startswith("streaming-analytics-"):
                        snapshot[row["Name"]] = (cpu, memory)
                self.snapshots.append((sample_started, snapshot))
                lag_output = subprocess.run(
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
                self.lag_samples.append((time.monotonic() - started, parse_consumer_lag(lag_output)))
            except (KeyError, ValueError, json.JSONDecodeError, subprocess.SubprocessError):
                pass
            next_sample += self.interval_seconds
            self._stop.wait(max(0.0, next_sample - time.monotonic()))

    def _sample_fixed(self) -> None:
        started = self._started or time.monotonic()
        if self._stop.wait(self.fixed_elapsed_seconds):
            return
        sample_started = time.monotonic() - started
        try:
            output = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            snapshot = {}
            for line in output.splitlines():
                row = json.loads(line)
                if row["Name"].startswith("streaming-analytics-"):
                    snapshot[row["Name"]] = (
                        float(row["CPUPerc"].rstrip("%")),
                        parse_memory_mib(row["MemUsage"]),
                    )
            self.fixed_snapshot = (sample_started, snapshot)
        except (KeyError, ValueError, json.JSONDecodeError, subprocess.SubprocessError):
            self.fixed_snapshot = None

    def summary(self) -> tuple[dict[str, dict[str, float | None]], dict[str, dict[str, int | None]]]:
        resources = {}
        for container, samples in sorted(self.resource_samples.items()):
            cpus = [sample[0] for sample in samples]
            memories = [sample[1] for sample in samples]
            resources[container] = {
                "cpu_median_percent": percentile(cpus, 0.5),
                "cpu_max_percent": max(cpus) if cpus else None,
                "memory_median_mib": percentile(memories, 0.5),
                "memory_max_mib": max(memories) if memories else None,
            }
        groups = {group for _, sample in self.lag_samples for group in sample}
        lag = {}
        for group in sorted(groups):
            values = [sample.get(group, 0) for _, sample in self.lag_samples]
            lag[group] = {
                "max": max(values) if values else None,
                "final": values[-1] if values else None,
            }
        return resources, lag

    def memory_at(self, elapsed_seconds: float) -> dict[str, float | int | None]:
        if self.fixed_snapshot is not None and elapsed_seconds == self.fixed_elapsed_seconds:
            observed, snapshot = self.fixed_snapshot
            return {
                "target_seconds": elapsed_seconds,
                "observed_seconds": observed,
                "total_memory_mib": sum(memory for _cpu, memory in snapshot.values()),
                "running_services": len(snapshot),
            }
        if not self.snapshots:
            return {
                "target_seconds": elapsed_seconds,
                "observed_seconds": None,
                "total_memory_mib": None,
                "running_services": 0,
            }
        observed, snapshot = min(self.snapshots, key=lambda sample: abs(sample[0] - elapsed_seconds))
        return {
            "target_seconds": elapsed_seconds,
            "observed_seconds": observed,
            "total_memory_mib": sum(memory for _cpu, memory in snapshot.values()),
            "running_services": len(snapshot),
        }
