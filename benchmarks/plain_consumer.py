"""Benchmark-only Python implementation of Spark's one-minute feature windows.

Semantics mirror the product job: one-minute event-time windows sliding every
30 seconds, keyed by source IP. A record at or behind the current
``max_event_time - watermark`` is late and is dropped. Windows are emitted once
their end is at or behind that watermark. Tail windows intentionally remain open
when input stops, just as they do in the Spark append-mode query.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError

from benchmarks.measurements import RAW_TOPICS, summarize


SLIDE_SECONDS = 30
WINDOW_SECONDS = 60


def _epoch(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat()


def window_starts(epoch: float) -> tuple[int, int]:
    latest = int(epoch // SLIDE_SECONDS) * SLIDE_SECONDS
    return latest - SLIDE_SECONDS, latest


@dataclass
class WindowAccumulator:
    source_ip: str
    start: int
    event_ids: list[str] = field(default_factory=list)
    broker_timestamps_ms: list[int] = field(default_factory=list)
    event_count: int = 0
    login_event_count: int = 0
    api_event_count: int = 0
    network_event_count: int = 0
    anomaly_event_count: int = 0
    failed_auth_count: int = 0
    api_error_count: int = 0
    api_5xx_count: int = 0
    api_response_total: float = 0.0
    network_bytes_total: float = 0.0
    high_risk_network_event_count: int = 0
    risk_total: float = 0.0
    high_risk_event_count: int = 0
    login_countries: set[str] = field(default_factory=set)
    destination_ips: set[str] = field(default_factory=set)
    first_event_epoch: float = float("inf")
    last_event_epoch: float = float("-inf")

    def add(self, payload: dict[str, Any], broker_timestamp_ms: int) -> None:
        event_type = payload["event_type"]
        risk = float(payload["risk_score"])
        event_epoch = _epoch(payload["timestamp"])
        self.event_ids.append(str(payload["event_id"]))
        self.broker_timestamps_ms.append(broker_timestamp_ms)
        self.event_count += 1
        self.risk_total += risk
        self.high_risk_event_count += int(risk >= 70)
        self.first_event_epoch = min(self.first_event_epoch, event_epoch)
        self.last_event_epoch = max(self.last_event_epoch, event_epoch)
        if event_type == "login":
            self.login_event_count += 1
            self.failed_auth_count += int(payload["login_success"] is False)
            if payload.get("country"):
                self.login_countries.add(str(payload["country"]))
        elif event_type == "api":
            status = int(payload["status_code"])
            self.api_event_count += 1
            self.api_error_count += int(400 <= status <= 599)
            self.api_5xx_count += int(500 <= status <= 599)
            self.api_response_total += float(payload["response_time_ms"])
        elif event_type == "network":
            self.network_event_count += 1
            self.network_bytes_total += float(payload["bytes_sent"]) + float(payload["bytes_received"])
            self.high_risk_network_event_count += int(risk >= 70)
            self.destination_ips.add(str(payload["destination_ip"]))
        elif event_type == "anomaly":
            self.anomaly_event_count += 1

    @staticmethod
    def _ratio(numerator: int, denominator: int) -> float | None:
        return numerator / denominator if denominator else None

    def emit(self, emitted_ms: int) -> dict[str, Any]:
        return {
            "feature_contract_version": "2.0",
            "event_type": "feature_window",
            "entity_type": "source_ip",
            "entity_id": self.source_ip,
            "source_event_ids": sorted(self.event_ids),
            "invalid_event_count": 0,
            "window_start": _iso(self.start),
            "window_end": _iso(self.start + WINDOW_SECONDS),
            "first_event_at": _iso(self.first_event_epoch),
            "last_event_at": _iso(self.last_event_epoch),
            "calculated_at": _iso(emitted_ms / 1000),
            "benchmark_latency_seconds": (emitted_ms - max(self.broker_timestamps_ms)) / 1000,
            "features": {
                "event_count": float(self.event_count),
                "login_event_count": float(self.login_event_count),
                "api_event_count": float(self.api_event_count),
                "network_event_count": float(self.network_event_count),
                "anomaly_event_count": float(self.anomaly_event_count),
                "failed_login_rate": self._ratio(self.failed_auth_count, self.login_event_count),
                "failed_auth_count": float(self.failed_auth_count),
                "requests_per_minute": float(self.api_event_count),
                "avg_api_response_time": self.api_response_total / self.api_event_count if self.api_event_count else None,
                "endpoint_error_rate": self._ratio(self.api_error_count, self.api_event_count),
                "api_5xx_rate": self._ratio(self.api_5xx_count, self.api_event_count),
                "avg_network_bytes": self.network_bytes_total / self.network_event_count if self.network_event_count else None,
                "high_risk_network_event_count": float(self.high_risk_network_event_count),
                "anomaly_rate": self.anomaly_event_count / self.event_count,
                "risk_score_moving_average": self.risk_total / self.event_count,
                "high_risk_event_count": float(self.high_risk_event_count),
                "distinct_login_country_count": float(len(self.login_countries)),
                "unique_destination_ip_count": float(len(self.destination_ips)),
            },
        }


class PlainWindowEngine:
    def __init__(self, watermark_seconds: int) -> None:
        self.watermark_seconds = watermark_seconds
        self.max_event_epoch = float("-inf")
        self.windows: dict[tuple[str, int], WindowAccumulator] = {}
        self.emitted: list[dict[str, Any]] = []
        self.late_events = 0

    @property
    def watermark(self) -> float:
        return self.max_event_epoch - self.watermark_seconds

    def add(self, payload: dict[str, Any], broker_timestamp_ms: int, emitted_ms: int | None = None) -> None:
        event_epoch = _epoch(payload["timestamp"])
        if self.max_event_epoch != float("-inf") and event_epoch <= self.watermark:
            self.late_events += 1
            return
        self.max_event_epoch = max(self.max_event_epoch, event_epoch)
        for start in window_starts(event_epoch):
            key = (str(payload["source_ip"]), start)
            accumulator = self.windows.setdefault(key, WindowAccumulator(source_ip=key[0], start=start))
            accumulator.add(payload, broker_timestamp_ms)
        self._close(emitted_ms or int(time.time() * 1000))

    def _close(self, emitted_ms: int) -> None:
        closed = [key for key, window in self.windows.items() if window.start + WINDOW_SECONDS <= self.watermark]
        for key in sorted(closed):
            self.emitted.append(self.windows.pop(key).emit(emitted_ms))


@dataclass(frozen=True)
class ConsumerResult:
    consumed: int
    late_events: int
    emitted_windows: int
    elapsed_seconds: float
    achieved_rate: float
    latency_seconds: dict[str, float | int | None]


def consume(
    bootstrap_servers: str,
    group_id: str,
    expected_events: int,
    watermark_seconds: int,
    timeout_seconds: float,
) -> tuple[ConsumerResult, list[dict[str, Any]]]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(sorted(RAW_TOPICS))
    engine = PlainWindowEngine(watermark_seconds)
    started = time.monotonic()
    first_received = None
    last_received = None
    consumed = 0
    try:
        while consumed < expected_events and time.monotonic() - started < timeout_seconds:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError(str(message.error()))
                continue
            payload = json.loads(message.value())
            received = time.monotonic()
            first_received = received if first_received is None else first_received
            last_received = received
            engine.add(payload, int(message.timestamp()[1]))
            consumed += 1
    finally:
        consumer.close()
    elapsed = max((last_received or started) - (first_received or started), 1e-9)
    achieved = (consumed - 1) / elapsed if consumed > 1 else consumed / elapsed
    latencies = [float(row["benchmark_latency_seconds"]) for row in engine.emitted]
    return (
        ConsumerResult(
            consumed=consumed,
            late_events=engine.late_events,
            emitted_windows=len(engine.emitted),
            elapsed_seconds=elapsed,
            achieved_rate=achieved,
            latency_seconds=summarize(latencies),
        ),
        engine.emitted,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--expected-events", type=int, required=True)
    parser.add_argument("--watermark-seconds", type=int, default=120)
    parser.add_argument("--timeout-seconds", type=float, default=600)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result, features = consume(
        args.bootstrap_servers,
        args.group_id,
        args.expected_events,
        args.watermark_seconds,
        args.timeout_seconds,
    )
    args.output.write_text(json.dumps({"result": asdict(result), "features": features}), encoding="utf-8")
    print(json.dumps(asdict(result), sort_keys=True))


if __name__ == "__main__":
    main()
