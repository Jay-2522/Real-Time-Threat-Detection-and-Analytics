"""Deterministic, schema-valid Kafka load generator for benchmark runs."""

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from ipaddress import IPv4Address
from typing import Any, Callable, Protocol
from uuid import UUID, uuid5

from confluent_kafka import Producer


TOPICS = {
    "login": "telemetry.login.events",
    "api": "telemetry.api.events",
    "network": "telemetry.network.events",
}
BENCHMARK_NAMESPACE = UUID("82519466-871b-4e32-8f78-06f50c41f0bf")


class KafkaProducer(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True)
class LoadConfig:
    rate: float
    duration: float
    source_ips: int
    out_of_order_fraction: float
    seed: int = 20261026
    run_id: str = "benchmark"
    max_lateness_seconds: float = 120.0

    def __post_init__(self) -> None:
        if self.rate <= 0 or self.duration <= 0:
            raise ValueError("rate and duration must be positive")
        if not 1 <= self.source_ips <= 65_534:
            raise ValueError("source_ips must be between 1 and 65534")
        if not 0.0 <= self.out_of_order_fraction <= 1.0:
            raise ValueError("out_of_order_fraction must be between 0 and 1")
        if self.max_lateness_seconds <= 0:
            raise ValueError("max_lateness_seconds must be positive")

    @property
    def event_count(self) -> int:
        return max(1, round(self.rate * self.duration))


@dataclass(frozen=True)
class LoadResult:
    run_id: str
    requested_rate: float
    requested_duration_seconds: float
    attempted: int
    delivered: int
    delivery_errors: int
    elapsed_seconds: float
    achieved_rate: float
    first_event_id: str
    last_event_id: str


def event_id_for(run_id: str, sequence: int) -> str:
    """Return a stable UUID accepted by the existing telemetry schema."""

    return str(uuid5(BENCHMARK_NAMESPACE, f"{run_id}:{sequence}"))


class EventFactory:
    def __init__(self, config: LoadConfig) -> None:
        self.config = config
        self.random = random.Random(config.seed)
        base = int(IPv4Address("198.18.0.1"))
        self.sources = [str(IPv4Address(base + index)) for index in range(config.source_ips)]

    def build(self, sequence: int, emitted_at: datetime) -> tuple[str, dict[str, Any]]:
        event_type = ("login", "api", "network")[sequence % 3]
        source_ip = self.sources[sequence % len(self.sources)]
        timestamp = emitted_at
        if self.random.random() < self.config.out_of_order_fraction:
            timestamp -= timedelta(seconds=self.random.uniform(0.001, self.config.max_lateness_seconds))

        common: dict[str, Any] = {
            "event_id": event_id_for(self.config.run_id, sequence),
            "timestamp": timestamp.isoformat(),
            "risk_score": round(self.random.uniform(10.0, 45.0), 2),
            "event_type": event_type,
            "source_ip": source_ip,
        }
        if event_type == "login":
            common.update(
                user_id=f"bench-user-{sequence % 100}",
                username=f"bench-user-{sequence % 100}",
                country="US",
                device_type="benchmark-client",
                login_success=True,
                failure_reason=None,
            )
        elif event_type == "api":
            common.update(
                user_id=f"bench-user-{sequence % 100}",
                endpoint="/api/v1/benchmark",
                method="GET",
                status_code=200,
                response_time_ms=25 + sequence % 200,
                request_size=512 + sequence % 4096,
            )
        else:
            common.update(
                destination_ip="203.0.113.10",
                protocol="TCP",
                bytes_sent=1024 + sequence % 8192,
                bytes_received=2048 + sequence % 16384,
                port=443,
                packet_count=10 + sequence % 100,
            )
        return TOPICS[event_type], common


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "streaming-analytics-benchmark-loadgen",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "delivery.timeout.ms": 30000,
        }
    )


def run_load(
    config: LoadConfig,
    producer: KafkaProducer,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> LoadResult:
    factory = EventFactory(config)
    delivered = 0
    delivery_errors = 0

    def delivery_callback(error: object | None, _message: object) -> None:
        nonlocal delivered, delivery_errors
        if error is None:
            delivered += 1
        else:
            delivery_errors += 1

    started = monotonic()
    for sequence in range(config.event_count):
        deadline = started + sequence / config.rate
        remaining = deadline - monotonic()
        if remaining > 0:
            sleep(remaining)
        topic, event = factory.build(sequence, now())
        payload = json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")
        while True:
            try:
                producer.produce(
                    topic,
                    key=event["event_id"].encode("utf-8"),
                    value=payload,
                    on_delivery=delivery_callback,
                )
                break
            except BufferError:
                producer.poll(0.01)
        producer.poll(0)

    undelivered = producer.flush(60.0)
    elapsed = max(monotonic() - started, 1e-9)
    if undelivered:
        delivery_errors += undelivered
    return LoadResult(
        run_id=config.run_id,
        requested_rate=config.rate,
        requested_duration_seconds=config.duration,
        attempted=config.event_count,
        delivered=delivered,
        delivery_errors=delivery_errors,
        elapsed_seconds=elapsed,
        achieved_rate=delivered / elapsed,
        first_event_id=event_id_for(config.run_id, 0),
        last_event_id=event_id_for(config.run_id, config.event_count - 1),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, required=True)
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--source-ips", type=int, default=100)
    parser.add_argument("--out-of-order-fraction", type=float, default=0.0)
    parser.add_argument("--max-lateness-seconds", type=float, default=120.0)
    parser.add_argument("--seed", type=int, default=20261026)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = LoadConfig(
        rate=args.rate,
        duration=args.duration,
        source_ips=args.source_ips,
        out_of_order_fraction=args.out_of_order_fraction,
        max_lateness_seconds=args.max_lateness_seconds,
        seed=args.seed,
        run_id=args.run_id,
    )
    result = run_load(config, make_producer(args.bootstrap_servers))
    print(json.dumps(asdict(result), sort_keys=True))


if __name__ == "__main__":
    main()
