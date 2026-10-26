import json
import importlib.util
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import TypeAdapter

from benchmarks.loadgen import EventFactory, LoadConfig, event_id_for, run_load


class FakeProducer:
    def __init__(self) -> None:
        self.records = []
        self.callbacks = []

    def produce(self, topic, **kwargs):
        self.records.append((topic, kwargs))
        self.callbacks.append(kwargs["on_delivery"])

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        for callback in self.callbacks:
            callback(None, object())
        return 0


def test_factory_is_seeded_schema_valid_and_uses_requested_sources():
    config = LoadConfig(rate=10, duration=1, source_ips=2, out_of_order_fraction=1, seed=7, run_id="same")
    instant = datetime(2026, 10, 26, tzinfo=UTC)
    first = EventFactory(config)
    second = EventFactory(config)
    events = [first.build(index, instant) for index in range(6)]
    assert events == [second.build(index, instant) for index in range(6)]
    assert {event[1]["source_ip"] for event in events} == {"198.18.0.1", "198.18.0.2"}
    assert {event[1]["event_type"] for event in events} == {"login", "api", "network"}
    assert all(datetime.fromisoformat(event[1]["timestamp"]) < instant for event in events)
    assert events[0][1]["event_id"] == event_id_for("same", 0)

    schema_path = Path(__file__).resolve().parents[2] / "kafka-producers/app/schemas/events.py"
    spec = importlib.util.spec_from_file_location("benchmark_product_event_schemas", schema_path)
    assert spec and spec.loader
    schemas = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schemas)
    adapter = TypeAdapter(schemas.TelemetryEvent)
    assert [adapter.validate_python(payload).event_type for _, payload in events] == [
        "login",
        "api",
        "network",
        "login",
        "api",
        "network",
    ]


def test_run_reports_delivered_achieved_rate_and_valid_json_payloads():
    producer = FakeProducer()
    ticks = iter([0.0, 0.0, 0.5, 1.0])
    result = run_load(
        LoadConfig(rate=2, duration=1, source_ips=1, out_of_order_fraction=0, run_id="unit"),
        producer,
        monotonic=lambda: next(ticks),
        sleep=lambda _seconds: None,
        now=lambda: datetime(2026, 10, 26, tzinfo=UTC),
    )
    assert result.attempted == result.delivered == 2
    assert result.delivery_errors == 0
    assert result.elapsed_seconds == 1
    assert result.achieved_rate == 2
    assert [json.loads(record[1]["value"])["event_id"] for record in producer.records] == [
        event_id_for("unit", 0),
        event_id_for("unit", 1),
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"rate": 0},
        {"duration": 0},
        {"source_ips": 0},
        {"out_of_order_fraction": 1.01},
    ],
)
def test_invalid_load_configuration_is_rejected(kwargs):
    values = dict(rate=1, duration=1, source_ips=1, out_of_order_fraction=0)
    values.update(kwargs)
    with pytest.raises(ValueError):
        LoadConfig(**values)
