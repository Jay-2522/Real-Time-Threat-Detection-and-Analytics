"""Contract tests for the Cowrie JSON-to-telemetry adapter."""

from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]


class _Metric:
    def labels(self, *_args):
        return self

    def inc(self):
        return None


class CapturingProducer:
    def __init__(self):
        self.records: list[dict] = []

    def produce(self, **kwargs):
        self.records.append(kwargs)
        kwargs["callback"](None, SimpleNamespace())

    def flush(self, _timeout):
        return 0


@pytest.fixture(scope="module")
def cowrie_module():
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(ROOT / "kafka-producers"))
    metrics = ModuleType("app.utils.metrics")
    metrics.EVENTS_PUBLISHED_TOTAL = _Metric()
    metrics.PRODUCER_ERRORS_TOTAL = _Metric()
    sys.modules["app.utils.metrics"] = metrics
    return importlib.import_module("app.adapters.cowrie")


def test_valid_login_and_network_mapping(cowrie_module):
    login = cowrie_module.map_cowrie_event(
        {
            "eventid": "cowrie.login.success",
            "timestamp": "2026-01-01T00:00:00Z",
            "session": "synthetic-session-login",
            "src_ip": "203.0.113.10",
            "username": "root",
            "password": "must-not-be-mapped",
        },
        source_position=1,
    )
    network = cowrie_module.map_cowrie_event(
        {
            "eventid": "cowrie.session.connect",
            "timestamp": "2026-01-01T00:00:01Z",
            "session": "synthetic-session-network",
            "src_ip": "198.51.100.20",
            "dst_ip": "203.0.113.20",
            "dst_port": 22,
            "protocol": "ssh",
        },
        source_position=2,
    )

    assert login.model_dump(exclude={"event_id"}) == {
        "timestamp": datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
        "original_timestamp": None,
        "risk_score": 0.0,
        "event_type": "login",
        "user_id": "root",
        "username": "root",
        "source_ip": "203.0.113.10",
        "country": None,
        "device_type": None,
        "login_success": True,
        "failure_reason": None,
    }
    assert network.source_ip == "198.51.100.20"
    assert network.destination_ip == "203.0.113.20"
    assert network.port == 22
    assert network.protocol == "TCP"


def test_password_never_leaves_adapter(cowrie_module):
    producer = CapturingProducer()
    adapter = cowrie_module.CowrieEventAdapter(producer)
    secret = "synthetic-password-never-forward"

    assert adapter.handle_line(
        json.dumps(
            {
                "eventid": "cowrie.login.failed",
                "timestamp": "2026-01-01T00:00:00Z",
                "session": "synthetic-session-secret",
                "src_ip": "203.0.113.11",
                "username": "admin",
                "password": secret,
            }
        ),
        source_position=1,
    )

    wire_value = producer.records[0]["value"]
    assert secret.encode() not in wire_value
    assert b"password" not in wire_value
    assert json.loads(wire_value)["username"] == "admin"


def test_malformed_line_is_safely_sent_to_dlq(cowrie_module):
    producer = CapturingProducer()
    adapter = cowrie_module.CowrieEventAdapter(producer)
    secret = "synthetic-malformed-password"

    assert not adapter.handle_line(
        '{"eventid":"cowrie.login.failed","password":"' + secret + '",',
        source_position=7,
    )

    assert producer.records[0]["topic"] == "telemetry.deadletter.events"
    wire_value = producer.records[0]["value"]
    assert secret.encode() not in wire_value
    dlq = json.loads(wire_value)
    assert dlq["source_topic"] == "cowrie.json"
    assert dlq["position"] == {"line": 7}
    assert dlq["raw_hex"] is None


def test_time_shift_preserves_relative_gaps(cowrie_module, tmp_path):
    source = tmp_path / "synthetic-cowrie.json"
    events = [
        {
            "eventid": "cowrie.login.failed",
            "timestamp": "2020-01-01T00:00:00Z",
            "session": "synthetic-replay-1",
            "src_ip": "203.0.113.12",
            "username": "root",
            "password": "synthetic-one",
        },
        {
            "eventid": "cowrie.login.failed",
            "timestamp": "2020-01-01T00:00:12.500000Z",
            "session": "synthetic-replay-2",
            "src_ip": "203.0.113.12",
            "username": "root",
            "password": "synthetic-two",
        },
    ]
    source.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    producer = CapturingProducer()
    ingestor = cowrie_module.CowrieLogIngestor(
        SimpleNamespace(kafka_bootstrap_servers="unused", producer_client_id="test")
    )
    ingestor._create_producer = lambda: producer

    assert ingestor.replay(source, time_shift=True) == 2

    published = [json.loads(record["value"]) for record in producer.records]
    shifted = [datetime.fromisoformat(row["timestamp"]) for row in published]
    originals = [datetime.fromisoformat(row["original_timestamp"]) for row in published]
    assert (shifted[1] - shifted[0]).total_seconds() == 12.5
    assert originals == [
        datetime.fromisoformat(events[0]["timestamp"]),
        datetime.fromisoformat(events[1]["timestamp"]),
    ]
    assert all("password" not in row for row in published)


def test_synthetic_fixture_separates_documentation_burst_from_rfc1918_grouping(cowrie_module):
    fixture = ROOT / "data" / "samples" / "cowrie-synthetic.json"
    rows = [json.loads(line) for line in fixture.read_text(encoding="utf-8").splitlines()]

    documentation_burst = [
        row
        for row in rows
        if row.get("eventid") == "cowrie.login.failed" and row.get("src_ip") == "203.0.113.42"
    ]
    grouping_rows = [row for row in rows if row.get("sensor") == "synthetic-rfc1918-grouping-fixture"]
    mapped = [cowrie_module.map_cowrie_event(row, index) for index, row in enumerate(grouping_rows, start=1)]

    assert len(documentation_burst) == 20
    assert len(grouping_rows) == 6
    assert all(str(row["session"]).startswith("synthetic-rfc1918-") for row in grouping_rows)
    assert sum(event.event_type == "login" and event.login_success is False for event in mapped) == 5
    network = next(event for event in mapped if event.event_type == "network")
    assert network.source_ip == "10.20.0.15"
    assert network.destination_ip == "10.20.0.20"
    assert network.port == 22
