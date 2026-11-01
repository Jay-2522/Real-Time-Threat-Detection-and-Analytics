"""The shared traffic model feeds the producer and ML training. These tests keep its labels honest:
benign traffic must never look like an attack to a detection rule, and every attack scenario must be
something the rules or the model can actually detect."""
import ipaddress
import sys
from collections import defaultdict
from pathlib import Path

from pipeline_runtime.traffic_model import HIGH_RISK_COUNTRIES, SCENARIOS, simulate

ROOT = Path(__file__).resolve().parents[2]
ADMIN_PORTS = {22, 445, 3389, 5985, 5986}
HOUR = 3600


def _private(address):
    return ipaddress.ip_address(address).is_private


def _breaches_a_detection_rule(event):
    """Conditions of the enabled detections/rules/*.yaml files, applied to one event."""
    f = event.fields
    if f["event_type"] == "login" and f.get("country") in HIGH_RISK_COUNTRIES and f["risk_score"] >= 45:
        return "high_risk_geo_login"
    if f["event_type"] == "network":
        if f["port"] == 53 and f["risk_score"] >= 58:
            return "high_risk_dns_activity"
        if f["port"] in ADMIN_PORTS and _private(f["source_ip"]) and _private(f["destination_ip"]):
            return "lateral_movement"
        if f["bytes_sent"] + f["bytes_received"] >= 8_000_000:
            return "large_network_transfer"
    return None


def test_same_seed_produces_the_same_traffic():
    first, second = simulate(5, 120), simulate(5, 120)
    assert [(e.timestamp, e.fields) for e in first] == [(e.timestamp, e.fields) for e in second]
    assert [e.fields for e in simulate(6, 120)] != [e.fields for e in first]


def test_benign_traffic_never_matches_a_detection_rule_or_brute_force_threshold():
    events = simulate(11, 2 * HOUR, anomaly_rate=0.0)
    assert not any(e.scenario for e in events)
    assert [_breaches_a_detection_rule(e) for e in events if _breaches_a_detection_rule(e)] == []
    failures = defaultdict(list)
    for e in events:
        if e.fields["event_type"] == "login" and e.fields["login_success"] is False:
            failures[e.fields["source_ip"]].append(e.timestamp.timestamp())
    for stamps in failures.values():  # brute_force_detection fires at 5 failures inside 300 s
        assert max(sum(1 for later in stamps[i:] if later - start <= 300) for i, start in enumerate(stamps)) < 5


def test_every_attack_scenario_satisfies_its_detection_condition():
    events = simulate(12, 4 * HOUR, anomaly_rate=0.3)
    by_scenario = defaultdict(lambda: defaultdict(list))
    for e in events:
        if e.scenario:
            by_scenario[e.scenario][e.scenario_id].append(e.fields)
    assert set(by_scenario) == set(SCENARIOS)
    for instance in by_scenario["brute_force"].values():
        assert sum(1 for f in instance if f["event_type"] == "login" and f["login_success"] is False) >= 5
        assert len({f["source_ip"] for f in instance}) == 1
    for instance in by_scenario["data_exfiltration"].values():
        assert any(f["event_type"] == "network" and f["bytes_sent"] + f["bytes_received"] >= 8_000_000 for f in instance)
    for instance in by_scenario["dns_beaconing"].values():
        assert any(f["port"] == 53 and f["risk_score"] >= 58 for f in instance)
    for instance in by_scenario["lateral_movement"].values():
        assert any(f["port"] in ADMIN_PORTS and _private(f["destination_ip"]) for f in instance)
    for instance in by_scenario["geo_login"].values():
        assert any(f["event_type"] == "login" and f["country"] in HIGH_RISK_COUNTRIES and f["risk_score"] >= 45 for f in instance)


def test_attack_share_follows_the_configured_rate():
    events = simulate(13, 4 * HOUR, anomaly_rate=0.05)
    share = sum(1 for e in events if e.scenario) / len(events)
    assert 0.035 < share < 0.065


def test_producer_generator_emits_schema_valid_events_of_every_type():
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(ROOT / "kafka-producers"))
    from app.generators.telemetry_generator import SyntheticTelemetryGenerator
    from app.schemas.events import AnomalyEvent, ApiEvent, LoginEvent, NetworkEvent

    generator = SyntheticTelemetryGenerator(anomaly_rate=0.3, seed=3)
    kinds = {type(generator.next_event()) for _ in range(4000)}
    assert kinds == {LoginEvent, ApiEvent, NetworkEvent, AnomalyEvent}
