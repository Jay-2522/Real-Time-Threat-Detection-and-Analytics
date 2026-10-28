import sys
from pathlib import Path

import pytest


def load_threat_module(module: str):
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    return __import__(module, fromlist=["*"])


def test_ioc_enrichment_detects_tor_ip_and_geo():
    module = load_threat_module("app.enrichment.ioc_enricher")
    findings, country, asn = module.IOCEnricher().enrich({"source_ip": "185.220.101.1", "country": "Russia"})
    assert country == "Russia"
    assert asn == "AS-TOR Local Intelligence"
    assert any(finding.feed_name == "tor-exit-nodes" for finding in findings)


def test_detection_rule_parser_and_execution(tmp_path):
    rules_dir = tmp_path / "rules"
    rules_dir.mkdir()
    (rules_dir / "test.yaml").write_text(
        """
id: test_rule
name: Test Rule
description: test
enabled: true
severity: high
tactic: Credential Access
technique: Brute Force
mitre_id: T1110
conditions:
  field_equals:
    event_type: [login]
    login_success: [false]
  min_risk_score: 50
score: 80
""",
        encoding="utf-8",
    )
    module = load_threat_module("app.detections.rule_engine")
    hits = module.DetectionRuleEngine(str(rules_dir)).evaluate({"event_type": "login", "login_success": False}, 72)
    assert len(hits) == 1
    assert hits[0].mitre_id == "T1110"


def test_unrelated_payload_text_does_not_trigger_structured_rules(tmp_path):
    rules_dir = tmp_path / "rules"
    rules_dir.mkdir()
    (rules_dir / "test.yaml").write_text(
        """
id: structured_rule
name: Structured Rule
description: test
enabled: true
severity: high
tactic: Execution
technique: Command and Scripting Interpreter
mitre_id: T1059
conditions:
  field_contains:
    process_name: [powershell]
  min_risk_score: 1
score: 80
""",
        encoding="utf-8",
    )
    module = load_threat_module("app.detections.rule_engine")
    engine = module.DetectionRuleEngine(str(rules_dir))

    assert engine.evaluate({"processed_at": "now", "feature_name": "powershell_login_rate"}, 99) == []
    assert len(engine.evaluate({"process_name": "PowerShell.exe"}, 99)) == 1


def test_each_repository_rule_has_an_honest_positive_and_negative_case():
    module = load_threat_module("app.detections.rule_engine")
    rules_path = Path(__file__).resolve().parents[2] / "detections" / "rules"
    engine = module.DetectionRuleEngine(str(rules_path))

    dns_positive = {"event_type": "network", "port": 53, "source_ip": "10.0.0.1", "destination_ip": "8.8.8.8"}
    dns_negative = {"event_type": "network", "port": 443, "processed_at": "login process"}
    assert "high_risk_dns_activity" in {hit.rule_id for hit in engine.evaluate(dns_positive, 70)}
    assert "high_risk_dns_activity" not in {hit.rule_id for hit in engine.evaluate(dns_negative, 99)}

    brute_engine = module.DetectionRuleEngine(str(rules_path))
    for second in range(4):
        hits = brute_engine.evaluate(
            {"event_type": "login", "login_success": False, "source_ip": "10.0.0.5", "timestamp": f"2026-01-01T00:00:0{second}Z"},
            10,
        )
        assert "brute_force_detection" not in {hit.rule_id for hit in hits}
    hits = brute_engine.evaluate(
        {"event_type": "login", "login_success": False, "source_ip": "10.0.0.5", "timestamp": "2026-01-01T00:00:04Z"},
        10,
    )
    assert "brute_force_detection" in {hit.rule_id for hit in hits}
    success_hits = brute_engine.evaluate(
        {"event_type": "login", "login_success": True, "source_ip": "10.0.0.5", "timestamp": "2026-01-01T00:00:05Z"},
        99,
    )
    assert "brute_force_detection" not in {hit.rule_id for hit in success_hits}

    geo_positive = {"event_type": "login", "login_success": True, "country": "Russia"}
    geo_negative = {"event_type": "login", "login_success": True, "country": "US"}
    assert "high_risk_geo_login" in {hit.rule_id for hit in engine.evaluate(geo_positive, 60)}
    assert "high_risk_geo_login" not in {hit.rule_id for hit in engine.evaluate(geo_negative, 99)}

    lateral_positive = {"event_type": "network", "source_ip": "10.0.0.1", "destination_ip": "192.168.0.2", "port": 22}
    lateral_negative = {"event_type": "network", "source_ip": "10.0.0.1", "destination_ip": "8.8.8.8", "port": 22}
    assert "lateral_movement" in {hit.rule_id for hit in engine.evaluate(lateral_positive, 10)}
    assert "lateral_movement" not in {hit.rule_id for hit in engine.evaluate(lateral_negative, 99)}

    loaded = {rule["id"] for rule in engine._rules}
    assert "impossible_travel" not in loaded
    assert "suspicious_privilege_escalation" not in loaded
    assert "suspicious_process_chain" not in loaded
    for candidate, benign in (
        ({"event_type": "anomaly", "anomaly_type": "impossible_travel"}, {"event_type": "login"}),
        ({"event_type": "anomaly", "anomaly_type": "privilege_escalation"}, {"event_type": "api"}),
        ({"process_name": "PowerShell.exe", "parent_process": "cmd.exe"}, {"processed_at": "PowerShell.exe"}),
    ):
        disabled_ids = {"impossible_travel", "suspicious_privilege_escalation", "suspicious_process_chain"}
        assert not ({hit.rule_id for hit in engine.evaluate(candidate, 100)} & disabled_ids)
        assert not ({hit.rule_id for hit in engine.evaluate(benign, 100)} & disabled_ids)


@pytest.mark.parametrize(
    ("source_ip", "destination_ip"),
    [
        ("203.0.113.42", "198.51.100.10"),
        ("127.0.0.1", "127.0.0.2"),
        ("169.254.1.10", "169.254.1.11"),
        ("8.8.8.8", "1.1.1.1"),
    ],
    ids=["documentation", "loopback", "link-local", "public"],
)
def test_lateral_movement_requires_rfc1918_addresses(source_ip, destination_ip):
    module = load_threat_module("app.detections.rule_engine")
    rules_path = Path(__file__).resolve().parents[2] / "detections" / "rules"
    engine = module.DetectionRuleEngine(str(rules_path))

    payload = {
        "event_type": "network",
        "source_ip": source_ip,
        "destination_ip": destination_ip,
        "port": 22,
    }

    assert "lateral_movement" not in {hit.rule_id for hit in engine.evaluate(payload, 99)}


def test_threat_scoring_escalates_with_iocs_and_rules():
    schemas = load_threat_module("app.schemas.threat")
    scorer_module = load_threat_module("app.scoring.threat_scorer")
    score, confidence, severity = scorer_module.ThreatScorer().score(
        70,
        [schemas.IOCFinding(value="1.1.1.1", ioc_type="ip", feed_name="test", confidence=0.9, severity="high", description="test")],
        [schemas.RuleHit(rule_id="r1", rule_name="R1", severity="high", tactic="Credential Access", technique="Brute Force", mitre_id="T1110", score=84)],
        82,
    )
    assert score >= 70
    assert confidence > 0.5
    assert severity in {"medium", "high", "critical"}
