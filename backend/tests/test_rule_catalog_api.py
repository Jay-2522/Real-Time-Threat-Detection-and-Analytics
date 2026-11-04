"""The rule list must come from the threat engine's own report, never from asking Prometheus."""
from test_dashboard_truth import add, naive_utc, stack  # noqa: F401  (stack is a pytest fixture)


def test_loaded_rules_come_from_the_engines_report_in_the_database_not_from_prometheus(stack):
    client, session, models = stack
    assert client.get("/api/v1/detections/loaded-rules").json() == []  # engine has not reported yet: empty, not an error
    add(session,
        models.DetectionRuleCatalog(rule_id="brute_force_detection", name="Repeated Failed Authentication", severity="high",
                                    enabled=True, tactic="Credential Access", technique="Brute Force", mitre_id="T1110",
                                    loaded_at=naive_utc()),
        models.DetectionRuleCatalog(rule_id="impossible_travel", name="Impossible Travel Login", severity="medium",
                                    enabled=False, loaded_at=naive_utc()))
    # Prometheus is unreachable in these tests: the answer must not depend on it.
    response = client.get("/api/v1/detections/loaded-rules")
    assert response.status_code == 200
    rules = {rule["rule_id"]: rule for rule in response.json()}
    assert rules["brute_force_detection"]["enabled"] is True and rules["brute_force_detection"]["mitre_id"] == "T1110"
    assert rules["impossible_travel"]["enabled"] is False
