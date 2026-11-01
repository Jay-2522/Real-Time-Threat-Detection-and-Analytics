"""Guards the trained model's quality on held-out simulated traffic and the honeypot scoring path.
The data is synthetic, so these numbers bound regressions; they are not real-world accuracy."""
import json

import pytest
from test_ml_contracts import load_ml_module


@pytest.fixture(scope="module")
def trained(tmp_path_factory):
    path = tmp_path_factory.mktemp("model")
    settings = load_ml_module("app.config.settings").MLSettings(ML_ARTIFACT_DIR=path, ML_TRAINING_SAMPLE_SIZE=6000)
    load_ml_module("app.training.train_model").train_and_save(settings)
    store = load_ml_module("app.models.artifact_store")
    predictor = load_ml_module("app.inference.predictor").AnomalyPredictor(settings, store.load_artifacts(path))
    return path, predictor


def test_held_out_evaluation_keeps_false_alarms_low_and_behavioural_attacks_detected(trained):
    evaluation = json.loads((trained[0] / "training_metrics.json").read_text())["evaluation"]
    assert evaluation["benign_false_positive_rate"] <= 0.01
    detected = {name: stats["detection_rate"] for name, stats in evaluation["per_scenario"].items()}
    # Volume and behaviour attacks belong to the model; indicator attacks (DNS port, watchlist country) to the YAML rules.
    # Exfiltration is also covered by the 8 MB large-transfer alert rule, so the model alone may miss some.
    assert detected["brute_force"] >= 0.9 and detected["api_abuse"] >= 0.9 and detected["data_exfiltration"] >= 0.6


def honeypot_window(**overrides):
    """A Cowrie brute-force window: all logins failed, no country, neutral risk score 0 (see cowrie.py)."""
    payload = {
        "feature_contract_version": "2.0", "entity_type": "source_ip", "entity_id": "203.0.113.42",
        "event_type": "feature_window", "invalid_event_count": 0,
        "window_start": "2026-01-01T12:00:00Z", "window_end": "2026-01-01T12:01:00Z",
        "first_event_at": "2026-01-01T12:00:03Z", "last_event_at": "2026-01-01T12:00:36Z",
        "calculated_at": "2026-01-01T12:02:00Z", "source_event_ids": [f"id-{i}" for i in range(12)],
        "features": dict(event_count=12, login_event_count=12, api_event_count=0, network_event_count=0,
                         anomaly_event_count=0, failed_login_rate=1.0, failed_auth_count=12, requests_per_minute=0,
                         avg_api_response_time=None, endpoint_error_rate=None, api_5xx_rate=None,
                         avg_network_bytes=None, high_risk_network_event_count=0, anomaly_rate=0,
                         risk_score_moving_average=0, high_risk_event_count=0,
                         distinct_login_country_count=0, unique_destination_ip_count=0),
    }
    payload["features"].update(overrides)
    return payload


def test_honeypot_brute_force_window_is_scoreable_and_flagged(trained):
    mapper = load_ml_module("app.features.feature_mapper").FeatureMapper()
    vector = mapper.to_feature_vector(honeypot_window())  # previously rejected: no country was reported
    result = trained[1].predict(vector, None, "feature_window", "203.0.113.42")
    assert result.is_anomaly is True


def test_honeypot_connect_window_without_byte_counts_is_scoreable():
    mapper = load_ml_module("app.features.feature_mapper").FeatureMapper()
    window = honeypot_window(event_count=2, login_event_count=1, network_event_count=1, failed_login_rate=1.0,
                             failed_auth_count=1, avg_network_bytes=0.0, unique_destination_ip_count=1)
    window["source_event_ids"] = ["a", "b"]
    assert mapper.to_feature_vector(window)["avg_network_bytes"] == 0.0


def test_stale_model_trained_on_an_older_traffic_profile_is_rejected(trained):
    store = load_ml_module("app.models.artifact_store")
    assert store.artifacts_current(trained[0])
    metadata = json.loads((trained[0] / "feature_metadata.json").read_text())
    metadata["traffic_profile_version"] = "0"
    stale = trained[0] / "stale"
    stale.mkdir()
    (stale / "feature_metadata.json").write_text(json.dumps(metadata))
    assert not store.artifacts_current(stale)
    assert not store.artifacts_current(trained[0] / "missing")
