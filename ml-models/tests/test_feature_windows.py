from datetime import datetime
import copy
import json
import os
from pathlib import Path
import pytest
from test_ml_contracts import load_ml_module


def payload():
    return {
        "feature_contract_version": "2.0", "entity_type": "source_ip", "entity_id": "192.0.2.1",
        "event_type": "feature_window", "invalid_event_count": 0,
        "window_start": "2026-01-01T12:00:00Z", "window_end": "2026-01-01T12:01:00Z",
        "first_event_at": "2026-01-01T12:00:10Z", "last_event_at": "2026-01-01T12:00:10Z",
        "calculated_at": "2026-01-01T12:04:00Z", "source_event_ids": [f"id-{i}" for i in range(12)],
        "features": dict(event_count=12, login_event_count=4, api_event_count=4, network_event_count=3,
                         anomaly_event_count=1, failed_login_rate=.5, failed_auth_count=2, requests_per_minute=4,
                         avg_api_response_time=100, endpoint_error_rate=.75, api_5xx_rate=.5, avg_network_bytes=150,
                         high_risk_network_event_count=3, anomaly_rate=1/12, risk_score_moving_average=80,
                         high_risk_event_count=12, distinct_login_country_count=1, unique_destination_ip_count=2),
    }


def test_rates_order_and_no_count_aliases():
    module = load_ml_module("app.features.feature_mapper")
    f = module.FeatureMapper().to_feature_vector(payload())
    assert list(f) == module.FEATURE_COLUMNS
    assert f["failed_login_rate"] == .5 and f["failed_auth_count"] == 2
    assert f["endpoint_error_rate"] == .75 and f["api_5xx_rate"] == .5
    assert f["unique_destination_ip_count"] == 2
    assert f["risk_score_moving_average"] == 80


@pytest.mark.parametrize("value", [None, "bad", "0.5", True, float("nan"), float("inf"), -1, 1.1])
def test_invalid_numerical_values_rejected(value):
    mapper = load_ml_module("app.features.feature_mapper").FeatureMapper()
    p = payload(); p["features"]["failed_login_rate"] = value
    with pytest.raises(ValueError): mapper.to_feature_vector(p)


@pytest.mark.parametrize("change", ["missing", "extra", "version", "scope", "duration", "naive", "provenance", "invalid", "count", "ratio", "5xx"])
def test_incomplete_or_inconsistent_windows_rejected(change):
    mapper = load_ml_module("app.features.feature_mapper").FeatureMapper()
    p = payload()
    if change == "missing": del p["features"]["failed_login_rate"]
    if change == "extra": p["features"]["session_activity_score"] = 0
    if change == "version": p["feature_contract_version"] = "1.0"
    if change == "scope": p["entity_type"] = "user"
    if change == "duration": p["window_end"] = "2026-01-01T12:02:00Z"
    if change == "naive": p["window_start"] = "2026-01-01T12:00:00"
    if change == "provenance": p["source_event_ids"].pop()
    if change == "invalid": p["invalid_event_count"] = 1
    if change == "count": p["features"]["failed_auth_count"] = 12
    if change == "ratio": p["features"]["failed_login_rate"] = 1
    if change == "5xx": p["features"]["api_5xx_rate"] = 1
    with pytest.raises(ValueError): mapper.to_feature_vector(p)


def test_structural_missingness_requires_zero_population():
    module = load_ml_module("app.features.feature_mapper")
    p = payload(); f = p["features"]
    f.update(api_event_count=0, event_count=8, requests_per_minute=0, avg_api_response_time=None,
             endpoint_error_rate=None, api_5xx_rate=None, anomaly_rate=1/8, high_risk_event_count=8)
    p["source_event_ids"] = p["source_event_ids"][:8]
    mapped = module.FeatureMapper().to_feature_vector(p)
    assert mapped["api_5xx_rate"] == 0 and mapped["api_event_count"] == 0
    f["api_5xx_rate"] = 0
    with pytest.raises(ValueError): module.FeatureMapper().to_feature_vector(p)


def train(tmp_path):
    settings = load_ml_module("app.config.settings").MLSettings(ML_ARTIFACT_DIR=tmp_path, ML_TRAINING_SAMPLE_SIZE=1000)
    load_ml_module("app.training.train_model").train_and_save(settings)
    return settings


def test_training_artifact_roundtrip_and_metadata_rejection(tmp_path):
    settings = train(tmp_path)
    store = load_ml_module("app.models.artifact_store")
    artifacts = store.load_artifacts(tmp_path)
    mapper = load_ml_module("app.features.feature_mapper").FeatureMapper()
    predictor = load_ml_module("app.inference.predictor").AnomalyPredictor(settings, artifacts)
    result = predictor.predict(mapper.to_feature_vector(payload()), None, "feature_window", "192.0.2.1")
    assert list(result.features_used) == store.FEATURE_COLUMNS
    for key, value in [("feature_contract_version", "1.0"), ("feature_columns", []), ("preprocessing", "none")]:
        changed = copy.deepcopy(artifacts.metadata); changed[key] = value
        (tmp_path / "feature_metadata.json").write_text(json.dumps(changed))
        with pytest.raises(ValueError): store.load_artifacts(tmp_path)
    artifacts.scaler.feature_names_in_ = artifacts.scaler.feature_names_in_[::-1]
    with pytest.raises(ValueError): store.validate_artifacts(artifacts)


def test_model_input_count_must_match_feature_contract(tmp_path):
    settings = train(tmp_path)
    store = load_ml_module("app.models.artifact_store")
    artifacts = store.load_artifacts(settings.artifact_dir)
    assert artifacts.model.n_features_in_ == len(store.FEATURE_COLUMNS) == 18
    artifacts.model.n_features_in_ -= 1
    with pytest.raises(ValueError, match="feature dimensions"):
        store.validate_artifacts(artifacts)


def test_legacy_real_artifact_is_rejected():
    store = load_ml_module("app.models.artifact_store")
    with pytest.raises(ValueError, match="Incompatible"):
        store.load_artifacts(Path(__file__).parents[1] / "artifacts")


def test_consumer_preserves_metadata_and_routes_invalid_window_to_dlq(tmp_path):
    settings = train(tmp_path)
    store = load_ml_module("app.models.artifact_store")
    artifacts = store.load_artifacts(tmp_path)
    predictor = load_ml_module("app.inference.predictor").AnomalyPredictor(settings, artifacts)
    consumer = load_ml_module("app.consumers.inference_consumer").MLInferenceConsumer(settings, predictor)
    published, dead = [], []
    consumer._publish_prediction = published.append
    consumer._publish_deadletter = lambda *args: dead.append(args)
    p = payload()
    consumer._handle_message("analytics.feature.engineering", json.dumps(p).encode())
    assert len(published) == 1 and not dead
    result = published[0].model_dump(mode="json")
    for key in ["source_event_ids", "entity_id", "entity_type", "window_start", "window_end", "feature_contract_version"]:
        if key in {"window_start", "window_end"}:
            assert datetime.fromisoformat(result[key]) == datetime.fromisoformat(p[key].replace("Z", "+00:00"))
        else:
            assert result[key] == p[key]
    assert result["source_event_id"] is None
    p["features"]["failed_login_rate"] = "bad"
    consumer._handle_message("analytics.feature.engineering", json.dumps(p).encode())
    assert len(dead) == 1 and len(published) == 1


def test_actual_spark_output_maps_without_fabrication():
    path = os.environ.get("SPARK_CONTRACT_OUTPUT")
    if not path:
        pytest.skip("Run actual Spark regression suite with SPARK_CONTRACT_OUTPUT first")
    module = load_ml_module("app.features.feature_mapper")
    records = json.loads(Path(path).read_text())
    assert records
    valid = invalid = 0
    for p in records:
        if p["invalid_event_count"]:
            with pytest.raises(ValueError): module.FeatureMapper().to_feature_vector(p)
            invalid += 1
        else:
            vector = module.FeatureMapper().to_feature_vector(p)
            for name, value in p["features"].items():
                assert vector[name] == (0.0 if value is None else value)
            valid += 1
    assert valid == 8 and invalid == 8


def test_legacy_partial_record_is_rejected():
    mapper = load_ml_module("app.features.feature_mapper").FeatureMapper()
    with pytest.raises(ValueError, match="contract version"):
        mapper.to_feature_vector({"event_type": "login", "login_success": False, "risk_score": 81})
