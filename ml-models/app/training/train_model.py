from __future__ import annotations

import logging
from collections import defaultdict
from datetime import UTC, datetime

import numpy as np
import pandas as pd
from pipeline_runtime.traffic_model import PROFILE_VERSION, SCENARIOS, SimWindow, build_windows, simulate
from sklearn.ensemble import IsolationForest
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler

from app.config.settings import MLSettings, get_settings
from app.features.feature_definitions import FEATURE_COLUMNS, FEATURE_CONTRACT_VERSION, PREPROCESSING
from app.features.feature_mapper import validate_features
from app.models.artifact_store import save_artifacts
from app.utils.logging import configure_logging

logger = logging.getLogger(__name__)

# Three independent simulations of the same traffic model: fit, threshold calibration, held-out evaluation.
TRAIN_SEED, CALIBRATION_SEED, EVALUATION_SEED = 1001, 2002, 3003
SIMULATED_ANOMALY_RATE = 0.05  # fraction of events belonging to attack scenarios, as the producer's default
_WINDOWS_PER_SECOND = 1.2  # conservative estimate used to size each simulation


def simulate_windows(seed: int, count: int) -> list[SimWindow]:
    seconds = max(900.0, count / _WINDOWS_PER_SECOND)
    while True:
        windows = build_windows(simulate(seed, seconds, SIMULATED_ANOMALY_RATE))
        if len(windows) >= count:
            return windows[:count]
        seconds *= 1.5


def to_matrix(windows: list[SimWindow]) -> pd.DataFrame:
    """Validate every simulated window against the production contract before using it."""
    rows = [validate_features(window.features) for window in windows]
    return pd.DataFrame(rows, columns=FEATURE_COLUMNS)


def evaluate(model: IsolationForest, scaler: StandardScaler, windows: list[SimWindow]) -> dict:
    """Window-level and scenario-level quality on windows the model never saw."""
    flagged = model.predict(scaler.transform(to_matrix(windows))) == -1
    truth = np.array([window.is_attack for window in windows])
    seen: dict[str, bool] = defaultdict(bool)
    for window, hit in zip(windows, flagged):
        for scenario_id in window.scenario_ids:
            seen[scenario_id] |= bool(hit)
    per_scenario = {}
    for name in SCENARIOS:
        outcomes = [hit for scenario_id, hit in seen.items() if scenario_id.split("#")[0] == name]
        per_scenario[name] = {"instances": len(outcomes), "detected": int(sum(outcomes)),
                              "detection_rate": round(sum(outcomes) / len(outcomes), 4) if outcomes else None}
    benign = ~truth
    return {
        "windows": len(windows), "attack_windows": int(truth.sum()),
        "benign_false_positive_rate": float(flagged[benign].mean()) if benign.any() else None,
        "scenario_instances": len(seen), "scenarios_detected": int(sum(seen.values())),
        "scenario_detection_rate": round(sum(seen.values()) / len(seen), 4) if seen else None,
        "per_scenario": per_scenario,
        "classification_report": classification_report(truth, flagged, output_dict=True, zero_division=0),
        "confusion_matrix": confusion_matrix(truth, flagged, labels=[False, True]).tolist(),
    }


def train_and_save(settings: MLSettings) -> dict:
    count = settings.training_sample_size
    train = [window for window in simulate_windows(TRAIN_SEED, count) if not window.is_attack]
    calibration = [window for window in simulate_windows(CALIBRATION_SEED, count) if not window.is_attack]
    held_out = simulate_windows(EVALUATION_SEED, count)

    scaler = StandardScaler()
    train_scaled = scaler.fit_transform(to_matrix(train))
    # Novelty detection: learn what normal looks like, so attacks are whatever deviates from it.
    model = IsolationForest(n_estimators=250, contamination="auto", random_state=42, max_samples="auto", n_jobs=-1)
    model.fit(train_scaled)
    # Alert threshold = the score below which only `target_false_positive_rate` of unseen benign windows fall.
    benign_scores = model.score_samples(scaler.transform(to_matrix(calibration)))
    model.offset_ = float(np.quantile(benign_scores, settings.target_false_positive_rate))
    evaluation = evaluate(model, scaler, held_out)

    metadata = {
        "model_name": settings.model_name,
        "model_version": settings.model_version,
        "algorithm": "IsolationForest",
        "feature_columns": FEATURE_COLUMNS,
        "feature_contract_version": FEATURE_CONTRACT_VERSION,
        "preprocessing": PREPROCESSING,
        "entity_type": "source_ip",
        "window_seconds": 60,
        "random_state": 42,
        "traffic_profile_version": PROFILE_VERSION,
        "training_data": "benign windows from the shared synthetic traffic model (pipeline_runtime.traffic_model)",
        "evaluation_data": "held-out synthetic simulation; not real-world accuracy",
        "target_false_positive_rate": float(settings.target_false_positive_rate),
        "calibrated_offset": model.offset_,
        "created_at": datetime.now(UTC).isoformat(),
        "training_sample_size": len(train),
    }
    metrics = {
        "model_name": settings.model_name,
        "model_version": settings.model_version,
        "traffic_profile_version": PROFILE_VERSION,
        "evaluation": evaluation,
        "feature_means": {column: float(value) for column, value in to_matrix(train).mean().items()},
    }
    save_artifacts(settings.artifact_dir, model, scaler, metadata, metrics)
    logger.info(
        "saved_model_artifacts artifact_dir=%s train_windows=%s benign_fpr=%.4f scenario_detection_rate=%s",
        settings.artifact_dir, len(train), evaluation["benign_false_positive_rate"], evaluation["scenario_detection_rate"],
    )
    return metrics


def main() -> None:
    settings = get_settings()
    configure_logging(settings.inference_log_level)
    train_and_save(settings)


if __name__ == "__main__":
    main()
