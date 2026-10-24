"""Detectors trained and tuned only on their authorized CIC-IDS-2017 days."""

from __future__ import annotations

import json
import runpy
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.metrics import confusion_matrix, precision_recall_curve
from sklearn.preprocessing import StandardScaler

from eval.features import FEATURE_COLUMNS, assert_no_identity_leakage


RANDOM_SEED = 42
REPO_ROOT = Path(__file__).resolve().parents[1]


class Detector(Protocol):
    name: str

    def predict(self, windows: pd.DataFrame) -> np.ndarray: ...


def _features(windows: pd.DataFrame) -> pd.DataFrame:
    assert_no_identity_leakage(FEATURE_COLUMNS)
    return windows.loc[:, FEATURE_COLUMNS]


def _binary_stats(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[float, float, float]:
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[False, True]).ravel()
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    fpr = fp / (fp + tn) if fp + tn else 0.0
    return f1, fpr, recall


def _candidate_thresholds(values: pd.Series) -> list[float]:
    quantiles = values.quantile([0.90, 0.95, 0.975, 0.99, 0.995, 0.999]).tolist()
    fixed = [2, 5, 10, 20, 50, 100, 250, 500]
    return sorted({float(max(1, round(value))) for value in [*quantiles, *fixed]})


@dataclass(frozen=True)
class RuleThresholds:
    flow_count: float
    top_destination_port_flow_count: float
    distinct_destination_ports: float


class ThresholdRules:
    name = "Threshold rules"

    def __init__(self, thresholds: RuleThresholds) -> None:
        self.thresholds = thresholds

    def predict(self, windows: pd.DataFrame) -> np.ndarray:
        t = self.thresholds
        return (
            (windows["flow_count"].to_numpy() >= t.flow_count)
            | (windows["top_destination_port_flow_count"].to_numpy() >= t.top_destination_port_flow_count)
            | (windows["distinct_destination_ports"].to_numpy() >= t.distinct_destination_ports)
        )


def tune_threshold_rules(validation: pd.DataFrame) -> ThresholdRules:
    y_true = validation["is_attack"].to_numpy(dtype=bool)
    best: tuple[tuple[float, float, float], RuleThresholds] | None = None
    for flow_count in _candidate_thresholds(validation["flow_count"]):
        for top_port in _candidate_thresholds(validation["top_destination_port_flow_count"]):
            for distinct_ports in _candidate_thresholds(validation["distinct_destination_ports"]):
                thresholds = RuleThresholds(flow_count, top_port, distinct_ports)
                y_pred = ThresholdRules(thresholds).predict(validation)
                f1, fpr, recall = _binary_stats(y_true, y_pred)
                ranking = (f1, -fpr, recall)
                if best is None or ranking > best[0]:
                    best = (ranking, thresholds)
    assert best is not None
    return best[1]


class IsolationForestDetector:
    name = "Isolation Forest"

    def __init__(self, scaler: StandardScaler, model: IsolationForest, threshold: float) -> None:
        self.scaler = scaler
        self.model = model
        self.threshold = threshold

    def scores(self, windows: pd.DataFrame) -> np.ndarray:
        scaled = self.scaler.transform(_features(windows))
        return -self.model.decision_function(scaled)

    def predict(self, windows: pd.DataFrame) -> np.ndarray:
        return self.scores(windows) >= self.threshold


def train_isolation_forest(train_benign: pd.DataFrame, validation: pd.DataFrame) -> IsolationForestDetector:
    if train_benign["is_attack"].any():
        raise ValueError("Isolation Forest training must contain benign windows only")
    scaler = StandardScaler()
    train_x = scaler.fit_transform(_features(train_benign))
    model = IsolationForest(
        n_estimators=200,
        contamination="auto",
        random_state=RANDOM_SEED,
        n_jobs=-1,
    )
    model.fit(train_x)
    validation_scores = -model.decision_function(scaler.transform(_features(validation)))
    y_true = validation["is_attack"].to_numpy(dtype=bool)
    precision, recall, thresholds = precision_recall_curve(y_true, validation_scores)
    f1 = np.divide(
        2 * precision[:-1] * recall[:-1],
        precision[:-1] + recall[:-1],
        out=np.zeros_like(thresholds),
        where=(precision[:-1] + recall[:-1]) != 0,
    )
    threshold = float(thresholds[int(np.argmax(f1))])
    return IsolationForestDetector(scaler, model, threshold)


class SupervisedRandomForest:
    name = "Random Forest (supervised reference upper bound)"

    def __init__(self, model: RandomForestClassifier) -> None:
        self.model = model

    def predict(self, windows: pd.DataFrame) -> np.ndarray:
        return self.model.predict(_features(windows)).astype(bool)


def train_supervised_reference(validation: pd.DataFrame) -> SupervisedRandomForest:
    model = RandomForestClassifier(
        n_estimators=200,
        min_samples_leaf=2,
        class_weight="balanced",
        random_state=RANDOM_SEED,
        n_jobs=-1,
    )
    model.fit(_features(validation), validation["is_attack"].to_numpy(dtype=bool))
    return SupervisedRandomForest(model)


class AllBenignBaseline:
    name = "All benign baseline"

    def predict(self, windows: pd.DataFrame) -> np.ndarray:
        return np.zeros(len(windows), dtype=bool)


def production_model_coverage() -> dict[str, object]:
    """Report exact flow equivalents without fabricating missing telemetry."""
    definitions = runpy.run_path(str(REPO_ROOT / "ml-models/app/features/feature_definitions.py"))
    production_features = list(definitions["FEATURE_COLUMNS"])
    equivalents = {
        "event_count": "flow_count",
        "network_event_count": "flow_count",
        "avg_network_bytes": "(total_fwd_bytes + total_bwd_bytes) / flow_count",
        "unique_destination_ip_count": "distinct_destination_ips",
    }
    applicable = {name: equivalents[name] for name in production_features if name in equivalents}
    missing = [name for name in production_features if name not in applicable]
    metadata_path = REPO_ROOT / "ml-models/artifacts/contract-v2/feature_metadata.json"
    artifact_feature_count = None
    if metadata_path.is_file():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        artifact_feature_count = len(metadata.get("feature_columns", []))
    return {
        "contract_feature_count": len(production_features),
        "real_equivalent_count": len(applicable),
        "equivalents": applicable,
        "missing": missing,
        "artifact_feature_count": artifact_feature_count,
        "can_evaluate": False,
        "reason": "Missing production telemetry features; filling them would invent values.",
    }
