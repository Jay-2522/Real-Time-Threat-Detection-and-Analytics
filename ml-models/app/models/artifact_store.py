from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from pathlib import Path

import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from pipeline_runtime.traffic_model import PROFILE_VERSION

from app.features.feature_definitions import FEATURE_COLUMNS, FEATURE_CONTRACT_VERSION, PREPROCESSING


@dataclass(frozen=True)
class ModelArtifacts:
    model: IsolationForest
    scaler: StandardScaler
    metadata: dict


def artifacts_exist(artifact_dir: Path) -> bool:
    return (
        (artifact_dir / "isolation_forest.joblib").exists()
        and (artifact_dir / "feature_scaler.joblib").exists()
        and (artifact_dir / "feature_metadata.json").exists()
    )


def artifacts_current(artifact_dir: Path) -> bool:
    """False when the stored model was trained on an older traffic profile and must be retrained."""
    try:
        metadata = json.loads((artifact_dir / "feature_metadata.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return metadata.get("traffic_profile_version") == PROFILE_VERSION


def save_artifacts(artifact_dir: Path, model: IsolationForest, scaler: StandardScaler, metadata: dict, metrics: dict) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, artifact_dir / "isolation_forest.joblib")
    joblib.dump(scaler, artifact_dir / "feature_scaler.joblib")
    (artifact_dir / "feature_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    (artifact_dir / "training_metrics.json").write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")


def validate_metadata(metadata: dict) -> None:
    if (metadata.get("feature_columns") != FEATURE_COLUMNS
            or metadata.get("feature_contract_version") != FEATURE_CONTRACT_VERSION
            or metadata.get("preprocessing") != PREPROCESSING
            or metadata.get("entity_type") != "source_ip"
            or metadata.get("window_seconds") != 60):
        raise ValueError("Incompatible model contract; explicitly train contract 2.0 into a new artifact directory")


def validate_artifacts(artifacts: ModelArtifacts) -> None:
    validate_metadata(artifacts.metadata)
    if (list(artifacts.scaler.feature_names_in_) != FEATURE_COLUMNS
            or artifacts.scaler.n_features_in_ != len(FEATURE_COLUMNS)
            or artifacts.model.n_features_in_ != len(FEATURE_COLUMNS)):
        raise ValueError("Model/scaler feature dimensions or order do not match metadata")


def load_artifacts(artifact_dir: Path) -> ModelArtifacts:
    metadata = json.loads((artifact_dir / "feature_metadata.json").read_text(encoding="utf-8"))
    validate_metadata(metadata)  # Reject legacy metadata before deserializing binaries.
    metadata["artifact_id"] = hashlib.sha256(
        (artifact_dir / "isolation_forest.joblib").read_bytes() +
        (artifact_dir / "feature_scaler.joblib").read_bytes()).hexdigest()
    artifacts = ModelArtifacts(
        model=joblib.load(artifact_dir / "isolation_forest.joblib"),
        scaler=joblib.load(artifact_dir / "feature_scaler.joblib"), metadata=metadata,
    )
    validate_artifacts(artifacts)
    return artifacts
