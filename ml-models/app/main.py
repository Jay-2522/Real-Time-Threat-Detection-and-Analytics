import argparse
import logging

from app.config.settings import get_settings
from app.consumers.inference_consumer import MLInferenceConsumer
from app.inference.predictor import AnomalyPredictor
from app.models.artifact_store import artifacts_current, artifacts_exist, load_artifacts
from app.training.train_model import train_and_save
from app.utils.logging import configure_logging
from app.utils.metrics import ML_MODEL_LOADED_STATUS, start_metrics_server

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ML anomaly detection runtime")
    parser.add_argument("mode", choices=["inference", "train"], nargs="?", default="inference")
    return parser.parse_args()


def ensure_model_artifacts() -> None:
    settings = get_settings()
    if artifacts_exist(settings.artifact_dir) and artifacts_current(settings.artifact_dir):
        logger.info("model_artifacts_found artifact_dir=%s", settings.artifact_dir)
        return
    reason = "older_traffic_profile" if artifacts_exist(settings.artifact_dir) else "missing"
    if not settings.auto_bootstrap_model:
        raise FileNotFoundError(
            f"ML model artifacts in {settings.artifact_dir} are {reason}. "
            "Run: docker compose run --rm ml-inference python -m app.training.train_model"
        )

    logger.warning("model_artifacts_unusable reason=%s action=train artifact_dir=%s", reason, settings.artifact_dir)
    train_and_save(settings)


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.inference_log_level)
    start_metrics_server(settings.metrics_port)
    logger.info("starting_ml_runtime mode=%s", args.mode)

    if args.mode == "train":
        train_and_save(settings)
        return

    ensure_model_artifacts()
    artifacts = load_artifacts(settings.artifact_dir)
    ML_MODEL_LOADED_STATUS.set(1)
    logger.info(
        "model_artifacts_loaded model_name=%s model_version=%s features=%s",
        artifacts.metadata.get("model_name"),
        artifacts.metadata.get("model_version"),
        len(artifacts.metadata.get("feature_columns", [])),
    )
    predictor = AnomalyPredictor(settings=settings, artifacts=artifacts)
    MLInferenceConsumer(settings=settings, predictor=predictor).run()


if __name__ == "__main__":
    main()
