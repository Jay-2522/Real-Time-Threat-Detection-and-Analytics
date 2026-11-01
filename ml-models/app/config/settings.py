from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.config.topics import ML_ANOMALY_PREDICTIONS, TELEMETRY_DEADLETTER_EVENTS


class MLSettings(BaseSettings):
    kafka_bootstrap_servers: str = Field(default="localhost:9092", validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_consumer_group: str = Field(default="streaming-analytics-ml-inference", validation_alias="ML_CONSUMER_GROUP")
    inference_log_level: str = Field(default="INFO", validation_alias="ML_LOG_LEVEL")
    artifact_dir: Path = Field(default=Path("/app/artifacts/contract-v2"), validation_alias="ML_ARTIFACT_DIR")
    model_name: str = Field(default="isolation_forest_anomaly_detector", validation_alias="ML_MODEL_NAME")
    model_version: str = Field(default="0.3.0", validation_alias="ML_MODEL_VERSION")
    prediction_topic: str = Field(default=ML_ANOMALY_PREDICTIONS, validation_alias="ML_PREDICTION_TOPIC")
    deadletter_topic: str = Field(default=TELEMETRY_DEADLETTER_EVENTS)
    auto_bootstrap_model: bool = Field(default=True, validation_alias="ML_AUTO_BOOTSTRAP_MODEL")
    training_sample_size: int = Field(default=12000, ge=1000, validation_alias="ML_TRAINING_SAMPLE_SIZE")
    target_false_positive_rate: float = Field(default=0.002, gt=0.0, le=0.1, validation_alias="ML_TARGET_FALSE_POSITIVE_RATE")
    metrics_port: int = Field(default=9101, validation_alias="ML_METRICS_PORT")

    producer_client_id: str = Field(default="streaming-analytics-ml-prediction-producer")
    consumer_client_id: str = Field(default="streaming-analytics-ml-inference-consumer")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        protected_namespaces=(),
    )

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def validate_bootstrap_servers(cls, value: str) -> str:
        servers = [server.strip() for server in value.split(",") if server.strip()]
        if not servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must define at least one Kafka broker")
        return ",".join(servers)

    @property
    def isolation_forest_path(self) -> Path:
        return self.artifact_dir / "isolation_forest.joblib"

    @property
    def feature_scaler_path(self) -> Path:
        return self.artifact_dir / "feature_scaler.joblib"

    @property
    def feature_metadata_path(self) -> Path:
        return self.artifact_dir / "feature_metadata.json"

    @property
    def training_metrics_path(self) -> Path:
        return self.artifact_dir / "training_metrics.json"


@lru_cache
def get_settings() -> MLSettings:
    return MLSettings()
