from infrastructure_runtime.security import database_url as secured_database_url
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageSinkSettings(BaseSettings):
    kafka_bootstrap_servers: str = Field(default="localhost:9092", validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_consumer_group: str = Field(default="streaming-analytics-storage-sink", validation_alias="STORAGE_SINK_CONSUMER_GROUP")
    deadletter_topic: str = Field(default="telemetry.deadletter.events", validation_alias="DEADLETTER_TOPIC")
    storage_sink_log_level: str = Field(default="INFO", validation_alias="STORAGE_SINK_LOG_LEVEL")

    postgres_host: str = Field(default="localhost", validation_alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, validation_alias="POSTGRES_PORT")
    postgres_db: str = Field(default="streaming_analytics", validation_alias="POSTGRES_DB")
    postgres_user: str = Field(default="streaming_user", validation_alias="POSTGRES_USER")
    postgres_password: str = Field(default="streaming_password", validation_alias="POSTGRES_PASSWORD")

    elasticsearch_host: str = Field(default="http://localhost:9200", validation_alias="ELASTICSEARCH_HOST")
    metrics_port: int = Field(default=9102, validation_alias="STORAGE_SINK_METRICS_PORT")

    consumer_client_id: str = "streaming-analytics-storage-sink"
    batch_size: int = Field(default=200, ge=1, le=1000, validation_alias="STORAGE_SINK_BATCH_SIZE")
    batch_wait_ms: int = Field(default=50, ge=0, le=1000, validation_alias="STORAGE_SINK_BATCH_WAIT_MS")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore")

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def validate_bootstrap_servers(cls, value: str) -> str:
        servers = [server.strip() for server in value.split(",") if server.strip()]
        if not servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must define at least one Kafka broker")
        return ",".join(servers)

    @property
    def database_url(self) -> str:
        return secured_database_url(f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}")


@lru_cache
def get_settings() -> StorageSinkSettings:
    return StorageSinkSettings()
