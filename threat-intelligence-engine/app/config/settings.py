from infrastructure_runtime.security import database_url as secured_database_url
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ThreatIntelSettings(BaseSettings):
    kafka_bootstrap_servers: str = Field(default="localhost:9092", validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    consumer_group: str = Field(default="streaming-analytics-threat-intelligence", validation_alias="THREAT_INTEL_CONSUMER_GROUP")
    log_level: str = Field(default="INFO", validation_alias="THREAT_INTEL_LOG_LEVEL")
    postgres_host: str = Field(default="localhost", validation_alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, validation_alias="POSTGRES_PORT")
    postgres_db: str = Field(default="streaming_analytics", validation_alias="POSTGRES_DB")
    postgres_user: str = Field(default="streaming_user", validation_alias="POSTGRES_USER")
    postgres_password: str = Field(default="streaming_password", validation_alias="POSTGRES_PASSWORD")
    elasticsearch_host: str = Field(default="http://localhost:9200", validation_alias="ELASTICSEARCH_HOST")
    rules_path: str = Field(default="/app/rules", validation_alias="DETECTION_RULES_PATH")
    metrics_port: int = Field(default=9106, validation_alias="THREAT_INTEL_METRICS_PORT")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore")

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def validate_bootstrap_servers(cls, value: str) -> str:
        servers = [server.strip() for server in value.split(",") if server.strip()]
        if not servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must define at least one broker")
        return ",".join(servers)

    @property
    def database_url(self) -> str:
        return secured_database_url(f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}")

@lru_cache
def get_settings() -> ThreatIntelSettings:
    return ThreatIntelSettings()
