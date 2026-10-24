from functools import lru_cache
from infrastructure_runtime.security import database_url, hardened, elasticsearch_options
from typing import List

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _parse_csv_origins(value: str) -> List[str]:
    origins = [origin.strip() for origin in value.split(",")]
    return [origin for origin in origins if origin]


class Settings(BaseSettings):
    project_name: str = "Real-Time Streaming Analytics Platform"
    app_version: str = "0.1.0"
    environment: str = "development"
    api_v1_prefix: str = "/api/v1"
    backend_host: str = "0.0.0.0"
    backend_port: int = 8000
    backend_cors_origins_raw: str = Field(
        default="http://localhost:5173",
        validation_alias="BACKEND_CORS_ORIGINS",
        description="Comma-separated CORS origins loaded from BACKEND_CORS_ORIGINS.",
    )
    auth_local_http: bool = False
    session_seconds: int = Field(default=28800, ge=300, le=86400)
    session_idle_seconds: int = Field(default=1800, ge=60, le=28800)
    metrics_token_file: str = ""
    log_level: str = "INFO"
    database_url: str = Field(default="sqlite+pysqlite:///./analytics.db", repr=False)
    elasticsearch_host: str = "http://localhost:9200"
    prometheus_url: str = "http://localhost:9090"
    grafana_url: str = "http://localhost:3000"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("backend_cors_origins_raw", mode="before")
    @classmethod
    def validate_cors_origins_raw(cls, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("BACKEND_CORS_ORIGINS must be a comma-separated string")

        if not _parse_csv_origins(value):
            raise ValueError("BACKEND_CORS_ORIGINS must define at least one origin")

        return value

    @model_validator(mode="after")
    def secure_origins(self):
        self.database_url = database_url(self.database_url)
        if self.environment == 'production':
            if not hardened() or self.auth_local_http or not self.elasticsearch_host.startswith('https://'):
                raise RuntimeError('Production requires explicit verified service transports')
            elasticsearch_options()
        from urllib.parse import urlsplit
        for origin in self.cors_origins:
            url = urlsplit(origin)
            if url.scheme not in ("http", "https") or not url.netloc or url.path or url.query or url.fragment or url.username:
                raise ValueError("Explicit origin URLs without paths or wildcards are required")
            if "*" in origin:
                raise ValueError("Wildcard origins are forbidden")
            if self.auth_local_http and (self.environment != "development" or url.hostname not in ("localhost", "127.0.0.1", "::1")):
                raise ValueError("HTTP cookie exception is restricted to development loopback origins")
            if self.environment != "development" and url.scheme != "https":
                raise ValueError("Production origins require HTTPS")
        return self

    @property
    def session_cookie(self):
        return "soc_session_local" if self.auth_local_http else "__Host-soc_session"

    @property
    def cors_origins(self) -> List[str]:
        return _parse_csv_origins(self.backend_cors_origins_raw)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
