from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_REDIS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    url: str = "redis://localhost:6379/0"
    max_messages: int = 20
    planner_window: int = 10
    key_prefix: str = "channel:history"


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_KAFKA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    bootstrap_servers: str = "localhost:9092"
    topic: str = "im.messages"
    group_id: str = "sementic-worker"
    client_id: str = "sementic-worker"


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_WORKER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    main_loop_interval_seconds: float = 1.0


class BotServiceSettings(BaseSettings):
    """Multica internal agents listing endpoint (full URL without query string)."""

    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_BOT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    agents_url: str = ""
    timeout_seconds: float = 5.0


class MulticaSettings(BaseSettings):
    """Multica control-plane base URL for workspace-scoped agent execution."""

    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_MULTICA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    service_base: str = "http://127.0.0.1:8080/api/multica"
    timeout_seconds: float = 30.0
