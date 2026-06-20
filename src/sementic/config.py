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
    """Bot management service; only base address is configurable."""

    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_BOT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    service_base: str = ""
    timeout_seconds: float = 5.0
