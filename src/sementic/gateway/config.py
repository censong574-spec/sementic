from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_GATEWAY_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = "0.0.0.0"
    port: int = 8080


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_KAFKA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    bootstrap_servers: str = "localhost:9092"
    topic: str = "im.messages"
    client_id: str = "sementic-gateway"
