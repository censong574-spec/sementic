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
    """Multica agents listing endpoint (GET /api/agents, full URL without query)."""

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


class MaosSettings(BaseSettings):
    """In-process MAOS / Temporal execution (merged from maos_job runtime)."""

    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_MAOS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    temporal_address: str = "127.0.0.1:7233"
    temporal_namespace: str = "default"
    startup_timeout_seconds: float = 60.0
    multica_job_api_base: str = "http://127.0.0.1:8080"
    multica_job_token: str = ""
    multica_job_workspace_id: str = ""
    multica_job_workspace_slug: str = ""


class ImEgressSettings(BaseSettings):
    """Worker-side Mattermost egress via Mattermost external_ingress (Redis shared token)."""

    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_MM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    url: str = ""
    enabled: bool = True
    timeout_seconds: float = 15.0
    poll_interval_seconds: float = 5.0
    completion_timeout_seconds: float = 7200.0
    external_ingress_redis_key: str = "shared:service_token"
    default_bot_token: str = ""
    bot_tokens_json: str = ""

    def token_for_bot(self, bot_user_id: str) -> str | None:
        import json

        mapping: dict[str, str] = {}
        if self.bot_tokens_json.strip():
            try:
                parsed = json.loads(self.bot_tokens_json)
                if isinstance(parsed, dict):
                    mapping = {str(k): str(v) for k, v in parsed.items() if v}
            except json.JSONDecodeError:
                pass
        token = mapping.get(bot_user_id) or mapping.get(bot_user_id.strip())
        if token:
            return token
        default = self.default_bot_token.strip()
        return default or None
