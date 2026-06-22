"""持久化执行层的运行时配置。

这里集中管理 Sandbox、Simulator、Multica/AgentService 的地址和轮询参数，
避免 provider 代码到处直接读取环境变量。
"""

import os


def simulator_api_base() -> str:
    return os.environ.get("SIMULATOR_API_BASE", "http://127.0.0.1:8767")


def sandbox_api_base() -> str:
    return os.environ.get("SANDBOX_API_BASE", "http://127.0.0.1:8765")


def agent_service_api_base() -> str:
    return os.environ.get("AGENT_SERVICE_API_BASE", "http://127.0.0.1:8091")


def multica_job_api_base() -> str:
    return os.environ.get("MULTICA_JOB_API_BASE", "http://localhost:8080")


def multica_job_token() -> str:
    return os.environ.get("MULTICA_JOB_TOKEN", "")


def multica_job_workspace_id() -> str:
    return os.environ.get("MULTICA_JOB_WORKSPACE_ID", "")


def multica_job_workspace_slug() -> str:
    return os.environ.get("MULTICA_JOB_WORKSPACE_SLUG", "")


def agent_poll_seconds() -> float:
    return float(os.environ.get("AGENT_SERVICE_POLL_SECONDS", "30"))


def agent_result_recent_comments() -> int:
    return int(os.environ.get("AGENT_SERVICE_RESULT_RECENT_COMMENTS", "3"))


def agent_result_text_limit() -> int:
    return int(os.environ.get("AGENT_SERVICE_RESULT_TEXT_LIMIT", "1200"))


def agent_message_text_limit() -> int:
    return int(os.environ.get("AGENT_SERVICE_MESSAGE_TEXT_LIMIT", "1000"))


def agent_fetch_run_messages() -> bool:
    value = os.environ.get("AGENT_SERVICE_FETCH_RUN_MESSAGES", "false")
    return value.lower() in {"1", "true", "yes", "on"}


def http_timeout_seconds(base_url: str) -> float:
    if base_url.rstrip("/") == multica_job_api_base().rstrip("/"):
        return float(os.environ.get("MULTICA_JOB_HTTP_TIMEOUT_SECONDS", "75"))
    if base_url.rstrip("/") == agent_service_api_base().rstrip("/"):
        return float(os.environ.get("AGENT_SERVICE_HTTP_TIMEOUT_SECONDS", "75"))
    return float(os.environ.get("SIMULATOR_HTTP_TIMEOUT_SECONDS", "15"))
