"""Agent provider 注册表。

核心 runtime 只依赖这里查找 provider。新增 backend 时注册一个实现即可，
不用再修改 send/poll/get_agent_card 的主分派逻辑。
"""

from __future__ import annotations

from maos_runtime.a2a_constants import (
    HERMES_BACKEND,
    MULTICA_BACKEND,
    MULTICA_JOB_BACKEND,
    SIMULATOR_BACKEND,
)
from maos_runtime.a2a_provider_base import AgentRuntimeProvider


_BACKEND_ALIASES = {
    "agent-service": MULTICA_BACKEND,
    "agentservice": MULTICA_BACKEND,
    "real-agent": MULTICA_BACKEND,
    "multica-daemon": MULTICA_BACKEND,
    "multica-job": MULTICA_JOB_BACKEND,
    "multica-native-job": MULTICA_JOB_BACKEND,
    "native-job": MULTICA_JOB_BACKEND,
    "sim": SIMULATOR_BACKEND,
    "mock": SIMULATOR_BACKEND,
    "simulator": SIMULATOR_BACKEND,
    "hermes": HERMES_BACKEND,
    "hermes-oneshot": HERMES_BACKEND,
    "direct-hermes": HERMES_BACKEND,
    "hermes-direct": HERMES_BACKEND,
}

_PROVIDERS: dict[str, AgentRuntimeProvider] = {}


def normalize_backend(backend: str) -> str:
    normalized = str(backend).lower().replace("_", "-")
    return _BACKEND_ALIASES.get(normalized, normalized)


def register_provider(
    provider: AgentRuntimeProvider,
    *,
    aliases: list[str] | tuple[str, ...] = (),
    replace: bool = False,
) -> None:
    backend = normalize_backend(provider.backend)
    if not backend:
        raise ValueError("Provider backend cannot be empty.")
    if backend in _PROVIDERS and not replace:
        raise ValueError(f"Provider backend already registered: {backend}")
    _PROVIDERS[backend] = provider
    for alias in aliases:
        _BACKEND_ALIASES[str(alias).lower().replace("_", "-")] = backend


def provider_for_backend(
    backend: str,
    *,
    node_id: str | None = None,
) -> AgentRuntimeProvider:
    normalized = normalize_backend(backend)
    provider = _PROVIDERS.get(normalized)
    if provider:
        return provider
    node_hint = f" for node {node_id}" if node_id else ""
    known = ", ".join(list_provider_backends())
    raise ValueError(
        f"Unsupported agent backend{node_hint}: {backend!r}. Known backends: {known}."
    )


def list_provider_backends() -> list[str]:
    return sorted(_PROVIDERS)


def clear_provider_registry() -> None:
    """测试辅助：清空 provider 注册表。生产代码不应调用。"""

    _PROVIDERS.clear()
