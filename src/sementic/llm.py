from __future__ import annotations

import json
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal

import httpx
from pydantic_settings import BaseSettings, SettingsConfigDict

ProviderName = Literal["openai", "aliyun"]


@dataclass(frozen=True)
class ProviderPreset:
    api_base: str
    model: str


PROVIDER_PRESETS: dict[ProviderName, ProviderPreset] = {
    "openai": ProviderPreset(
        api_base="https://api.openai.com/v1",
        model="gpt-4o-mini",
    ),
    "aliyun": ProviderPreset(
        api_base="https://dashscope.aliyuncs.com/compatible-mode/v1",
        model="qwen-plus",
    ),
}


class LLMSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_LLM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    provider: ProviderName = "aliyun"
    api_base: str = ""
    api_key: str = ""
    model: str = ""
    timeout_seconds: float = 180.0

    def resolved(self) -> LLMSettings:
        preset = PROVIDER_PRESETS[self.provider]
        return self.model_copy(
            update={
                "api_base": self.api_base or preset.api_base,
                "model": self.model or preset.model,
                "api_key": self.api_key or os.getenv("DASHSCOPE_API_KEY", ""),
            }
        )


class IntentLLMSettings(BaseSettings):
    """Stage-1 task-intent gate; swap to a local small model via SEMENTIC_INTENT_LLM_*."""

    model_config = SettingsConfigDict(
        env_prefix="SEMENTIC_INTENT_LLM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    provider: ProviderName = "aliyun"
    api_base: str = ""
    api_key: str = ""
    model: str = ""
    timeout_seconds: float = 30.0

    def resolved(self) -> IntentLLMSettings:
        preset = PROVIDER_PRESETS[self.provider]
        fallback = LLMSettings().resolved()
        return self.model_copy(
            update={
                "api_base": self.api_base or preset.api_base,
                "model": self.model or preset.model,
                "api_key": self.api_key or os.getenv("DASHSCOPE_API_KEY", "") or fallback.api_key,
            }
        )


class LLMClient(ABC):
    @abstractmethod
    async def complete(self, *, system: str, user: str) -> str:
        raise NotImplementedError


def create_llm_client(
    *,
    provider: ProviderName | None = None,
    mock: bool = False,
    settings: LLMSettings | None = None,
) -> LLMClient:
    if mock:
        return MockLLMClient()

    raw = settings or LLMSettings()
    if provider:
        raw = raw.model_copy(update={"provider": provider})

    config = raw.resolved()
    if not config.api_key:
        raise RuntimeError(
            "LLM API key is missing. Set SEMENTIC_LLM_API_KEY or DASHSCOPE_API_KEY in .env, "
            "or run with --mock."
        )

    return OpenAICompatibleClient(config)


def create_intent_llm_client(
    *,
    provider: ProviderName | None = None,
    mock: bool = False,
    settings: IntentLLMSettings | None = None,
) -> LLMClient:
    if mock:
        return MockIntentLLMClient()

    raw = settings or IntentLLMSettings()
    if provider:
        raw = raw.model_copy(update={"provider": provider})

    config = raw.resolved()
    if not config.api_key:
        raise RuntimeError(
            "Intent LLM API key is missing. Set SEMENTIC_INTENT_LLM_API_KEY or "
            "SEMENTIC_LLM_API_KEY in .env, or run with --mock."
        )

    return OpenAICompatibleClient(
        LLMSettings(
            provider=config.provider,
            api_base=config.api_base,
            api_key=config.api_key,
            model=config.model,
            timeout_seconds=config.timeout_seconds,
        )
    )


class OpenAICompatibleClient(LLMClient):
    def __init__(self, settings: LLMSettings | None = None) -> None:
        self.settings = (settings or LLMSettings()).resolved()

    async def complete(self, *, system: str, user: str) -> str:
        if not self.settings.api_key:
            raise RuntimeError(
                "LLM API key is missing. Set SEMENTIC_LLM_API_KEY or DASHSCOPE_API_KEY in .env, "
                "or use MockLLMClient."
            )

        headers = {
            "Authorization": f"Bearer {self.settings.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.settings.model,
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }

        # trust_env=True (default): use HTTP_PROXY/HTTPS_PROXY for DashScope etc.
        async with httpx.AsyncClient(timeout=self.settings.timeout_seconds) as client:
            response = await client.post(
                f"{self.settings.api_base.rstrip('/')}/chat/completions",
                headers=headers,
                json=payload,
            )

        if response.is_error:
            raise RuntimeError(
                f"LLM request failed ({response.status_code}) "
                f"provider={self.settings.provider} model={self.settings.model}: "
                f"{response.text}"
            )

        data = response.json()
        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise RuntimeError(f"Unexpected LLM response shape: {data}") from exc


class MockLLMClient(LLMClient):
    """Deterministic planner for local development and tests."""

    async def complete(self, *, system: str, user: str) -> str:
        payload = _mock_plan_from_user_prompt(user)
        return json.dumps(payload, ensure_ascii=False)


class MockIntentLLMClient(LLMClient):
    """Deterministic stage-1 task-intent gate for tests."""

    async def complete(self, *, system: str, user: str) -> str:
        payload = _mock_intent_from_user_prompt(user)
        return json.dumps(payload, ensure_ascii=False)


def normalize_plan_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    steps = normalized.get("steps")
    if not isinstance(steps, list):
        return normalized

    normalized_steps: list[Any] = []
    for step in steps:
        if not isinstance(step, dict):
            normalized_steps.append(step)
            continue

        step_copy = dict(step)
        step_copy["params"] = _coerce_params_dict(step_copy.get("params"))
        normalized_steps.append(step_copy)

    normalized["steps"] = normalized_steps
    return normalized


def _coerce_params_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return {}
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            raise ValueError("params must be a JSON object")
        return parsed
    raise ValueError("params must be a JSON object")


def extract_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"LLM output is not valid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError("LLM output must be a JSON object")

    return parsed


def _mock_plan_from_user_prompt(user: str) -> dict[str, Any]:
    current_message = _extract_section(user, "Latest user message:")
    if not current_message:
        current_message = _extract_section(user, "Latest message:")
    channel_id = _extract_section(user, "Channel ID:") or "room"
    bot_ids = _extract_bot_ids(user)
    primary_bot_id = bot_ids[0] if bot_ids else "bot_project_assistant"

    if "clarify" in current_message.lower() or "那个东西" in current_message:
        return _mock_task_graph_plan(
            confidence=0.42,
            reply_to_user="需要更多信息才能继续：请补充脚本名或报错关键词。",
            graph_name="澄清用户意图",
            channel_id=channel_id,
            user_message=current_message,
            nodes=[
                _mock_emit_node(
                    "intake",
                    "整理输入",
                    user_message=current_message,
                    channel_id=channel_id,
                ),
                {
                    "id": "clarify",
                    "label": "向用户澄清",
                    "operation": "join",
                    "deps": ["intake"],
                    "params": {
                        "fields": {
                            "status": "clarification_needed",
                            "text": "请说明要重启的具体脚本名称，或提供报错关键词。",
                        }
                    },
                },
            ],
            edges=[
                {"from": "intake", "to": "clarify", "label": "需要澄清"},
            ],
        )

    if len(bot_ids) >= 2 and ("挂个单" in current_message or "日志" in current_message):
        return _mock_task_graph_plan(
            confidence=0.9,
            reply_to_user="我先安排 Jira 机器人建单，再让日志机器人排查。",
            graph_name="建单并查日志",
            channel_id=channel_id,
            user_message=current_message,
            nodes=[
                _mock_emit_node("intake", "整理输入", user_message=current_message, channel_id=channel_id),
                _mock_agent_node(
                    "create_ticket",
                    "Jira建单",
                    bot_ids[0],
                    deps=["intake"],
                    prompt="只基于A2A payload创建工单。完成后评论并更新状态。",
                ),
                _mock_agent_node(
                    "log_analysis",
                    "日志分析",
                    bot_ids[1],
                    deps=["create_ticket"],
                    prompt="只基于A2A payload分析日志。完成后评论并更新状态。",
                ),
                _mock_join_node(
                    "final_summary",
                    "汇总结果",
                    deps=["create_ticket", "log_analysis"],
                    fields={
                        "status": "ready",
                        "ticket": "$deps.create_ticket.latest_comment",
                        "logs": "$deps.log_analysis.latest_comment",
                    },
                ),
            ],
            edges=[
                {"from": "intake", "to": "create_ticket"},
                {"from": "create_ticket", "to": "log_analysis"},
                {"from": "log_analysis", "to": "final_summary"},
            ],
        )

    if "报错" in current_message or "刚才" in current_message:
        return _mock_task_graph_plan(
            confidence=0.88,
            reply_to_user="我先检索频道里的报错记录，然后在你的 Windows 机器上重启 sharpen.py。",
            graph_name="检索报错并重启脚本",
            channel_id=channel_id,
            user_message=current_message,
            nodes=[
                _mock_emit_node("intake", "整理输入", user_message=current_message, channel_id=channel_id),
                {
                    "id": "search_history",
                    "label": "检索频道历史",
                    "operation": "emit",
                    "deps": ["intake"],
                    "params": {
                        "keyword": "Exception",
                        "time_range": "last_week",
                    },
                },
                _mock_agent_node(
                    "restart_cli",
                    "重启脚本",
                    primary_bot_id,
                    deps=["search_history"],
                    prompt="只基于A2A payload重启 sharpen.py。完成后评论并更新状态。",
                ),
            ],
            edges=[
                {"from": "intake", "to": "search_history"},
                {"from": "search_history", "to": "restart_cli"},
            ],
        )

    if "deploy" in current_message.lower() or "部署" in current_message:
        return _mock_task_graph_plan(
            confidence=0.93,
            reply_to_user="我将开始部署前端。",
            graph_name="部署前端",
            channel_id=channel_id,
            user_message=current_message,
            nodes=[
                _mock_emit_node("intake", "整理输入", user_message=current_message, channel_id=channel_id),
                _mock_agent_node(
                    "deploy",
                    "部署任务",
                    primary_bot_id,
                    deps=["intake"],
                    prompt="只基于A2A payload部署 frontend。完成后评论并更新状态。",
                ),
            ],
            edges=[{"from": "intake", "to": "deploy"}],
        )

    return _mock_task_graph_plan(
        confidence=0.75,
        reply_to_user=f"已收到你的消息：{current_message}",
        graph_name="单Bot执行任务",
        channel_id=channel_id,
        user_message=current_message,
        nodes=[
            _mock_emit_node("intake", "整理输入", user_message=current_message, channel_id=channel_id),
            _mock_agent_node(
                "run_task",
                "执行任务",
                primary_bot_id,
                deps=["intake"],
                prompt=f"只基于A2A payload处理：{current_message}。完成后评论并更新状态。",
            ),
        ],
        edges=[{"from": "intake", "to": "run_task"}],
    )


def _mock_task_graph_plan(
    *,
    confidence: float,
    reply_to_user: str,
    graph_name: str,
    channel_id: str,
    user_message: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "confidence": confidence,
        "reply_to_user": reply_to_user,
        "graph": {
            "id": f"mock-{graph_name}",
            "name": graph_name,
            "graph_type": "control_flow",
            "input": {
                "channel_id": channel_id,
                "user_message": user_message,
            },
            "start": "intake",
            "max_total_visits": 10,
            "nodes": nodes,
            "edges": edges,
        },
    }


def _mock_emit_node(
    node_id: str,
    label: str,
    *,
    user_message: str,
    channel_id: str,
) -> dict[str, Any]:
    return {
        "id": node_id,
        "label": label,
        "operation": "emit",
        "params": {
            "user_message": user_message,
            "channel_id": channel_id,
        },
    }


def _mock_agent_node(
    node_id: str,
    label: str,
    agent_key: str,
    *,
    deps: list[str],
    prompt: str,
) -> dict[str, Any]:
    return {
        "id": node_id,
        "label": label,
        "operation": "agent_task",
        "deps": deps,
        "agent": {
            "backend": "multica",
            "agent_key": agent_key,
            "context_policy": "provided_context_only",
            "runtime_profile": "maos_compact_agent",
            "execution_mode": "multica",
            "status": "in_progress",
            "priority": "medium",
            "poll_seconds": 30,
            "prompt": prompt,
        },
        "timeout_seconds": 7200,
    }


def _mock_join_node(
    node_id: str,
    label: str,
    *,
    deps: list[str],
    fields: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": node_id,
        "label": label,
        "operation": "join",
        "deps": deps,
        "params": {"fields": fields},
    }


def _mock_intent_from_user_prompt(user: str) -> dict[str, Any]:
    current_message = _extract_section(user, "Latest message:")
    text = current_message.strip()

    task_keywords = (
        "部署",
        "重启",
        "挂个单",
        "报错",
        "查下日志",
        "deploy",
        "restart",
        "create",
        "ticket",
        "run job",
        "分析",
        "查询",
    )
    if any(keyword in text for keyword in task_keywords) or "@" in text:
        return {
            "needs_task": True,
            "confidence": 0.9,
            "reason": "消息包含明确可执行的工作请求",
        }

    if "哈哈" in text or "我是" in text or len(text) <= 12:
        return {
            "needs_task": False,
            "confidence": 0.88,
            "reason": "属于寒暄或自我介绍，不需要创建任务",
        }

    return {
        "needs_task": False,
        "confidence": 0.7,
        "reason": "未识别到需要执行的工作指令",
    }


def _extract_bot_ids(user: str) -> list[str]:
    available_ids = _extract_available_bot_ids(user)
    mentioned = _extract_section(user, "Mentioned bot IDs:")
    if mentioned and mentioned != "(none)":
        mentioned_ids = [part.strip() for part in mentioned.split(",") if part.strip()]
        allowed = [bot_id for bot_id in mentioned_ids if bot_id in available_ids]
        if allowed:
            return allowed
    return available_ids


def _extract_available_bot_ids(user: str) -> list[str]:
    available = _extract_section(user, "Available bots:")
    bot_ids: list[str] = []
    for line in available.splitlines():
        marker = "id="
        start = line.find(marker)
        if start == -1:
            continue
        start += len(marker)
        end = line.find(",", start)
        bot_ids.append(line[start:end] if end != -1 else line[start:].strip())
    return bot_ids


def _extract_section(text: str, header: str) -> str:
    start = text.find(header)
    if start == -1:
        return ""
    start += len(header)
    rest = text[start:].lstrip("\n")
    end = rest.find("\n\n")
    if end == -1:
        return rest.strip()
    return rest[:end].strip()
