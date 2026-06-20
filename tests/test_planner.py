from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from sementic.llm import MockLLMClient, extract_json_object, normalize_plan_payload
from sementic.models import BotProfile, ChatMessage, PlannerRequest
from sementic.planner import Planner
from sementic.prompts import SYSTEM_PROMPT
from sementic.task_graph import (
    TaskGraph,
    TaskGraphPlan,
    normalize_task_graph_payload,
)


def _sample_request(**overrides) -> PlannerRequest:
    base = {
        "channel_id": "room_99",
        "sender_user_id": "user_hassan",
        "sender_display_name": "Hassan",
        "recent_messages": [
            ChatMessage(sender="Hassan", text="刚才那个视频增强脚本报错了"),
        ],
        "mentioned_bot_ids": ["bot_project_assistant"],
        "available_bots": [
            BotProfile(
                bot_user_id="bot_project_assistant",
                display_name="项目助手",
                role="DevOps assistant",
                expertise=["restart_cli"],
                owner_user_id="user_hassan",
                share_scope="channel_shared",
                is_online=True,
            )
        ],
        "current_message": "@项目助手，把刚才那个报错的视频增强 CLI 脚本在我的 Windows 电脑上重启一下。",
    }
    base.update(overrides)
    return PlannerRequest(**base)


def test_system_prompt_includes_task_graph_format() -> None:
    assert "control_flow" in SYSTEM_PROMPT
    assert '"graph"' in SYSTEM_PROMPT
    assert "CANNOT" in SYSTEM_PROMPT
    assert "规划顺序" in SYSTEM_PROMPT
    assert "图形态" in SYSTEM_PROMPT
    assert "agent.prompt" in SYSTEM_PROMPT
    assert "澄清图仅当" in SYSTEM_PROMPT
    assert "给出代码" in SYSTEM_PROMPT
    assert 'params 固定为 {}' in SYSTEM_PROMPT


def test_normalize_task_graph_payload_coerces_string_params() -> None:
    payload = normalize_task_graph_payload(
        {
            "confidence": 0.9,
            "reply_to_user": "ok",
            "graph": {
                "id": "g1",
                "graph_type": "control_flow",
                "nodes": [
                    {
                        "id": "n1",
                        "operation": "emit",
                        "params": '{"goal": "restart"}',
                    }
                ],
                "edges": [],
            },
        }
    )
    plan = TaskGraphPlan.model_validate(payload)
    assert plan.graph.nodes[0].params["goal"] == "restart"


def test_task_graph_validates_edges() -> None:
    with pytest.raises(ValidationError):
        TaskGraph(
            id="g1",
            nodes=[{"id": "a", "operation": "emit"}],
            edges=[{"from": "a", "to": "missing"}],
        )


@pytest.mark.asyncio
async def test_mock_planner_returns_task_graph() -> None:
    planner = Planner(llm=MockLLMClient())
    plan = await planner.plan(_sample_request())

    assert plan.graph.graph_type == "control_flow"
    assert plan.graph.start == "intake"
    assert len(plan.graph.nodes) >= 2
    assert any(node.operation == "agent_task" for node in plan.graph.nodes)
    assert plan.graph.edges


@pytest.mark.asyncio
async def test_planner_rejects_offline_mentioned_bot() -> None:
    request = _sample_request(
        available_bots=[
            BotProfile(
                bot_user_id="bot_project_assistant",
                display_name="项目助手",
                role="DevOps assistant",
                owner_user_id="user_hassan",
                is_online=False,
            )
        ]
    )
    planner = Planner(llm=MockLLMClient())

    with pytest.raises(RuntimeError, match="offline"):
        await planner.plan(request)


@pytest.mark.asyncio
async def test_planner_rejects_private_bot_for_non_owner() -> None:
    request = _sample_request(
        sender_user_id="user_other",
        available_bots=[
            BotProfile(
                bot_user_id="bot_project_assistant",
                display_name="项目助手",
                role="DevOps assistant",
                owner_user_id="user_hassan",
                share_scope="private",
                is_online=True,
            )
        ],
    )
    planner = Planner(llm=MockLLMClient())

    with pytest.raises(PermissionError):
        await planner.plan(request)


def test_extract_json_object_strips_markdown_fence() -> None:
    payload = extract_json_object(
        '```json\n{"confidence": 0.5, "reply_to_user": "need more info", '
        '"graph": {"id": "g1", "graph_type": "control_flow", "nodes": [{"id": "intake", '
        '"operation": "emit", "params": {"text": "need more info"}}], "edges": []}}\n```'
    )
    plan = TaskGraphPlan.model_validate(normalize_task_graph_payload(payload))
    assert plan.graph.nodes[0].id == "intake"


@pytest.mark.asyncio
async def test_clarification_graph_from_mock_llm() -> None:
    planner = Planner(llm=MockLLMClient())
    plan = await planner.plan(
        _sample_request(current_message="帮我把那个东西处理一下，clarify")
    )

    assert any(node.id == "clarify" for node in plan.graph.nodes)
    assert "params" in json.loads(plan.model_dump_json())["graph"]["nodes"][1]
