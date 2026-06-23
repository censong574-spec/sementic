from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import json
import pytest
from fakeredis.aioredis import FakeRedis

from sementic.config import ImEgressSettings, RedisSettings
from sementic.im_egress.client import MattermostPostClient
from sementic.im_egress.context import build_egress_context
from sementic.im_egress.publisher import ImEgressPublisher
from sementic.im_egress.result import extract_final_reply_text, iter_agent_node_replies
from sementic.im_models import IMMessageEvent
from sementic.models import BotProfile
from sementic.redis_history import RedisHistoryStore
from sementic.task_graph import TaskGraph, TaskGraphNode, TaskGraphPlan


def _event() -> IMMessageEvent:
    return IMMessageEvent.model_validate(
        {
            "event_id": "user:123",
            "group_session_id": "channel_1",
            "workspace_id": "ws-1",
            "multica_token": "token",
            "user_context": {
                "user_id": "user",
                "username": "alice",
                "is_bot": False,
                "ownership": "OTHERS",
            },
            "message_context": {
                "msg_id": "user:123",
                "content": "写 LED 代码",
                "mentions_registry": [],
            },
        }
    )


def _plan() -> TaskGraphPlan:
    return TaskGraphPlan(
        confidence=0.9,
        reply_to_user="我来写代码",
        graph=TaskGraph(
            id="plan-1",
            nodes=[
                TaskGraphNode(id="intake", operation="emit"),
                TaskGraphNode(
                    id="run_task",
                    operation="agent_task",
                    deps=["intake"],
                    agent={
                        "backend": "multica_job",
                        "agent_key": "bot_codex",
                        "agent_name": "codex-desktop",
                    },
                ),
            ],
        ),
    )


def test_build_egress_context_uses_run_task_agent() -> None:
    bots = [
        BotProfile(
            bot_user_id="bot_codex",
            display_name="codex-desktop",
            role="coding",
            multica_agent_id="agent-uuid",
        )
    ]
    ctx = build_egress_context(_event(), _plan(), bots)
    assert ctx is not None
    assert ctx.bot_user_id == "bot_codex"
    assert ctx.bot_username == "codex-desktop"
    assert ctx.root_post_id == "user:123"
    assert ctx.channel_id == "channel_1"


def test_extract_final_reply_text_from_snapshot() -> None:
    snapshot = {
        "status": "completed",
        "result": {
            "results": {
                "run_task": {
                    "final_reply": "```c\n#include <reg52.h>\n```",
                }
            }
        },
    }
    assert "reg52" in (extract_final_reply_text(snapshot) or "")


def test_extract_final_reply_text_from_generate_code_node() -> None:
    snapshot = {
        "status": "completed",
        "result": {
            "results": {
                "generate_code": {
                    "final_reply": "```c\n#include <REG51.H>\n```",
                }
            }
        },
    }
    assert "REG51" in (extract_final_reply_text(snapshot) or "")


def test_iter_agent_node_replies_skips_emit_and_incomplete() -> None:
    snapshot = {
        "status": "running",
        "result": {
            "results": {
                "intake": {"channel_id": "c1", "user_message": "hello"},
                "generate_code": {"status": "running"},
                "run_task": {
                    "status": "completed",
                    "final_reply": "done output",
                },
            }
        },
    }
    replies = iter_agent_node_replies(snapshot)
    assert replies == [("run_task", "done output")]


def test_mattermost_client_external_ingress(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        captured["json"] = json.loads(request.content.decode())
        return httpx.Response(201, json={"id": "post_1"})

    settings = ImEgressSettings(url="http://mattermost.test")
    client = MattermostPostClient(settings)
    monkeypatch.setattr(client, "_external_ingress_token", lambda: "shared-secret")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as http_client:

        def patched_do_post(url, *, headers, payload, bot_user_id):
            response = http_client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()

        client._do_post = patched_do_post  # type: ignore[method-assign]
        result = client.post_reply(
            bot_user_id="bot_codex",
            channel_id="channel_1",
            root_post_id="root_1",
            message="hello mm",
        )

    assert result == {"id": "post_1"}
    assert "external_ingress=true" in captured["url"]
    assert captured["headers"]["x-mm-external-ingress-token"] == "shared-secret"
    assert captured["headers"]["x-mm-external-post-as-user-id"] == "bot_codex"
    assert captured["json"]["user_id"] == "bot_codex"
    assert captured["json"]["root_id"] == "root_1"


@pytest.mark.asyncio
async def test_im_egress_publisher_posts_final_and_writes_redis() -> None:
    client = MagicMock()
    client.post_reply.return_value = {"id": "post_bot_1"}

    redis = FakeRedis(decode_responses=True)
    history = RedisHistoryStore(redis, RedisSettings(max_messages=20, planner_window=10))
    publisher = ImEgressPublisher(client=client, history_store=history)
    ctx = build_egress_context(_event(), _plan(), [
        BotProfile(
            bot_user_id="bot_codex",
            display_name="codex-desktop",
            role="coding",
        )
    ])
    assert ctx is not None
    await publisher.publish_final(ctx, "final result text")

    client.post_reply.assert_called_once_with(
        bot_user_id="bot_codex",
        channel_id="channel_1",
        root_post_id="user:123",
        message="final result text",
    )
    recent = await history.get_recent("channel_1", count=5)
    assert len(recent) == 1
    assert recent[0].is_bot is True
    assert recent[0].sender_name == "codex-desktop"
    assert recent[0].content == "final result text"
