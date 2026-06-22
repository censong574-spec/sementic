from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fakeredis.aioredis import FakeRedis

from sementic.bot_registry import BotRegistry
from sementic.config import RedisSettings
from sementic.execution.maos_executor import MaosExecutor
from sementic.handler import MessageHandler
from sementic.im_models import IMMessageEvent
from sementic.intent_classifier import TaskIntentClassifier
from sementic.llm import MockIntentLLMClient, MockLLMClient
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore
from sementic.task_graph import TaskGraphPlan


def _task_event() -> IMMessageEvent:
    return IMMessageEvent.model_validate(
        {
            "event_id": "evt_task",
            "group_session_id": "room_1",
            "workspace_id": "ws-1",
            "multica_token": "token-abc",
            "user_context": {
                "user_id": "user_1",
                "username": "alice",
                "is_bot": False,
                "ownership": "OTHERS",
            },
            "message_context": {
                "msg_id": "post_1",
                "content": "@Jira-Helper 挂个单",
                "mentions_registry": [{"entity_id": "bot_jira_123", "ownership": "MY_SYSTEM"}],
            },
        }
    )


@pytest.mark.asyncio
async def test_handler_submits_plan_to_maos() -> None:
    redis = FakeRedis(decode_responses=True)
    history_store = RedisHistoryStore(redis, RedisSettings(max_messages=20, planner_window=10))

    maos = MagicMock(spec=MaosExecutor)
    maos.submit_plan.return_value = ["task-demo-1"]

    handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
        bot_registry=BotRegistry(),
        maos_executor=maos,
    )

    response = await handler.handle(_task_event())

    assert response.plan is not None
    assert response.maos_task_ids == ["task-demo-1"]
    maos.submit_plan.assert_called_once()
    submitted_plan: TaskGraphPlan = maos.submit_plan.call_args.args[0]
    assert submitted_plan.graph.input["multica_token"] == "token-abc"
    assert submitted_plan.graph.input["workspace_id"] == "ws-1"
