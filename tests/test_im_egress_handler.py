from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fakeredis.aioredis import FakeRedis

from sementic.bot_registry import BotRegistry
from sementic.config import RedisSettings
from sementic.execution.maos_executor import MaosExecutor
from sementic.handler import MessageHandler
from sementic.im_egress.watcher import MaosCompletionWatcher
from sementic.im_models import IMMessageEvent
from sementic.intent_classifier import TaskIntentClassifier
from sementic.llm import MockIntentLLMClient, MockLLMClient
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore


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
async def test_handler_starts_maos_watch_without_immediate_egress() -> None:
    redis = FakeRedis(decode_responses=True)
    history_store = RedisHistoryStore(redis, RedisSettings(max_messages=20, planner_window=10))

    maos = MagicMock(spec=MaosExecutor)
    maos.submit_plan.return_value = ["task-demo-1"]

    im_egress = AsyncMock()
    watcher = MagicMock(spec=MaosCompletionWatcher)

    handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
        bot_registry=BotRegistry(),
        maos_executor=maos,
        im_egress=im_egress,
        maos_completion_watcher=watcher,
    )

    response = await handler.handle(_task_event())

    assert response.plan is not None
    assert response.maos_task_ids == ["task-demo-1"]
    im_egress.publish_ack.assert_not_called()
    im_egress.publish_final.assert_not_called()
    watcher.watch.assert_called_once()
    assert watcher.watch.call_args.kwargs["task_id"] == "task-demo-1"
