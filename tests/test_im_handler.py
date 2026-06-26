from __future__ import annotations

import pytest
from fakeredis.aioredis import FakeRedis

from sementic.bot_registry import BotRegistry
from sementic.config import RedisSettings
from sementic.direct_chat import DirectChatSubmission
from sementic.handler import MessageHandler
from sementic.im_egress.models import EgressContext
from sementic.im_models import (
    IMMessageEvent,
    MentionRegistryItem,
    MessageContext,
    Ownership,
    UserContext,
)
from sementic.intent_classifier import TaskIntentClassifier
from sementic.llm import MockIntentLLMClient, MockLLMClient
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore


def _sample_event(**overrides) -> IMMessageEvent:
    base = {
        "event_id": "evt_10923841029384",
        "group_session_id": "room_dev_ecommerce_001",
        "user_context": {
            "user_id": "usr_hassan_95",
            "username": "Hassan",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": "post_7a8b9c1d",
            "parent_msg_id": "post_7a8b9c1d",
            "content": "@Jira-Helper 挂个单，@Bug-Hunter 顺便准备查下日志。",
            "mentions_registry": [
                {"entity_id": "bot_jira_123", "ownership": "OTHERS"},
                {"entity_id": "bot_bug_hunter_01", "ownership": "MY_SYSTEM"},
            ],
        },
    }
    base.update(overrides)
    return IMMessageEvent.model_validate(base)


@pytest.fixture
def history_store():
    redis = FakeRedis(decode_responses=True)
    settings = RedisSettings(max_messages=20, planner_window=10)
    return RedisHistoryStore(redis, settings)


@pytest.fixture
def handler(history_store: RedisHistoryStore) -> MessageHandler:
    return MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
        bot_registry=BotRegistry(),
    )


class FakeDirectChatRouter:
    def __init__(self) -> None:
        self.calls = []

    async def maybe_submit(self, *, event, owned_bots):
        self.calls.append((event, owned_bots))
        bot = next(bot for bot in owned_bots if bot.bot_user_id == "bot_project_assistant")
        return DirectChatSubmission(
            bot=bot,
            session_id="chat-session-1",
            task_id="chat-task-1",
            message_id="chat-message-1",
        )


class FakeDirectChatWatcher:
    def __init__(self) -> None:
        self.calls = []

    def watch(self, *, session_id: str, task_id: str, context: EgressContext) -> None:
        self.calls.append(
            {
                "session_id": session_id,
                "task_id": task_id,
                "context": context,
            }
        )


class FailingPlanner:
    async def plan(self, request):
        raise AssertionError("planner should not run for direct chat")


@pytest.mark.asyncio
async def test_append_and_trim_to_twenty_messages(history_store: RedisHistoryStore) -> None:
    group_id = "room_trim_test"

    for index in range(25):
        event = _sample_event(
            event_id=f"evt_{index}",
            group_session_id=group_id,
            message_context={
                "msg_id": f"post_{index}",
                "content": f"message {index}",
                "mentions_registry": [],
            },
        )
        await history_store.append(event)

    count = await history_store.message_count(group_id)
    recent = await history_store.get_recent(group_id, count=20)

    assert count == 20
    assert len(recent) == 20
    assert recent[0].content == "message 24"
    assert recent[-1].content == "message 5"


@pytest.mark.asyncio
async def test_get_recent_returns_latest_ten(history_store: RedisHistoryStore) -> None:
    group_id = "room_window_test"

    for index in range(15):
        event = _sample_event(
            event_id=f"evt_{index}",
            group_session_id=group_id,
            message_context={
                "msg_id": f"post_{index}",
                "content": f"message {index}",
                "mentions_registry": [],
            },
        )
        await history_store.append(event)

    recent = await history_store.get_recent(group_id, count=10)
    assert len(recent) == 10
    assert recent[0].content == "message 14"
    assert recent[-1].content == "message 5"


@pytest.mark.asyncio
async def test_handler_persists_and_plans_with_history_window(
    handler: MessageHandler,
) -> None:
    group_id = "room_dev_ecommerce_001"
    for index in range(3):
        await handler.history_store.append(
            _sample_event(
                event_id=f"evt_old_{index}",
                group_session_id=group_id,
                message_context={
                    "msg_id": f"post_old_{index}",
                    "content": f"历史消息 {index}",
                    "mentions_registry": [],
                },
            )
        )

    response = await handler.handle(_sample_event())

    assert response.skipped_planning is False
    assert response.task_intent is not None
    assert response.task_intent.needs_task is True
    assert response.plan is not None
    assert response.history_window_size == 4
    assert response.stored_message_id == "post_7a8b9c1d"
    assert len(response.plan.graph.nodes) >= 1


@pytest.mark.asyncio
async def test_handler_skips_planning_for_bot_messages(
    history_store: RedisHistoryStore,
) -> None:
    handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
    )

    response = await handler.handle(
        _sample_event(
            user_context={
                "user_id": "bot_jira_123",
                "username": "Jira-Helper",
                "is_bot": True,
                "ownership": "MY_SYSTEM",
            }
        )
    )

    assert response.skipped_planning is True
    assert response.skip_reason == "bot_message_skip_orchestration"
    assert response.plan is None
    assert await history_store.message_count("room_dev_ecommerce_001") == 1


@pytest.mark.asyncio
async def test_handler_direct_single_bot_mention_submits_native_chat(
    history_store: RedisHistoryStore,
) -> None:
    router = FakeDirectChatRouter()
    watcher = FakeDirectChatWatcher()
    handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=FailingPlanner(),
        bot_registry=BotRegistry(),
        direct_chat_router=router,
        direct_chat_completion_watcher=watcher,
    )

    response = await handler.handle(
        _sample_event(
            message_context={
                "msg_id": "abcdefghijklmnopqrstuvwxyz",
                "parent_msg_id": "zyxwvutsrqponmlkjihgfedcba",
                "content": "@Project-Assistant hello",
                "mentions_registry": [
                    {"entity_id": "bot_project_assistant", "ownership": "MY_SYSTEM"},
                ],
            }
        )
    )

    assert response.skipped_planning is True
    assert response.skip_reason == "direct_chat_submitted"
    assert response.plan is None
    assert response.direct_chat_session_id == "chat-session-1"
    assert response.direct_chat_task_id == "chat-task-1"
    assert len(router.calls) == 1
    assert len(watcher.calls) == 1
    assert watcher.calls[0]["session_id"] == "chat-session-1"
    assert watcher.calls[0]["task_id"] == "chat-task-1"
    context = watcher.calls[0]["context"]
    assert context.event_id == "evt_10923841029384"
    assert context.trace_id.startswith("tr_")
    assert context.channel_id == "room_dev_ecommerce_001"
    assert context.root_post_id == "zyxwvutsrqponmlkjihgfedcba"
    assert context.bot_user_id == "bot_project_assistant"
    assert context.maos_task_id == "chat-task-1"


@pytest.mark.asyncio
async def test_handler_skips_orchestration_for_chitchat_without_task_intent(
    handler: MessageHandler,
) -> None:
    response = await handler.handle(
        _sample_event(
            user_context={
                "user_id": "usr_hassan_95",
                "username": "Hassan",
                "is_bot": False,
                "ownership": "OTHERS",
            },
            message_context={
                "msg_id": "post_plain",
                "content": "哈哈哈，我是刘颂啊",
                "mentions_registry": [],
            },
        )
    )

    assert response.skipped_planning is True
    assert response.skip_reason == "no_task_intent"
    assert response.task_intent is not None
    assert response.task_intent.needs_task is False
    assert response.plan is None


@pytest.mark.asyncio
async def test_handler_does_not_direct_chat_for_multiple_mentions(
    handler: MessageHandler,
) -> None:
    response = await handler.handle(_sample_event())

    assert response.skipped_planning is False
    assert response.plan is not None
    assert response.direct_chat_task_id is None


async def test_handler_skips_orchestration_when_no_owned_bots(
    history_store: RedisHistoryStore,
) -> None:
    handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
        bot_registry=BotRegistry(),
    )

    response = await handler.handle(
        _sample_event(
            user_context={
                "user_id": "user_without_bots",
                "username": "guest",
                "is_bot": False,
                "ownership": "OTHERS",
            },
            message_context={
                "msg_id": "post_task",
                "content": "@Jira-Helper 挂个单",
                "mentions_registry": [
                    {"entity_id": "bot_jira_123", "ownership": "OTHERS"},
                ],
            },
        )
    )

    assert response.skipped_planning is True
    assert response.skip_reason == "no_owned_bots"
    assert response.task_intent is not None
    assert response.task_intent.needs_task is True
    assert response.plan is None


def test_bot_registry_lists_owned_bots() -> None:
    registry = BotRegistry()
    owned = registry.list_owned_bots("usr_hassan_95")
    owned_ids = {bot.bot_user_id for bot in owned}
    assert owned_ids == {"bot_project_assistant"}


def test_bot_registry_lists_usable_channel_bots() -> None:
    registry = BotRegistry()
    bots = registry.list_usable_bots("usr_hassan_95")
    bot_ids = {bot.bot_user_id for bot in bots}
    assert "bot_jira_123" in bot_ids
    assert "bot_project_assistant" in bot_ids
    assert "bot_bug_hunter_01" not in bot_ids


def test_bot_registry_applies_mention_ownership() -> None:
    registry = BotRegistry()
    bots = registry.resolve_for_event(
        sender_user_id="usr_hassan_95",
        mentions=[
            MentionRegistryItem(entity_id="bot_jira_123", ownership=Ownership.OTHERS),
            MentionRegistryItem(
                entity_id="bot_bug_hunter_01",
                ownership=Ownership.MY_SYSTEM,
            ),
        ],
    )

    by_id = {bot.bot_user_id: bot for bot in bots}
    assert by_id["bot_jira_123"].share_scope == "channel_shared"
    assert by_id["bot_bug_hunter_01"].share_scope == "private"
    assert by_id["bot_bug_hunter_01"].owner_user_id == "usr_hassan_95"
