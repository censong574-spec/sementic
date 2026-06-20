from __future__ import annotations

import pytest
from fakeredis.aioredis import FakeRedis

from sementic.bot_registry import BotRegistry
from sementic.config import RedisSettings
from sementic.handler import MessageHandler
from sementic.im_models import IMMessageEvent
from sementic.intent_classifier import TaskIntentClassifier
from sementic.kafka_consumer import process_kafka_message
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
            "content": "@Jira-Helper 挂个单，@Bug-Hunter 顺便准备查下日志。",
            "mentions_registry": [
                {"entity_id": "bot_jira_123", "ownership": "OTHERS"},
                {"entity_id": "bot_bug_hunter_01", "ownership": "MY_SYSTEM"},
            ],
        },
    }
    base.update(overrides)
    return IMMessageEvent.model_validate(base)


@pytest.mark.asyncio
async def test_kafka_consumer_writes_redis_and_skips_bot_orchestration() -> None:
    redis = FakeRedis(decode_responses=True)
    history_store = RedisHistoryStore(redis, RedisSettings(max_messages=20, planner_window=10))
    handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
        bot_registry=BotRegistry(),
    )

    payload = {
        "event_id": "evt_bot",
        "group_session_id": "room_dev_ecommerce_001",
        "user_context": {
            "user_id": "bot_jira_123",
            "username": "Jira-Helper",
            "is_bot": True,
            "ownership": "MY_SYSTEM",
        },
        "message_context": {
            "msg_id": "post_bot",
            "content": "已创建 JIRA-123",
            "mentions_registry": [],
        },
        "ingested_at": "2026-06-15T10:00:00.000Z",
    }

    await process_kafka_message(payload, handler)

    assert await history_store.message_count("room_dev_ecommerce_001") == 1


@pytest.mark.asyncio
async def test_kafka_consumer_runs_planner_for_human_message() -> None:
    redis = FakeRedis(decode_responses=True)
    history_store = RedisHistoryStore(redis, RedisSettings(max_messages=20, planner_window=10))
    handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
        bot_registry=BotRegistry(),
    )

    event = _sample_event()
    payload = {
        "event_id": event.event_id,
        "group_session_id": event.group_session_id,
        "user_context": event.user_context.model_dump(mode="json"),
        "message_context": event.message_context.model_dump(mode="json"),
        "ingested_at": "2026-06-15T10:00:00.000Z",
    }

    await process_kafka_message(payload, handler)

    assert await history_store.message_count("room_dev_ecommerce_001") == 1
