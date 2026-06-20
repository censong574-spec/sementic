from __future__ import annotations

import threading
import time
from unittest.mock import AsyncMock, patch

import pytest
from fakeredis.aioredis import FakeRedis

from sementic.bot_registry import BotRegistry
from sementic.config import RedisSettings
from sementic.handler import MessageHandler
from sementic.intent_classifier import TaskIntentClassifier
from sementic.kafka_consumer import KafkaConsumerLoop, KafkaConsumerThread, process_kafka_message
from sementic.llm import MockIntentLLMClient, MockLLMClient
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore


@pytest.mark.asyncio
async def test_process_kafka_message_skips_orchestration_for_chitchat() -> None:
    redis = FakeRedis(decode_responses=True)
    history_store = RedisHistoryStore(redis, RedisSettings(max_messages=20, planner_window=10))
    handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
        bot_registry=BotRegistry(),
    )

    payload = {
        "event_id": "evt_no_mention",
        "group_session_id": "room_1",
        "user_context": {
            "user_id": "user_1",
            "username": "liusong",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": "post_1",
            "content": "哈哈哈，我是刘颂啊",
            "mentions_registry": [],
        },
    }

    await process_kafka_message(payload, handler)

    assert await history_store.message_count("room_1") == 1


@pytest.mark.asyncio
async def test_process_kafka_message_skips_when_speaker_has_no_owned_bots() -> None:
    redis = FakeRedis(decode_responses=True)
    history_store = RedisHistoryStore(redis, RedisSettings(max_messages=20, planner_window=10))
    handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
        bot_registry=BotRegistry(profiles={}),
    )

    payload = {
        "event_id": "evt_no_bot",
        "group_session_id": "room_1",
        "user_context": {
            "user_id": "user_1",
            "username": "alice",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": "post_1",
            "content": "@Jira-Helper 挂个单",
            "mentions_registry": [],
        },
    }

    await process_kafka_message(payload, handler)

    assert await history_store.message_count("room_1") == 1


def test_kafka_consumer_thread_starts_background_loop() -> None:
    started = threading.Event()

    async def fake_run(self) -> None:
        started.set()
        while not self._stop_event.is_set():
            time.sleep(0.05)

    redis = FakeRedis(decode_responses=True)
    history_store = RedisHistoryStore(redis, RedisSettings())
    handler = MessageHandler(
        history_store=history_store,
        planner=Planner(llm=MockLLMClient()),
    )

    with patch.object(KafkaConsumerLoop, "run", fake_run):
        thread = KafkaConsumerThread(handler)
        thread.start()
        assert started.wait(timeout=3)
        thread.stop()
