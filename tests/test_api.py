from __future__ import annotations

import pytest
from fakeredis.aioredis import FakeRedis
from fastapi.testclient import TestClient

from sementic.api import create_app
from sementic.config import RedisSettings
from sementic.handler import MessageHandler
from sementic.intent_classifier import TaskIntentClassifier
from sementic.llm import MockIntentLLMClient, MockLLMClient
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore


@pytest.fixture
def client():
    redis = FakeRedis(decode_responses=True)
    history_store = RedisHistoryStore(redis, RedisSettings())
    message_handler = MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
    )
    app = create_app(message_handler=message_handler, redis_client=redis)

    with TestClient(app) as test_client:
        yield test_client


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ingest_im_message(client: TestClient) -> None:
    payload = {
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

    response = client.post("/api/v1/im/messages", json=payload)
    assert response.status_code == 200

    body = response.json()
    assert body["event_id"] == payload["event_id"]
    assert body["group_session_id"] == payload["group_session_id"]
    assert body["skipped_planning"] is False
    assert body["plan"] is not None
    assert body["plan"]["graph"]["graph_type"] == "control_flow"
    assert len(body["plan"]["graph"]["nodes"]) >= 1
