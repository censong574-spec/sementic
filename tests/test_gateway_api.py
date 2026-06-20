from __future__ import annotations

import pytest
from fakeredis.aioredis import FakeRedis
from fastapi.testclient import TestClient

from sementic.gateway.api import create_app
from sementic.gateway.circuit_breaker import BotCircuitBreaker
from sementic.gateway.filter import StaticNoiseFilter
from sementic.gateway.producer import InMemoryKafkaProducer
from sementic.gateway.service import GatewayService


@pytest.fixture
def client():
    redis = FakeRedis(decode_responses=True)
    producer = InMemoryKafkaProducer()
    service = GatewayService(
        kafka_producer=producer,
        noise_filter=StaticNoiseFilter(),
        circuit_breaker=BotCircuitBreaker(redis),
    )
    app = create_app(
        gateway_service=service,
        redis_client=redis,
        kafka_producer=producer,
    )

    with TestClient(app) as test_client:
        yield test_client, producer


def test_gateway_health(client) -> None:
    test_client, _producer = client
    response = test_client.get("/health")
    assert response.status_code == 200
    assert response.json()["service"] == "sementic-gateway"


def test_gateway_ingress_accepts_null_mentions_registry(client) -> None:
    test_client, producer = client
    payload = {
        "event_id": "evt_null_mentions",
        "group_session_id": "room_dev_ecommerce_001",
        "user_context": {
            "user_id": "usr_hassan_95",
            "username": "Hassan",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": "post_null_mentions",
            "content": "message without bot mentions",
            "mentions_registry": None,
        },
    }

    response = test_client.post("/api/v1/im/messages", json=payload)
    assert response.status_code == 200
    assert response.json()["data"]["action_taken"] == "ASYNC_PROCESSING"
    assert len(producer.messages) == 1


def test_gateway_ingress_accepts_missing_mentions_registry_field(client) -> None:
    test_client, producer = client
    payload = {
        "event_id": "evt_missing_mentions",
        "group_session_id": "room_dev_ecommerce_001",
        "user_context": {
            "user_id": "usr_hassan_95",
            "username": "Hassan",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": "post_missing_mentions",
            "content": "message without mentions field",
        },
    }

    response = test_client.post("/api/v1/im/messages", json=payload)
    assert response.status_code == 200
    assert response.json()["data"]["action_taken"] == "ASYNC_PROCESSING"
    assert len(producer.messages) == 1


def test_gateway_ingress_endpoint(client) -> None:
    test_client, producer = client
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
            "content": "@Jira-Helper 挂个单",
            "mentions_registry": [
                {"entity_id": "bot_jira_123", "ownership": "OTHERS"},
            ],
        },
    }

    response = test_client.post("/api/v1/im/messages", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["action_taken"] == "ASYNC_PROCESSING"
    assert len(producer.messages) == 1
