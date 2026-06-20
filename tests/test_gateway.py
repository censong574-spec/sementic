from __future__ import annotations

import pytest
from fakeredis.aioredis import FakeRedis

from sementic.config import RedisSettings
from sementic.gateway.circuit_breaker import BotCircuitBreaker
from sementic.gateway.filter import StaticNoiseFilter
from sementic.gateway.models import GatewayAction
from sementic.gateway.producer import InMemoryKafkaProducer
from sementic.gateway.service import GatewayService
from sementic.im_models import IMMessageEvent


def _event(**overrides) -> IMMessageEvent:
    base = {
        "event_id": "evt_001",
        "group_session_id": "room_dev_ecommerce_001",
        "user_context": {
            "user_id": "usr_hassan_95",
            "username": "Hassan",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": "post_001",
            "content": "@Jira-Helper 挂个单",
            "mentions_registry": [
                {"entity_id": "bot_jira_123", "ownership": "OTHERS"},
            ],
        },
    }
    base.update(overrides)
    return IMMessageEvent.model_validate(base)


@pytest.fixture
def gateway_service():
    redis = FakeRedis(decode_responses=True)
    producer = InMemoryKafkaProducer()
    service = GatewayService(
        kafka_producer=producer,
        noise_filter=StaticNoiseFilter(),
        circuit_breaker=BotCircuitBreaker(redis),
    )
    return service, producer, redis


@pytest.mark.asyncio
async def test_gateway_filters_noise_without_kafka(gateway_service) -> None:
    service, producer, _redis = gateway_service
    response = await service.ingest(_event(message_context={"msg_id": "p1", "content": "哈哈"}))

    assert response.data.action_taken == GatewayAction.FILTERED
    assert len(producer.messages) == 0


@pytest.mark.asyncio
async def test_gateway_queues_valid_message_to_kafka(gateway_service) -> None:
    service, producer, _redis = gateway_service
    response = await service.ingest(_event())

    assert response.data.action_taken == GatewayAction.ASYNC_PROCESSING
    assert len(producer.messages) == 1
    assert producer.messages[0]["key"] == "room_dev_ecommerce_001"


@pytest.mark.asyncio
async def test_gateway_rejects_offline_bot_without_kafka(gateway_service) -> None:
    service, producer, redis = gateway_service
    await redis.set("status:bot_jira_123", "offline")

    response = await service.ingest(_event())

    assert response.data.action_taken == GatewayAction.BLOCKED
    assert len(producer.messages) == 0
    assert "断开" in response.message


@pytest.mark.asyncio
async def test_gateway_queues_bot_message_to_kafka(gateway_service) -> None:
    service, producer, _redis = gateway_service
    response = await service.ingest(
        _event(
            user_context={
                "user_id": "bot_jira_123",
                "username": "Jira-Helper",
                "is_bot": True,
                "ownership": "MY_SYSTEM",
            },
            message_context={
                "msg_id": "post_bot",
                "content": "已创建 JIRA-123",
                "mentions_registry": [],
            },
        )
    )

    assert response.data.action_taken == GatewayAction.ASYNC_PROCESSING
    assert len(producer.messages) == 1
