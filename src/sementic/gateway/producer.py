from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from aiokafka import AIOKafkaProducer

from sementic.gateway.config import KafkaSettings
from sementic.im_models import IMMessageEvent


@dataclass(frozen=True)
class KafkaPublishResult:
    topic: str
    partition: int
    offset: int


class KafkaProducer(ABC):
    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def publish(self, event: IMMessageEvent) -> KafkaPublishResult:
        raise NotImplementedError


class AIOKafkaMessageProducer(KafkaProducer):
    def __init__(self, settings: KafkaSettings | None = None) -> None:
        self.settings = settings or KafkaSettings()
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.bootstrap_servers,
            client_id=self.settings.client_id,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def publish(self, event: IMMessageEvent) -> KafkaPublishResult:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")

        payload = _build_kafka_payload(event)
        metadata = await self._producer.send_and_wait(
            self.settings.topic,
            value=payload,
            key=event.group_session_id,
        )
        return KafkaPublishResult(
            topic=metadata.topic,
            partition=metadata.partition,
            offset=metadata.offset,
        )


class InMemoryKafkaProducer(KafkaProducer):
    """Test double that records published messages in memory."""

    def __init__(self, settings: KafkaSettings | None = None) -> None:
        self.settings = settings or KafkaSettings()
        self.messages: list[dict[str, Any]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish(self, event: IMMessageEvent) -> KafkaPublishResult:
        payload = _build_kafka_payload(event)
        partition = abs(hash(event.group_session_id)) % 8
        offset = len(self.messages)
        self.messages.append(
            {
                "topic": self.settings.topic,
                "partition": partition,
                "offset": offset,
                "key": event.group_session_id,
                "value": payload,
            }
        )
        return KafkaPublishResult(
            topic=self.settings.topic,
            partition=partition,
            offset=offset,
        )


def _build_kafka_payload(event: IMMessageEvent) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_id": event.event_id,
        "trace_id": event.trace_id,
        "group_session_id": event.group_session_id,
        "user_context": event.user_context.model_dump(mode="json"),
        "message_context": event.message_context.model_dump(mode="json"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    if event.workspace_id:
        payload["workspace_id"] = event.workspace_id
    if event.multica_token:
        payload["multica_token"] = event.multica_token
    return payload
