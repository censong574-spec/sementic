from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any

from aiokafka import AIOKafkaConsumer

from sementic.config import KafkaSettings
from sementic.handler import MessageHandler
from sementic.im_models import IMMessageEvent

logger = logging.getLogger(__name__)


def parse_kafka_ingress_payload(payload: dict[str, Any]) -> IMMessageEvent:
    """Rebuild IMMessageEvent from Gateway Kafka envelope."""
    return IMMessageEvent.model_validate(payload)


async def process_kafka_message(
    raw_value: bytes | str | dict[str, Any],
    handler: MessageHandler,
) -> None:
    if isinstance(raw_value, bytes):
        payload = json.loads(raw_value.decode("utf-8"))
    elif isinstance(raw_value, str):
        payload = json.loads(raw_value)
    else:
        payload = raw_value

    event = parse_kafka_ingress_payload(payload)
    await handler.handle(event)
    logger.info(
        "kafka message processed event_id=%s channel=%s is_bot=%s",
        event.event_id,
        event.group_session_id,
        event.user_context.is_bot,
    )


class KafkaConsumerLoop:
    """Async Kafka consume loop; intended to run inside a background thread."""

    def __init__(
        self,
        handler: MessageHandler,
        settings: KafkaSettings | None = None,
    ) -> None:
        self.handler = handler
        self.settings = settings or KafkaSettings()
        self._stop_event = threading.Event()
        self._consumer: AIOKafkaConsumer | None = None

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self.settings.topic,
            bootstrap_servers=self.settings.bootstrap_servers,
            group_id=self.settings.group_id,
            client_id=self.settings.client_id,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        logger.info(
            "kafka consumer starting topic=%s bootstrap=%s group=%s",
            self.settings.topic,
            self.settings.bootstrap_servers,
            self.settings.group_id,
        )
        await self._consumer.start()
        logger.info("kafka consumer ready")
        try:
            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(self._consumer.getone(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                try:
                    await process_kafka_message(message.value, self.handler)
                except ValueError as exc:
                    logger.warning(
                        "kafka message skipped event_id=%s reason=%s",
                        _safe_event_id(message.value),
                        exc,
                    )
                except PermissionError as exc:
                    logger.warning(
                        "kafka message skipped event_id=%s reason=%s",
                        _safe_event_id(message.value),
                        exc,
                    )
                except Exception:
                    logger.exception(
                        "kafka message failed event_id=%s",
                        _safe_event_id(message.value),
                    )
        finally:
            await self._consumer.stop()
            self._consumer = None
            logger.info("kafka consumer stopped")


class KafkaConsumerThread:
    """Run KafkaConsumerLoop in a daemon thread so main() stays free."""

    def __init__(self, handler: MessageHandler, settings: KafkaSettings | None = None) -> None:
        self._loop = KafkaConsumerLoop(handler, settings)
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run,
            name="sementic-kafka-consumer",
            daemon=True,
        )
        self._thread.start()
        logger.info("kafka consumer thread started")

    def stop(self, *, timeout_seconds: float = 10.0) -> None:
        self._loop.request_stop()
        if self._thread is None:
            return
        self._thread.join(timeout=timeout_seconds)
        if self._thread.is_alive():
            logger.warning("kafka consumer thread did not stop within %.1fs", timeout_seconds)
        else:
            logger.info("kafka consumer thread joined")
        self._thread = None

    def _run(self) -> None:
        try:
            asyncio.run(self._loop.run())
        except Exception:
            logger.exception("kafka consumer thread crashed")


def _safe_event_id(raw_value: bytes | str | dict[str, Any]) -> str:
    try:
        if isinstance(raw_value, bytes):
            payload = json.loads(raw_value.decode("utf-8"))
        elif isinstance(raw_value, str):
            payload = json.loads(raw_value)
        else:
            payload = raw_value
        return str(payload.get("event_id", "unknown"))
    except Exception:
        return "unknown"
