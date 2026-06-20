from __future__ import annotations

import logging
import time

from redis.asyncio import Redis

from sementic.bot_registry import BotRegistry
from sementic.config import KafkaSettings, RedisSettings, WorkerSettings
from sementic.handler import MessageHandler
from sementic.intent_classifier import TaskIntentClassifier
from sementic.kafka_consumer import KafkaConsumerThread
from sementic.llm import MockIntentLLMClient, MockLLMClient, create_intent_llm_client, create_llm_client
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )


def build_message_handler(redis_client: Redis) -> MessageHandler:
    redis_settings = RedisSettings()
    history_store = RedisHistoryStore(redis_client, redis_settings)
    return MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=_build_intent_llm_client()),
        planner=Planner(llm=create_llm_client(provider="aliyun")),
        bot_registry=BotRegistry(),
    )


def _build_intent_llm_client():
    try:
        return create_intent_llm_client(provider="aliyun")
    except RuntimeError:
        logger.warning("intent LLM unavailable, falling back to mock intent classifier")
        return MockIntentLLMClient()


def main() -> None:
    configure_logging()
    worker_settings = WorkerSettings()
    kafka_settings = KafkaSettings()
    redis_settings = RedisSettings()

    logger.info(
        "sementic worker starting redis=%s kafka=%s topic=%s group=%s",
        redis_settings.url,
        kafka_settings.bootstrap_servers,
        kafka_settings.topic,
        kafka_settings.group_id,
    )

    redis_client = Redis.from_url(redis_settings.url, decode_responses=True)
    handler = build_message_handler(redis_client)
    consumer_thread = KafkaConsumerThread(handler, kafka_settings)
    consumer_thread.start()

    try:
        while True:
            # Main thread stays alive for future tasks (metrics, admin API, etc.).
            time.sleep(worker_settings.main_loop_interval_seconds)
    except KeyboardInterrupt:
        logger.info("sementic worker shutting down")
    finally:
        consumer_thread.stop()
        logger.info("sementic worker stopped")


if __name__ == "__main__":
    main()
