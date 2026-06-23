from __future__ import annotations

import logging
import time

from redis.asyncio import Redis

from sementic.bot_registry import BotRegistry
from sementic.config import ImEgressSettings, KafkaSettings, MaosSettings, RedisSettings, WorkerSettings
from sementic.execution.maos_executor import MaosExecutor
from sementic.handler import MessageHandler
from sementic.im_egress.client import MattermostPostClient
from sementic.im_egress.publisher import ImEgressPublisher
from sementic.im_egress.watcher import MaosCompletionWatcher
from sementic.intent_classifier import TaskIntentClassifier
from sementic.kafka_consumer import KafkaConsumerThread
from sementic.llm import MockIntentLLMClient, create_intent_llm_client, create_llm_client
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )


def _build_intent_llm_client():
    try:
        return create_intent_llm_client(provider="aliyun")
    except RuntimeError:
        logger.warning("intent LLM unavailable, falling back to mock intent classifier")
        return MockIntentLLMClient()


def build_maos_executor() -> MaosExecutor:
    executor = MaosExecutor(MaosSettings())
    executor.start()
    return executor


def build_message_handler(
    redis_client: Redis,
    *,
    maos_executor: MaosExecutor | None = None,
) -> MessageHandler:
    redis_settings = RedisSettings()
    history_store = RedisHistoryStore(redis_client, redis_settings)
    egress_settings = ImEgressSettings()
    mm_client = MattermostPostClient(egress_settings)
    im_egress = (
        ImEgressPublisher(client=mm_client, history_store=history_store)
        if mm_client.enabled
        else None
    )
    maos_completion_watcher = None
    if maos_executor is not None and im_egress is not None:
        maos_completion_watcher = MaosCompletionWatcher(
            maos_executor=maos_executor,
            egress_publisher=im_egress,
            settings=egress_settings,
        )
    return MessageHandler(
        history_store=history_store,
        intent_classifier=TaskIntentClassifier(llm=_build_intent_llm_client()),
        planner=Planner(llm=create_llm_client(provider="aliyun")),
        bot_registry=BotRegistry(),
        maos_executor=maos_executor,
        im_egress=im_egress,
        maos_completion_watcher=maos_completion_watcher,
    )


def main() -> None:
    configure_logging()
    worker_settings = WorkerSettings()
    kafka_settings = KafkaSettings()
    redis_settings = RedisSettings()
    maos_settings = MaosSettings()

    logger.info(
        "sementic worker starting redis=%s kafka=%s topic=%s group=%s maos_temporal=%s mm_egress=%s",
        redis_settings.url,
        kafka_settings.bootstrap_servers,
        kafka_settings.topic,
        kafka_settings.group_id,
        maos_settings.temporal_address,
        ImEgressSettings().url or "(disabled)",
    )

    maos_executor = build_maos_executor()
    info = maos_executor.runtime_info()
    logger.info("MAOS runtime info=%s", info)

    redis_client = Redis.from_url(redis_settings.url, decode_responses=True)
    handler = build_message_handler(redis_client, maos_executor=maos_executor)
    consumer_thread = KafkaConsumerThread(handler, kafka_settings)
    consumer_thread.start()

    try:
        while True:
            time.sleep(worker_settings.main_loop_interval_seconds)
    except KeyboardInterrupt:
        logger.info("sementic worker shutting down")
    finally:
        consumer_thread.stop()
        logger.info("sementic worker stopped")


if __name__ == "__main__":
    main()
