from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from sementic.config import RedisSettings
from sementic.gateway.circuit_breaker import BotCircuitBreaker
from sementic.gateway.config import KafkaSettings
from sementic.gateway.filter import StaticNoiseFilter
from sementic.gateway.models import GatewayApiResponse
from sementic.gateway.producer import AIOKafkaMessageProducer, KafkaProducer
from sementic.gateway.service import GatewayService
from sementic.im_models import IMMessageEvent


def _build_kafka_producer() -> KafkaProducer:
    return AIOKafkaMessageProducer(KafkaSettings())


@asynccontextmanager
async def _lifespan(
    app: FastAPI,
    *,
    gateway_service: GatewayService | None = None,
    redis_client: Redis | None = None,
    kafka_producer: KafkaProducer | None = None,
):
    redis_settings = RedisSettings()
    redis = redis_client or Redis.from_url(redis_settings.url, decode_responses=True)
    producer = kafka_producer or _build_kafka_producer()

    await producer.start()
    app.state.redis = redis
    app.state.kafka_producer = producer
    app.state.gateway_service = gateway_service or GatewayService(
        kafka_producer=producer,
        noise_filter=StaticNoiseFilter(),
        circuit_breaker=BotCircuitBreaker(redis),
    )
    try:
        yield
    finally:
        await producer.stop()
        if redis_client is None:
            await redis.aclose()


def create_app(
    *,
    gateway_service: GatewayService | None = None,
    redis_client: Redis | None = None,
    kafka_producer: KafkaProducer | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with _lifespan(
            app,
            gateway_service=gateway_service,
            redis_client=redis_client,
            kafka_producer=kafka_producer,
        ):
            yield

    app = FastAPI(
        title="sementic-gateway",
        description="AI semantic gateway: static filter and Kafka ingress",
        lifespan=lifespan,
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "sementic-gateway"}

    @app.post("/api/v1/im/messages", response_model=GatewayApiResponse)
    async def ingest_im_message(event: IMMessageEvent) -> GatewayApiResponse:
        service: GatewayService = app.state.gateway_service
        return await service.ingest(event)

    return app


app = create_app()
