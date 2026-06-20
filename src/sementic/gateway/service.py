from __future__ import annotations

from sementic.gateway.circuit_breaker import BotCircuitBreaker
from sementic.gateway.filter import StaticNoiseFilter
from sementic.gateway.models import GatewayAction, GatewayApiResponse, build_gateway_response
from sementic.gateway.producer import KafkaProducer
from sementic.im_models import IMMessageEvent

OFFLINE_REPLY = (
    "🤖 系统提示：当前机器人的端侧常驻通道已断开，无法承载任务，正在等待重连。"
)


class GatewayService:
    def __init__(
        self,
        *,
        kafka_producer: KafkaProducer,
        noise_filter: StaticNoiseFilter | None = None,
        circuit_breaker: BotCircuitBreaker | None = None,
    ) -> None:
        self.kafka_producer = kafka_producer
        self.noise_filter = noise_filter or StaticNoiseFilter()
        self.circuit_breaker = circuit_breaker

    async def ingest(self, event: IMMessageEvent) -> GatewayApiResponse:
        is_noise, _reason = self.noise_filter.is_noise(event.message_context.content)
        if is_noise:
            return build_gateway_response(
                event.event_id,
                GatewayAction.FILTERED,
                message="Message filtered by gateway.",
            )

        if self.circuit_breaker is not None:
            offline_bots = await self.circuit_breaker.find_offline_mentioned_bots(event)
            if offline_bots:
                return build_gateway_response(
                    event.event_id,
                    GatewayAction.BLOCKED,
                    message=OFFLINE_REPLY,
                )

        await self.kafka_producer.publish(event)

        return build_gateway_response(event.event_id, GatewayAction.ASYNC_PROCESSING)
