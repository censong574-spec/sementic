from __future__ import annotations

from sementic.im_models import IMMessageEvent


class BotCircuitBreaker:
    def __init__(self, redis_client, *, key_prefix: str = "status") -> None:
        self.redis = redis_client
        self.key_prefix = key_prefix

    def _key(self, bot_user_id: str) -> str:
        return f"{self.key_prefix}:{bot_user_id}"

    async def find_offline_mentioned_bots(self, event: IMMessageEvent) -> list[str]:
        offline: list[str] = []
        for mention in event.message_context.mentions_registry:
            status = await self.redis.get(self._key(mention.entity_id))
            if status == "offline":
                offline.append(mention.entity_id)
        return offline
