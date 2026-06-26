from __future__ import annotations

import json
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from sementic.config import RedisSettings
from sementic.im_models import IMMessageEvent


class StoredChatMessage(BaseModel):
    msg_id: str
    sender_id: str
    sender_name: str
    content: str
    is_bot: bool = False
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str) -> StoredChatMessage:
        return cls.model_validate_json(raw)

    @classmethod
    def from_im_event(cls, event: IMMessageEvent) -> StoredChatMessage:
        return cls(
            msg_id=event.message_context.msg_id,
            sender_id=event.user_context.user_id,
            sender_name=event.user_context.username,
            content=event.message_context.content,
            is_bot=event.user_context.is_bot,
        )


class RedisHistoryStore:
    def __init__(
        self,
        redis_client,
        settings: RedisSettings | None = None,
    ) -> None:
        self.redis = redis_client
        self.settings = settings or RedisSettings()

    def _key(self, group_session_id: str) -> str:
        return f"{self.settings.key_prefix}:{group_session_id}"

    async def append(self, event: IMMessageEvent) -> StoredChatMessage:
        message = StoredChatMessage.from_im_event(event)
        key = self._key(event.group_session_id)

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.lpush(key, message.to_json())
            pipe.ltrim(key, 0, self.settings.max_messages - 1)
            await pipe.execute()

        return message

    async def append_bot_reply(
        self,
        *,
        group_session_id: str,
        bot_user_id: str,
        bot_username: str,
        content: str,
        msg_id: str | None = None,
    ) -> StoredChatMessage:
        message = self._bot_reply_message(
            group_session_id=group_session_id,
            bot_user_id=bot_user_id,
            bot_username=bot_username,
            content=content,
            msg_id=msg_id,
        )
        key = self._key(group_session_id)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.lpush(key, message.to_json())
            pipe.ltrim(key, 0, self.settings.max_messages - 1)
            await pipe.execute()
        return message

    def append_bot_reply_blocking(
        self,
        *,
        group_session_id: str,
        bot_user_id: str,
        bot_username: str,
        content: str,
        msg_id: str | None = None,
    ) -> StoredChatMessage:
        """Sync Redis write for egress threads that run outside the main asyncio loop."""
        import redis as sync_redis

        message = self._bot_reply_message(
            group_session_id=group_session_id,
            bot_user_id=bot_user_id,
            bot_username=bot_username,
            content=content,
            msg_id=msg_id,
        )
        key = self._key(group_session_id)
        client = sync_redis.Redis.from_url(self.settings.url, decode_responses=True)
        try:
            with client.pipeline(transaction=True) as pipe:
                pipe.lpush(key, message.to_json())
                pipe.ltrim(key, 0, self.settings.max_messages - 1)
                pipe.execute()
        finally:
            client.close()
        return message

    @staticmethod
    def _bot_reply_message(
        *,
        group_session_id: str,
        bot_user_id: str,
        bot_username: str,
        content: str,
        msg_id: str | None,
    ) -> StoredChatMessage:
        _ = group_session_id
        return StoredChatMessage(
            msg_id=msg_id or f"egress:{bot_user_id}:{int(datetime.now(timezone.utc).timestamp() * 1000)}",
            sender_id=bot_user_id,
            sender_name=bot_username,
            content=content,
            is_bot=True,
        )

    async def get_recent(
        self,
        group_session_id: str,
        *,
        count: int | None = None,
    ) -> list[StoredChatMessage]:
        limit = count or self.settings.planner_window
        key = self._key(group_session_id)
        raw_messages = await self.redis.lrange(key, 0, max(limit - 1, 0))

        messages: list[StoredChatMessage] = []
        for raw in raw_messages:
            payload = raw.decode() if isinstance(raw, bytes) else raw
            messages.append(StoredChatMessage.from_json(payload))
        return messages

    async def message_count(self, group_session_id: str) -> int:
        key = self._key(group_session_id)
        return int(await self.redis.llen(key))
