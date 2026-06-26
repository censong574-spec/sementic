from __future__ import annotations

import asyncio
import logging
from typing import Any

from sementic.im_egress.client import MattermostPostClient
from sementic.im_egress.models import EgressContext, EgressKind
from sementic.redis_history import RedisHistoryStore

logger = logging.getLogger(__name__)


class ImEgressPublisher:
    def __init__(
        self,
        *,
        client: MattermostPostClient | None = None,
        history_store: RedisHistoryStore | None = None,
    ) -> None:
        self.client = client or MattermostPostClient()
        self.history_store = history_store

    async def publish(
        self,
        context: EgressContext,
        *,
        kind: EgressKind,
        text: str,
        node_id: str = "",
    ) -> dict[str, Any] | None:
        text = (text or "").strip()
        if not text:
            logger.info("skip empty egress kind=%s event_id=%s", kind.value, context.event_id)
            return None

        try:
            post = self.client.post_reply(
                bot_user_id=context.bot_user_id,
                channel_id=context.channel_id,
                root_post_id=context.root_post_id,
                message=text,
                trace_id=context.trace_id,
            )
        except Exception:
            logger.exception(
                "mattermost egress failed kind=%s event_id=%s bot=%s",
                kind.value,
                context.event_id,
                context.bot_user_id,
            )
            return None

        if post and self.history_store is not None:
            msg_id = str(post.get("id") or f"egress:{context.event_id}:{kind.value}")
            if kind == EgressKind.NODE and node_id:
                msg_id = str(post.get("id") or f"egress:{context.event_id}:node:{node_id}")
            await self._append_bot_reply_to_history(
                channel_id=context.channel_id,
                bot_user_id=context.bot_user_id,
                bot_username=context.bot_username,
                content=text,
                msg_id=msg_id,
                event_id=context.event_id,
                kind=kind.value,
            )
        return post

    async def _append_bot_reply_to_history(
        self,
        *,
        channel_id: str,
        bot_user_id: str,
        bot_username: str,
        content: str,
        msg_id: str,
        event_id: str,
        kind: str,
    ) -> None:
        assert self.history_store is not None
        kwargs = {
            "group_session_id": channel_id,
            "bot_user_id": bot_user_id,
            "bot_username": bot_username,
            "content": content,
            "msg_id": msg_id,
        }
        try:
            await self.history_store.append_bot_reply(**kwargs)
        except Exception:
            logger.warning(
                "async redis append failed after mattermost post; retrying sync "
                "event_id=%s kind=%s bot=%s",
                event_id,
                kind,
                bot_user_id,
                exc_info=True,
            )
            try:
                await asyncio.to_thread(
                    self.history_store.append_bot_reply_blocking,
                    **kwargs,
                )
            except Exception:
                logger.warning(
                    "sync redis append failed after mattermost post "
                    "event_id=%s kind=%s bot=%s",
                    event_id,
                    kind,
                    bot_user_id,
                    exc_info=True,
                )

    async def publish_node(
        self,
        context: EgressContext,
        node_id: str,
        text: str,
    ) -> dict[str, Any] | None:
        return await self.publish(context, kind=EgressKind.NODE, text=text, node_id=node_id)

    async def publish_final(self, context: EgressContext, text: str) -> dict[str, Any] | None:
        return await self.publish(context, kind=EgressKind.FINAL, text=text)

    async def publish_error(self, context: EgressContext, text: str) -> dict[str, Any] | None:
        return await self.publish(context, kind=EgressKind.ERROR, text=text)
