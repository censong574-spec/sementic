from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass

from sementic.config import ImEgressSettings
from sementic.im_egress.models import EgressContext
from sementic.im_egress.publisher import ImEgressPublisher
from sementic.im_models import IMMessageEvent
from sementic.models import BotProfile
from sementic.multica_chat_client import MulticaChatClient

logger = logging.getLogger(__name__)

DIRECT_CHAT_SESSION_TTL_SECONDS = 60 * 60 * 24 * 30


@dataclass(frozen=True)
class DirectChatSubmission:
    bot: BotProfile
    session_id: str
    task_id: str
    message_id: str


class DirectChatSessionStore:
    def __init__(
        self,
        redis_client,
        *,
        ttl_seconds: int = DIRECT_CHAT_SESSION_TTL_SECONDS,
    ) -> None:
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds

    def key(self, *, group_session_id: str, bot_user_id: str) -> str:
        return f"sementic:multica_chat_session:{group_session_id}:{bot_user_id}"

    async def get(self, *, group_session_id: str, bot_user_id: str) -> str:
        key = self.key(group_session_id=group_session_id, bot_user_id=bot_user_id)
        value = await self.redis.get(key)
        session_id = value.decode() if isinstance(value, bytes) else value
        session_id = str(session_id or "").strip()
        logger.info(
            "direct chat session lookup group=%s bot=%s hit=%s",
            group_session_id,
            bot_user_id,
            bool(session_id),
        )
        return session_id

    async def set(self, *, group_session_id: str, bot_user_id: str, session_id: str) -> None:
        key = self.key(group_session_id=group_session_id, bot_user_id=bot_user_id)
        ttl = max(int(self.ttl_seconds), 0)
        if ttl:
            await self.redis.set(key, session_id, ex=ttl)
        else:
            await self.redis.set(key, session_id)
        logger.info(
            "direct chat session stored group=%s bot=%s session_id=%s ttl=%s",
            group_session_id,
            bot_user_id,
            session_id,
            ttl or "none",
        )


class DirectChatRouter:
    def __init__(
        self,
        *,
        client: MulticaChatClient,
        session_store: DirectChatSessionStore,
    ) -> None:
        self.client = client
        self.session_store = session_store

    async def maybe_submit(
        self,
        *,
        event: IMMessageEvent,
        owned_bots: list[BotProfile],
    ) -> DirectChatSubmission | None:
        bot = direct_mention_target(event, owned_bots)
        if bot is None:
            return None
        if not bot.multica_agent_id:
            logger.info(
                "direct chat skipped: bot has no multica agent id event_id=%s bot=%s",
                event.event_id,
                bot.bot_user_id,
            )
            return None
        if event.workspace_id and bot.multica_agent_id == event.workspace_id:
            logger.info(
                "direct chat skipped: bot looks like workspace fallback event_id=%s bot=%s workspace_id=%s",
                event.event_id,
                bot.bot_user_id,
                event.workspace_id,
            )
            return None
        if not bot.is_online:
            logger.info(
                "direct chat skipped: bot offline event_id=%s bot=%s",
                event.event_id,
                bot.bot_user_id,
            )
            return None

        session_id = await self.session_store.get(
            group_session_id=event.group_session_id,
            bot_user_id=bot.bot_user_id,
        )
        if not session_id:
            session_id = await self._create_and_store_session(event, bot)
        else:
            await self.session_store.set(
                group_session_id=event.group_session_id,
                bot_user_id=bot.bot_user_id,
                session_id=session_id,
            )

        try:
            sent = await self.client.send_message(
                session_id=session_id,
                content=event.message_context.content,
            )
        except Exception:
            logger.exception(
                "direct chat send failed; recreating session once event_id=%s group=%s bot=%s old_session_id=%s",
                event.event_id,
                event.group_session_id,
                bot.bot_user_id,
                session_id,
            )
            session_id = await self._create_and_store_session(event, bot)
            sent = await self.client.send_message(
                session_id=session_id,
                content=event.message_context.content,
            )

        task_id = str(sent.get("task_id") or "").strip()
        message_id = str(sent.get("message_id") or "").strip()
        if not task_id:
            raise RuntimeError(f"Multica chat send returned no task_id: {sent!r}")

        logger.info(
            "direct chat submitted event_id=%s group=%s bot=%s agent_id=%s session_id=%s task_id=%s message_id=%s",
            event.event_id,
            event.group_session_id,
            bot.bot_user_id,
            bot.multica_agent_id,
            session_id,
            task_id,
            message_id,
        )
        return DirectChatSubmission(
            bot=bot,
            session_id=session_id,
            task_id=task_id,
            message_id=message_id,
        )

    async def _create_and_store_session(self, event: IMMessageEvent, bot: BotProfile) -> str:
        session = await self.client.create_session(
            agent_id=str(bot.multica_agent_id),
            title=f"Mattermost {event.group_session_id}",
        )
        session_id = str(session.get("id") or "").strip()
        if not session_id:
            raise RuntimeError(f"Multica chat create session returned no id: {session!r}")
        await self.session_store.set(
            group_session_id=event.group_session_id,
            bot_user_id=bot.bot_user_id,
            session_id=session_id,
        )
        return session_id


class DirectChatCompletionWatcher:
    """Poll native chat completion and publish the assistant row to Mattermost."""

    def __init__(
        self,
        *,
        client: MulticaChatClient,
        egress_publisher: ImEgressPublisher,
        settings: ImEgressSettings | None = None,
    ) -> None:
        self.client = client
        self.egress_publisher = egress_publisher
        self.settings = settings or ImEgressSettings()

    def watch(self, *, session_id: str, task_id: str, context: EgressContext) -> None:
        thread = threading.Thread(
            target=self._run_watch,
            name=f"direct-chat-egress-{task_id[:24]}",
            args=(session_id, task_id, context),
            daemon=True,
        )
        thread.start()

    def _run_watch(self, session_id: str, task_id: str, context: EgressContext) -> None:
        asyncio.run(self._watch_async(session_id=session_id, task_id=task_id, context=context))

    async def _watch_async(self, *, session_id: str, task_id: str, context: EgressContext) -> None:
        poll_seconds = self.settings.poll_interval_seconds
        deadline = time.time() + self.settings.completion_timeout_seconds
        logger.info(
            "direct chat egress watch started task_id=%s session_id=%s event_id=%s bot=%s",
            task_id,
            session_id,
            context.event_id,
            context.bot_user_id,
        )

        while time.time() < deadline:
            try:
                reply = await self._assistant_reply_for_task(session_id=session_id, task_id=task_id)
                if reply:
                    post = await self.egress_publisher.publish_final(context, reply)
                    if post is not None:
                        logger.info(
                            "direct chat egress watch finished task_id=%s session_id=%s event_id=%s",
                            task_id,
                            session_id,
                            context.event_id,
                        )
                        return

                pending = await self.client.get_pending_task(session_id=session_id)
                pending_task_id = str(pending.get("task_id") or "").strip()
                if not pending_task_id:
                    logger.warning(
                        "direct chat completed without assistant reply task_id=%s session_id=%s event_id=%s",
                        task_id,
                        session_id,
                        context.event_id,
                    )
                    await self.egress_publisher.publish_error(
                        context,
                        "Task finished, but no Multica chat assistant reply was found.",
                    )
                    return
            except Exception:
                logger.exception(
                    "direct chat egress poll failed task_id=%s session_id=%s event_id=%s",
                    task_id,
                    session_id,
                    context.event_id,
                )

            await asyncio.sleep(poll_seconds)

        logger.warning(
            "direct chat egress watch timeout task_id=%s session_id=%s event_id=%s",
            task_id,
            session_id,
            context.event_id,
        )
        await self.egress_publisher.publish_error(
            context,
            "Multica chat timed out. Please check the task status later.",
        )

    async def _assistant_reply_for_task(self, *, session_id: str, task_id: str) -> str:
        messages = await self.client.list_messages(session_id=session_id)
        for message in reversed(messages):
            if str(message.get("role") or "") != "assistant":
                continue
            message_task_id = message.get("task_id")
            if message_task_id is not None and str(message_task_id) != task_id:
                continue
            content = str(message.get("content") or "").strip()
            if content:
                return content
        return ""


def direct_mention_target(
    event: IMMessageEvent,
    owned_bots: list[BotProfile],
) -> BotProfile | None:
    mention_ids = [
        str(mention.entity_id).strip()
        for mention in event.message_context.mentions_registry
        if str(mention.entity_id).strip()
    ]
    unique_mentions = list(dict.fromkeys(mention_ids))
    if len(unique_mentions) != 1:
        logger.info(
            "direct chat skipped: mention count is not one event_id=%s mentions=%s",
            event.event_id,
            unique_mentions,
        )
        return None

    bot_index = {bot.bot_user_id: bot for bot in owned_bots}
    bot = bot_index.get(unique_mentions[0])
    if bot is None:
        logger.info(
            "direct chat skipped: mentioned bot not in owned bots event_id=%s bot=%s owned=%s",
            event.event_id,
            unique_mentions[0],
            list(bot_index),
        )
        return None
    return bot
