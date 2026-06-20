from __future__ import annotations

from pydantic import BaseModel

from sementic.bot_registry import BotRegistry
from sementic.bot_service import BotServiceClient
from sementic.im_models import IMMessageEvent, MentionRegistryItem
from sementic.intent_classifier import TaskIntentClassifier
from sementic.models import BotProfile, ChatMessage, PlannerRequest, TaskIntentDecision
from sementic.task_graph import TaskGraphPlan
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore, StoredChatMessage


class PlanMessageResponse(BaseModel):
    event_id: str
    group_session_id: str
    stored_message_id: str
    history_window_size: int
    skipped_planning: bool = False
    skip_reason: str | None = None
    task_intent: TaskIntentDecision | None = None
    plan: TaskGraphPlan | None = None


class MessageHandler:
    def __init__(
        self,
        *,
        history_store: RedisHistoryStore,
        planner: Planner,
        intent_classifier: TaskIntentClassifier | None = None,
        bot_registry: BotRegistry | None = None,
        bot_service: BotServiceClient | None = None,
    ) -> None:
        self.history_store = history_store
        self.planner = planner
        self.intent_classifier = intent_classifier or TaskIntentClassifier()
        self.bot_registry = bot_registry or BotRegistry()
        self.bot_service = bot_service or BotServiceClient()

    async def handle(self, event: IMMessageEvent) -> PlanMessageResponse:
        stored = await self.history_store.append(event)

        if event.user_context.is_bot:
            return PlanMessageResponse(
                event_id=event.event_id,
                group_session_id=event.group_session_id,
                stored_message_id=stored.msg_id,
                history_window_size=await self.history_store.message_count(
                    event.group_session_id
                ),
                skipped_planning=True,
                skip_reason="bot_message_skip_orchestration",
            )

        recent = await self.history_store.get_recent(event.group_session_id)
        history_messages = self._history_for_prompt(event, recent)

        task_intent = await self.intent_classifier.classify(
            channel_id=event.group_session_id,
            sender_display_name=event.user_context.username,
            recent_messages=history_messages,
            current_message=event.message_context.content,
        )
        if not task_intent.needs_task:
            return PlanMessageResponse(
                event_id=event.event_id,
                group_session_id=event.group_session_id,
                stored_message_id=stored.msg_id,
                history_window_size=len(recent),
                skipped_planning=True,
                skip_reason="no_task_intent",
                task_intent=task_intent,
            )

        owned_bots = await self._resolve_owned_bots(
            sender_user_id=event.user_context.user_id,
            mentions=event.message_context.mentions_registry,
        )
        if not owned_bots:
            return PlanMessageResponse(
                event_id=event.event_id,
                group_session_id=event.group_session_id,
                stored_message_id=stored.msg_id,
                history_window_size=len(recent),
                skipped_planning=True,
                skip_reason="no_owned_bots",
                task_intent=task_intent,
            )

        planner_request = self._build_planner_request(
            event,
            recent,
            available_bots=owned_bots,
        )
        plan = await self.planner.plan(planner_request)

        return PlanMessageResponse(
            event_id=event.event_id,
            group_session_id=event.group_session_id,
            stored_message_id=stored.msg_id,
            history_window_size=len(recent),
            task_intent=task_intent,
            plan=plan,
        )

    @staticmethod
    def _history_for_prompt(
        event: IMMessageEvent,
        recent: list[StoredChatMessage],
    ) -> list[ChatMessage]:
        current_msg_id = event.message_context.msg_id
        history = [msg for msg in recent if msg.msg_id != current_msg_id]
        history.reverse()
        return [
            ChatMessage(
                sender=msg.sender_name,
                text=msg.content,
                timestamp=msg.timestamp,
                message_id=msg.msg_id,
            )
            for msg in history
        ]

    def _build_planner_request(
        self,
        event: IMMessageEvent,
        recent: list[StoredChatMessage],
        *,
        available_bots: list[BotProfile],
    ) -> PlannerRequest:
        history_messages = self._history_for_prompt(event, recent)

        return PlannerRequest(
            channel_id=event.group_session_id,
            sender_user_id=event.user_context.user_id,
            sender_display_name=event.user_context.username,
            recent_messages=history_messages,
            mentioned_bot_ids=[
                mention.entity_id for mention in event.message_context.mentions_registry
            ],
            available_bots=available_bots,
            current_message=event.message_context.content,
        )

    async def _resolve_owned_bots(
        self,
        *,
        sender_user_id: str,
        mentions: list[MentionRegistryItem],
    ) -> list[BotProfile]:
        if self.bot_service.enabled:
            return await self.bot_service.resolve_owned_bots_for_orchestration(
                sender_user_id=sender_user_id,
                mentions=mentions,
            )
        return self.bot_registry.resolve_owned_bots_for_orchestration(
            sender_user_id=sender_user_id,
            mentions=mentions,
        )
