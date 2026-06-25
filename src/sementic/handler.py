from __future__ import annotations

import asyncio
import logging

from pydantic import BaseModel, Field

from sementic.bot_registry import BotRegistry
from sementic.bot_service import BotServiceClient
from sementic.execution.maos_executor import MaosExecutor
from sementic.execution.plan_enricher import enrich_plan_for_execution
from sementic.im_egress.context import build_egress_context
from sementic.im_egress.publisher import ImEgressPublisher
from sementic.im_egress.watcher import MaosCompletionWatcher
from sementic.im_models import IMMessageEvent, MentionRegistryItem
from sementic.intent_classifier import TaskIntentClassifier
from sementic.models import BotProfile, ChatMessage, PlannerRequest, TaskIntentDecision
from sementic.task_graph import TaskGraphPlan
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore, StoredChatMessage

logger = logging.getLogger(__name__)


class PlanMessageResponse(BaseModel):
    event_id: str
    group_session_id: str
    stored_message_id: str
    history_window_size: int
    skipped_planning: bool = False
    skip_reason: str | None = None
    task_intent: TaskIntentDecision | None = None
    plan: TaskGraphPlan | None = None
    maos_task_ids: list[str] = Field(default_factory=list)


class MessageHandler:
    def __init__(
        self,
        *,
        history_store: RedisHistoryStore,
        planner: Planner,
        intent_classifier: TaskIntentClassifier | None = None,
        bot_registry: BotRegistry | None = None,
        bot_service: BotServiceClient | None = None,
        maos_executor: MaosExecutor | None = None,
        im_egress: ImEgressPublisher | None = None,
        maos_completion_watcher: MaosCompletionWatcher | None = None,
    ) -> None:
        self.history_store = history_store
        self.planner = planner
        self.intent_classifier = intent_classifier or TaskIntentClassifier()
        self.bot_registry = bot_registry or BotRegistry()
        self.bot_service = bot_service or BotServiceClient()
        self.maos_executor = maos_executor
        self.im_egress = im_egress
        self.maos_completion_watcher = maos_completion_watcher

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
            event=event,
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

        if event.has_workspace_credentials:
            logger.info(
                "workspace context present event_id=%s workspace_id=%s has_multica_token=%s",
                event.event_id,
                event.workspace_id,
                bool(event.multica_token),
            )

        planner_request = self._build_planner_request(
            event,
            recent,
            available_bots=owned_bots,
        )
        plan = await self.planner.plan(planner_request)
        plan = _inject_workspace_context_into_plan(plan, event)

        egress_context = build_egress_context(event, plan, owned_bots)

        maos_task_ids: list[str] = []
        if self.maos_executor is not None:
            plan = enrich_plan_for_execution(
                plan,
                owned_bots,
                event_id=event.event_id,
            )
            maos_task_ids = await asyncio.to_thread(self.maos_executor.submit_plan, plan)
            if (
                egress_context
                and maos_task_ids
                and self.maos_completion_watcher is not None
            ):
                self.maos_completion_watcher.watch(
                    task_id=maos_task_ids[0],
                    context=egress_context,
                )

        logger.info(
            "task graph plan event_id=%s maos_task_ids=%s plan=%s",
            event.event_id,
            maos_task_ids,
            plan.model_dump_json(ensure_ascii=False),
        )

        return PlanMessageResponse(
            event_id=event.event_id,
            group_session_id=event.group_session_id,
            stored_message_id=stored.msg_id,
            history_window_size=len(recent),
            task_intent=task_intent,
            plan=plan,
            maos_task_ids=maos_task_ids,
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
            workspace_id=event.workspace_id,
            multica_token=event.multica_token,
        )

    async def _resolve_owned_bots(
        self,
        *,
        sender_user_id: str,
        mentions: list[MentionRegistryItem],
        event: IMMessageEvent,
    ) -> list[BotProfile]:
        if self.bot_service.enabled and event.can_query_im_agents:
            try:
                bots = await self.bot_service.resolve_workspace_agents_for_orchestration(
                    team_id=event.team_id or "",
                    owner_user_id=event.user_context.user_id,
                    sender_user_id=sender_user_id,
                    mentions=mentions,
                )
                if bots or not event.has_workspace_credentials:
                    return bots
                return [_workspace_default_bot(event, sender_user_id=sender_user_id, mentions=mentions)]
            except Exception as exc:
                logger.warning(
                    "bot service unavailable, falling back to local registry: %s",
                    exc,
                )
        bots = self.bot_registry.resolve_owned_bots_for_orchestration(
            sender_user_id=sender_user_id,
            mentions=mentions,
        )
        if bots or not event.has_workspace_credentials:
            return bots
        return [_workspace_default_bot(event, sender_user_id=sender_user_id, mentions=mentions)]


def _workspace_default_bot(
    event: IMMessageEvent,
    *,
    sender_user_id: str,
    mentions: list[MentionRegistryItem],
) -> BotProfile:
    if mentions:
        bot_user_id = mentions[0].entity_id
    else:
        bot_user_id = f"workspace:{event.workspace_id}"
    return BotProfile(
        bot_user_id=bot_user_id,
        display_name=bot_user_id,
        role="Managed workspace agent",
        expertise=["general", "coding", "multica"],
        owner_user_id=sender_user_id,
        share_scope="private",
        multica_agent_id=event.workspace_id,
        is_online=True,
    )


def _inject_workspace_context_into_plan(
    plan: TaskGraphPlan,
    event: IMMessageEvent,
) -> TaskGraphPlan:
    if not event.has_workspace_credentials:
        return plan

    graph_input = dict(plan.graph.input)
    graph_input.setdefault("channel_id", event.group_session_id)
    graph_input.setdefault("user_message", event.message_context.content)
    graph_input["workspace_id"] = event.workspace_id
    graph_input["multica_token"] = event.multica_token
    plan.graph.input = graph_input
    return plan
