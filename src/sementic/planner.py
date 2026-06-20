from __future__ import annotations

from dataclasses import dataclass

from pydantic import ValidationError

from sementic.llm import LLMClient, MockLLMClient, extract_json_object
from sementic.models import PlannerRequest
from sementic.prompts import SYSTEM_PROMPT, build_repair_prompt, build_user_prompt
from sementic.task_graph import (
    TaskGraphPlan,
    normalize_task_graph_payload,
    validate_graph_against_request,
)


@dataclass
class PlannerConfig:
    max_retries: int = 2


class Planner:
    def __init__(self, llm: LLMClient | None = None, config: PlannerConfig | None = None) -> None:
        self.llm = llm or MockLLMClient()
        self.config = config or PlannerConfig()

    async def plan(self, request: PlannerRequest) -> TaskGraphPlan:
        self._validate_permissions(request)

        user_prompt = build_user_prompt(
            channel_id=request.channel_id,
            sender_user_id=request.sender_user_id,
            sender_display_name=request.sender_display_name,
            recent_messages=request.recent_messages,
            available_bots=request.available_bots,
            mentioned_bot_ids=request.mentioned_bot_ids,
            current_message=request.current_message,
        )

        last_error = "unknown error"
        last_output = ""

        for attempt in range(self.config.max_retries + 1):
            prompt = user_prompt if attempt == 0 else build_repair_prompt(
                user_prompt=user_prompt,
                previous_output=last_output,
                error=last_error,
            )
            raw = await self.llm.complete(system=SYSTEM_PROMPT, user=prompt)
            last_output = raw

            try:
                payload = normalize_task_graph_payload(extract_json_object(raw))
                plan = TaskGraphPlan.model_validate(payload)
                validate_graph_against_request(plan, request)
                return plan
            except (ValidationError, ValueError, PermissionError) as exc:
                last_error = str(exc)
                if attempt >= self.config.max_retries:
                    raise ValueError(
                        f"Failed to produce a valid task graph after "
                        f"{self.config.max_retries + 1} attempts: {last_error}"
                    ) from exc

        raise RuntimeError("Planner retry loop exited unexpectedly")

    def _validate_permissions(self, request: PlannerRequest) -> None:
        allowed_bot_ids = {
            bot.bot_user_id
            for bot in request.available_bots
            if self._sender_can_use_bot(request.sender_user_id, bot)
        }
        if not allowed_bot_ids:
            raise PermissionError("Sender has no usable bots in this channel")

        offline_mentioned = [
            bot.bot_user_id
            for bot in request.available_bots
            if bot.bot_user_id in request.mentioned_bot_ids and not bot.is_online
        ]
        if offline_mentioned:
            raise RuntimeError(
                "Mentioned bot is offline: " + ", ".join(offline_mentioned)
            )

    @staticmethod
    def _sender_can_use_bot(sender_user_id: str, bot) -> bool:
        if bot.share_scope == "channel_shared":
            return True
        return bot.owner_user_id == sender_user_id
