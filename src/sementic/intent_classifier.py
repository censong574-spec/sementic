from __future__ import annotations

import logging
from dataclasses import dataclass

from pydantic import ValidationError

from sementic.llm import LLMClient, MockLLMClient, extract_json_object
from sementic.models import ChatMessage, TaskIntentDecision
from sementic.prompts import INTENT_SYSTEM_PROMPT, build_intent_user_prompt

logger = logging.getLogger(__name__)


@dataclass
class IntentClassifierConfig:
    max_retries: int = 1


class TaskIntentClassifier:
    """Stage-1 gate: decide whether the latest message needs task creation."""

    def __init__(
        self,
        llm: LLMClient | None = None,
        config: IntentClassifierConfig | None = None,
    ) -> None:
        self.llm = llm or MockLLMClient()
        self.config = config or IntentClassifierConfig()

    async def classify(
        self,
        *,
        channel_id: str,
        sender_display_name: str,
        recent_messages: list[ChatMessage],
        current_message: str,
    ) -> TaskIntentDecision:
        user_prompt = build_intent_user_prompt(
            channel_id=channel_id,
            sender_display_name=sender_display_name,
            recent_messages=recent_messages,
            current_message=current_message,
        )

        last_error = "unknown error"
        last_output = ""

        for attempt in range(self.config.max_retries + 1):
            raw = await self.llm.complete(system=INTENT_SYSTEM_PROMPT, user=user_prompt)
            last_output = raw
            try:
                payload = extract_json_object(raw)
                decision = TaskIntentDecision.model_validate(payload)
                logger.info(
                    "task intent classified needs_task=%s confidence=%.2f reason=%s",
                    decision.needs_task,
                    decision.confidence,
                    decision.reason,
                )
                return decision
            except (ValidationError, ValueError) as exc:
                last_error = str(exc)
                if attempt >= self.config.max_retries:
                    raise ValueError(
                        f"Failed to classify task intent after "
                        f"{self.config.max_retries + 1} attempts: {last_error}"
                    ) from exc

        raise RuntimeError("Intent classifier retry loop exited unexpectedly")
