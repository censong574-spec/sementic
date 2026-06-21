from __future__ import annotations

from datetime import datetime
from enum import Enum
import json
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class CollaborationMode(str, Enum):
    SINGLE_BOT = "single_bot"
    SEQUENTIAL_MULTI_BOT = "sequential_multi_bot"
    DISCUSS_ARBITRATE_EXECUTE = "discuss_arbitrate_execute"
    CLARIFICATION_NEEDED = "clarification_needed"


class StepType(str, Enum):
    TOOL_CALL = "tool_call"
    TASK = "task"
    DISCUSSION = "discussion"
    ARBITRATION = "arbitration"
    REPLY = "reply"


class ChatMessage(BaseModel):
    sender: str
    text: str
    timestamp: datetime | None = None
    message_id: str | None = None

    def format_line(self) -> str:
        if self.timestamp:
            ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            return f"[{ts}] {self.sender}: {self.text}"
        return f"{self.sender}: {self.text}"


class BotProfile(BaseModel):
    bot_user_id: str
    display_name: str
    role: str
    expertise: list[str] = Field(default_factory=list)
    owner_user_id: str | None = None
    share_scope: str = "private"
    multica_agent_id: str | None = None
    is_online: bool = True
    can_arbitrate: bool = False

    def format_profile(self) -> str:
        skills = ", ".join(self.expertise) if self.expertise else "general assistance"
        status = "online" if self.is_online else "offline"
        arbitrator = "yes" if self.can_arbitrate else "no"
        return (
            f"- id={self.bot_user_id}, name={self.display_name}, "
            f"role={self.role}, expertise=[{skills}], "
            f"can_arbitrate={arbitrator}, "
            f"owner={self.owner_user_id or 'unknown'}, scope={self.share_scope}, "
            f"status={status}"
        )

    def sender_can_use(self, sender_user_id: str) -> bool:
        """Workspace agents from internal API are all usable; legacy registry still checks owner."""
        if self.share_scope in ("channel_shared", "workspace"):
            return True
        if self.multica_agent_id:
            return True
        return self.owner_user_id == sender_user_id


class PlannerRequest(BaseModel):
    channel_id: str
    sender_user_id: str
    sender_display_name: str
    recent_messages: list[ChatMessage]
    mentioned_bot_ids: list[str] = Field(default_factory=list)
    available_bots: list[BotProfile]
    current_message: str
    workspace_id: str | None = None
    multica_token: str | None = None

    @property
    def has_workspace_credentials(self) -> bool:
        return bool((self.workspace_id or "").strip() and (self.multica_token or "").strip())

    @model_validator(mode="after")
    def validate_bots_present(self) -> PlannerRequest:
        if not self.available_bots:
            raise ValueError("available_bots must not be empty")
        return self


class TaskIntentDecision(BaseModel):
    needs_task: bool
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str


class ExecutionStep(BaseModel):
    order: int = Field(ge=1)
    type: StepType
    name: str | None = None
    action: str | None = None
    target_bot_id: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)

    @field_validator("params", mode="before")
    @classmethod
    def coerce_params(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return {}
            parsed = json.loads(cleaned)
            if not isinstance(parsed, dict):
                raise ValueError("params must be a JSON object")
            return parsed
        raise ValueError("params must be a JSON object")

    @model_validator(mode="after")
    def validate_step_fields(self) -> ExecutionStep:
        if self.type == StepType.TOOL_CALL and not self.name:
            raise ValueError("tool_call steps require name")
        if self.type == StepType.TASK and not self.action:
            raise ValueError("task steps require action")
        if self.type == StepType.REPLY and not self.params.get("text"):
            raise ValueError("reply steps require params.text")
        if self.type == StepType.DISCUSSION:
            participants = self.params.get("participant_bot_ids")
            if not participants or not isinstance(participants, list):
                raise ValueError("discussion steps require params.participant_bot_ids")
            if not self.params.get("topic"):
                raise ValueError("discussion steps require params.topic")
        if self.type == StepType.ARBITRATION:
            if not self.target_bot_id:
                raise ValueError("arbitration steps require target_bot_id")
            input_orders = self.params.get("input_from_orders")
            if not input_orders or not isinstance(input_orders, list):
                raise ValueError("arbitration steps require params.input_from_orders")
        return self


class ExecutionPlan(BaseModel):
    plan_id: str = Field(default_factory=lambda: str(uuid4()))
    confidence: float = Field(ge=0.0, le=1.0)
    collaboration_mode: CollaborationMode
    primary_bot_id: str | None = None
    steps: list[ExecutionStep]
    reply_to_user: str

    @field_validator("steps")
    @classmethod
    def validate_steps(cls, steps: list[ExecutionStep]) -> list[ExecutionStep]:
        if not steps:
            raise ValueError("steps must not be empty")

        orders = [step.order for step in steps]
        if len(orders) != len(set(orders)):
            raise ValueError("step order values must be unique")

        expected = list(range(1, len(steps) + 1))
        if sorted(orders) != expected:
            raise ValueError("step order must be contiguous starting from 1")

        return steps

    @model_validator(mode="after")
    def validate_mode_consistency(self) -> ExecutionPlan:
        if self.collaboration_mode == CollaborationMode.CLARIFICATION_NEEDED:
            return self

        if not self.primary_bot_id:
            raise ValueError("primary_bot_id is required unless mode is clarification_needed")

        referenced_bot_ids = set()
        for step in self.steps:
            if step.target_bot_id:
                referenced_bot_ids.add(step.target_bot_id)
            if step.type == StepType.DISCUSSION:
                participants = step.params.get("participant_bot_ids", [])
                if isinstance(participants, list):
                    referenced_bot_ids.update(str(bot_id) for bot_id in participants)

        if referenced_bot_ids and self.primary_bot_id not in referenced_bot_ids:
            raise ValueError("primary_bot_id must appear in plan participant or target bots")

        return self
