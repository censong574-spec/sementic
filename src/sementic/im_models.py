from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator


class Ownership(str, Enum):
    MY_SYSTEM = "MY_SYSTEM"
    OTHERS = "OTHERS"


class UserContext(BaseModel):
    user_id: str
    username: str
    is_bot: bool = False
    ownership: Ownership = Ownership.OTHERS
    workspace_id: str | None = None
    multica_token: str | None = None

    @property
    def has_workspace_credentials(self) -> bool:
        return bool((self.workspace_id or "").strip() and (self.multica_token or "").strip())


class MentionRegistryItem(BaseModel):
    entity_id: str
    ownership: Ownership = Ownership.OTHERS


class MessageContext(BaseModel):
    msg_id: str
    parent_msg_id: str | None = None
    content: str
    mentions_registry: list[MentionRegistryItem] = Field(default_factory=list)

    @field_validator("mentions_registry", mode="before")
    @classmethod
    def normalize_mentions_registry(cls, value: object) -> object:
        if value is None:
            return []
        return value


class IMMessageEvent(BaseModel):
    event_id: str
    group_session_id: str
    workspace_id: str | None = None
    multica_token: str | None = None
    user_context: UserContext
    message_context: MessageContext

    @model_validator(mode="before")
    @classmethod
    def lift_workspace_fields(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value

        data = dict(value)
        user_context = dict(data.get("user_context") or {})
        for field in ("workspace_id", "multica_token"):
            top_value = data.get(field)
            if top_value and not user_context.get(field):
                user_context[field] = top_value
        data["user_context"] = user_context
        return data

    @model_validator(mode="after")
    def normalize_workspace_context(self) -> IMMessageEvent:
        workspace_id = self.workspace_id or self.user_context.workspace_id
        multica_token = self.multica_token or self.user_context.multica_token
        updates: dict[str, str | None] = {}
        if workspace_id != self.user_context.workspace_id:
            updates["workspace_id"] = workspace_id
        if multica_token != self.user_context.multica_token:
            updates["multica_token"] = multica_token
        if updates:
            self.user_context = self.user_context.model_copy(update=updates)
        self.workspace_id = workspace_id
        self.multica_token = multica_token
        return self

    @property
    def has_workspace_credentials(self) -> bool:
        return self.user_context.has_workspace_credentials
