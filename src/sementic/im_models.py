from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, field_validator


class Ownership(str, Enum):
    MY_SYSTEM = "MY_SYSTEM"
    OTHERS = "OTHERS"


class UserContext(BaseModel):
    user_id: str
    username: str
    is_bot: bool = False
    ownership: Ownership = Ownership.OTHERS


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
    user_context: UserContext
    message_context: MessageContext
