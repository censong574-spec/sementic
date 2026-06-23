from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class EgressKind(str, Enum):
    NODE = "node"
    FINAL = "final"
    ERROR = "error"


@dataclass(frozen=True)
class EgressContext:
    event_id: str
    channel_id: str
    root_post_id: str
    bot_user_id: str
    bot_username: str
    plan_id: str
    maos_task_id: str | None = None
