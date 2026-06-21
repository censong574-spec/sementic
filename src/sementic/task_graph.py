from __future__ import annotations

import json
import re
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from sementic.models import BotProfile, PlannerRequest


class TaskGraphEdge(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_node: str = Field(alias="from")
    to: str
    when: str | None = None
    label: str | None = None
    kind: str | None = None


class TaskGraphNode(BaseModel):
    id: str
    label: str | None = None
    type: str | None = None
    operation: str | None = None
    deps: list[str] = Field(default_factory=list)
    join: str | None = None
    max_visits: int | None = Field(default=None, ge=1)
    params: dict[str, Any] = Field(default_factory=dict)
    simulate: dict[str, Any] | None = None
    agent: dict[str, Any] | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)

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
                raise ValueError("node params must be a JSON object")
            return parsed
        raise ValueError("node params must be a JSON object")


class TaskGraph(BaseModel):
    id: str
    name: str | None = None
    graph_type: str = "control_flow"
    input: dict[str, Any] = Field(default_factory=dict)
    start: str | list[str] | None = None
    start_nodes: list[str] | None = None
    default_join: str | None = None
    max_total_visits: int | None = Field(default=None, ge=1)
    nodes: list[TaskGraphNode]
    edges: list[TaskGraphEdge] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_graph_shape(self) -> TaskGraph:
        if not self.nodes:
            raise ValueError("graph.nodes must not be empty")

        node_ids = [node.id for node in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("graph node ids must be unique")

        known = set(node_ids)
        for edge in self.edges:
            if edge.from_node not in known:
                raise ValueError(f"unknown edge.from node: {edge.from_node}")
            if edge.to not in known:
                raise ValueError(f"unknown edge.to node: {edge.to}")

        if self.start is not None:
            starts = [self.start] if isinstance(self.start, str) else list(self.start)
            for node_id in starts:
                if node_id not in known:
                    raise ValueError(f"unknown start node: {node_id}")

        if self.start_nodes:
            for node_id in self.start_nodes:
                if node_id not in known:
                    raise ValueError(f"unknown start_nodes entry: {node_id}")

        return self


class TaskGraphPlan(BaseModel):
    """Planner output: runnable control-flow graph + user-facing summary."""

    plan_id: str = Field(default_factory=lambda: str(uuid4()))
    confidence: float = Field(ge=0.0, le=1.0)
    reply_to_user: str
    graph: TaskGraph


def normalize_task_graph_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    graph = normalized.get("graph")
    if not isinstance(graph, dict):
        return normalized

    graph_copy = dict(graph)
    nodes = graph_copy.get("nodes")
    if not isinstance(nodes, list):
        return normalized

    graph_copy["nodes"] = [
        {**node, "params": _coerce_params_dict(node.get("params"))}
        if isinstance(node, dict)
        else node
        for node in nodes
    ]
    normalized["graph"] = graph_copy
    return normalized


def _coerce_params_dict(value: Any) -> dict[str, Any]:
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
            raise ValueError("node params must be a JSON object")
        return parsed
    raise ValueError("node params must be a JSON object")


def validate_graph_against_request(
    plan: TaskGraphPlan,
    request: PlannerRequest,
) -> None:
    bots_by_id = {bot.bot_user_id: bot for bot in request.available_bots}

    for node in plan.graph.nodes:
        agent = node.agent
        if not agent:
            continue
        agent_key = str(agent.get("agent_key", "")).strip()
        if not agent_key:
            raise ValueError(f"node {node.id} agent.agent_key is required")
        bot = bots_by_id.get(agent_key)
        if bot is None:
            raise ValueError(f"unknown agent_key: {agent_key}")
        if not bot.sender_can_use(request.sender_user_id):
            raise PermissionError(f"sender cannot use bot: {agent_key}")
        if not bot.is_online and not bot.multica_agent_id:
            raise ValueError(f"bot is offline: {agent_key}")


def _sender_can_use_bot(sender_user_id: str, bot: BotProfile) -> bool:
    return bot.sender_can_use(sender_user_id)


def slug_graph_id(channel_id: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", channel_id).strip("-").lower()
    return f"im-{slug or 'session'}-{uuid4().hex[:8]}"
