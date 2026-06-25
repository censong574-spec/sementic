from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

_ACTIVE_STATUSES = frozenset({"starting", "running", "pending", "queued"})


def filter_tasks_by_status(tasks: list[dict[str, Any]], status: str) -> list[dict[str, Any]]:
    normalized = (status or "active").lower()
    if normalized in {"all", "*"}:
        return list(tasks)
    if normalized in {"active", "running", "in_progress"}:
        return [task for task in tasks if str(task.get("status") or "").lower() in _ACTIVE_STATUSES]
    return [
        task
        for task in tasks
        if str(task.get("status") or "").lower() == normalized
    ]


def summarize_task(task: dict[str, Any]) -> dict[str, Any]:
    state = task.get("state") if isinstance(task.get("state"), dict) else {}
    nodes = state.get("nodes") if isinstance(state.get("nodes"), list) else []
    node_rows = [
        {
            "id": node.get("id"),
            "status": node.get("status"),
            "summary": node.get("summary"),
            "agent_name": node.get("agent_name"),
            "backend": node.get("backend"),
        }
        for node in nodes
        if isinstance(node, dict)
    ]
    return {
        "task_id": task.get("task_id") or task.get("workflow_id"),
        "workflow_id": task.get("workflow_id") or task.get("task_id"),
        "graph_id": task.get("graph_id") or state.get("graph_id"),
        "graph_name": task.get("graph_name") or state.get("graph_name"),
        "status": task.get("status"),
        "workflow_status": state.get("workflow_status"),
        "submitted_at": _format_timestamp(task.get("submitted_at")),
        "event_id": _extract_event_id(task, state),
        "user_message": _extract_user_message(state),
        "channel_id": _extract_channel_id(state),
        "nodes": node_rows,
        "error": task.get("error"),
    }


def export_tasks_payload(
    snapshot: dict[str, Any],
    *,
    status: str = "active",
) -> dict[str, Any]:
    tasks = snapshot.get("tasks") if isinstance(snapshot.get("tasks"), list) else []
    filtered = filter_tasks_by_status(tasks, status)
    return {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "filter": status,
        "runtime_status": snapshot.get("runtime_status"),
        "temporal": snapshot.get("temporal"),
        "task_count": len(filtered),
        "tasks": [summarize_task(task) for task in filtered],
    }


def _extract_event_id(task: dict[str, Any], state: dict[str, Any]) -> str:
    for container in (
        task.get("result") if isinstance(task.get("result"), dict) else {},
        state.get("results") if isinstance(state.get("results"), dict) else {},
        state.get("instance_results") if isinstance(state.get("instance_results"), dict) else {},
    ):
        for payload in container.values():
            if not isinstance(payload, dict):
                continue
            event_id = payload.get("sementic_event_id") or payload.get("event_id")
            if event_id:
                return str(event_id)
    return ""


def _extract_user_message(state: dict[str, Any]) -> str:
    results = state.get("results") if isinstance(state.get("results"), dict) else {}
    for key in ("intake", "user_message"):
        payload = results.get(key)
        if isinstance(payload, dict) and payload.get("user_message"):
            return str(payload["user_message"])
    instance_results = (
        state.get("instance_results") if isinstance(state.get("instance_results"), dict) else {}
    )
    for payload in instance_results.values():
        if isinstance(payload, dict) and payload.get("user_message"):
            return str(payload["user_message"])
    return ""


def _extract_channel_id(state: dict[str, Any]) -> str:
    results = state.get("results") if isinstance(state.get("results"), dict) else {}
    intake = results.get("intake")
    if isinstance(intake, dict) and intake.get("channel_id"):
        return str(intake["channel_id"])
    return ""


def _format_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
