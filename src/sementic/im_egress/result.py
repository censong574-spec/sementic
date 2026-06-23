from __future__ import annotations

from typing import Any

_TERMINAL_NODE_STATUSES = frozenset({"completed", "done", "success"})


def _node_reply_text(payload: dict[str, Any]) -> str | None:
    for field in ("final_reply", "result", "latest_comment"):
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _is_emit_node(payload: dict[str, Any]) -> bool:
    if payload.get("agent_name") or payload.get("agent_backend") or payload.get("multica_job_id"):
        return False
    return "user_message" in payload or "channel_id" in payload


def _node_is_completed(payload: dict[str, Any]) -> bool:
    status = str(payload.get("status") or "").lower()
    if status:
        return status in _TERMINAL_NODE_STATUSES
    return _node_reply_text(payload) is not None


def iter_agent_node_replies(task_snapshot: dict[str, Any]) -> list[tuple[str, str]]:
    """Return completed agent node replies from a MAOS snapshot."""
    result = task_snapshot.get("result")
    if not isinstance(result, dict):
        return []

    seen: set[str] = set()
    replies: list[tuple[str, str]] = []
    for key in ("results", "instance_results"):
        node_results = result.get(key)
        if not isinstance(node_results, dict):
            continue
        for node_id, payload in node_results.items():
            if not isinstance(payload, dict) or "#" in node_id:
                continue
            if node_id in seen or _is_emit_node(payload):
                continue
            if not _node_is_completed(payload):
                continue
            text = _node_reply_text(payload)
            if not text:
                continue
            seen.add(node_id)
            replies.append((node_id, text))
    return replies


def extract_final_reply_text(task_snapshot: dict[str, Any]) -> str | None:
    """Pull agent_task final text from a MAOS Temporal task snapshot."""
    replies = iter_agent_node_replies(task_snapshot)
    if not replies:
        result = task_snapshot.get("result")
        if isinstance(result, dict):
            error = task_snapshot.get("error") or result.get("error")
            if isinstance(error, str) and error.strip():
                return f"任务执行失败：{error.strip()}"
        return None

    for node_id, text in replies:
        if node_id == "run_task":
            return text
    return replies[0][1]
