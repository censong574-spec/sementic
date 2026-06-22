"""A2A task 暂存与幂等记录。

Provider 派发任务后会先形成 A2A task。这个模块保存运行期 task，
并把 idempotency key 写到本地 registry，避免 Temporal retry 时重复创建外部任务。
"""

import copy
import json
import os
import threading
import time
from typing import Any


TASKS: dict[str, dict[str, Any]] = {}
TASKS_LOCK = threading.Lock()
_INVOCATIONS_LOCK = threading.Lock()


def store_task(task: dict[str, Any]) -> None:
    with TASKS_LOCK:
        TASKS[task["id"]] = {
            "task": task,
            "result_artifact_created": False,
        }
    store_idempotent_task(task)


def request_idempotency_key(request: dict[str, Any]) -> str | None:
    value = request.get("metadata", {}).get("idempotency_key")
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def task_for_idempotency_key(idempotency_key: str | None) -> dict[str, Any] | None:
    if not idempotency_key:
        return None
    with TASKS_LOCK:
        for record in TASKS.values():
            task = record.get("task", {})
            if task.get("metadata", {}).get("idempotencyKey") == idempotency_key:
                return copy.deepcopy(task)
    record = _load_invocation_record(idempotency_key)
    task = record.get("task") if record else None
    if not isinstance(task, dict):
        return None
    with TASKS_LOCK:
        TASKS[task["id"]] = {
            "task": copy.deepcopy(task),
            "result_artifact_created": bool(record.get("result_artifact_created", False)),
        }
    return copy.deepcopy(task)


def store_idempotent_task(task: dict[str, Any]) -> None:
    idempotency_key = task.get("metadata", {}).get("idempotencyKey")
    if not idempotency_key:
        return
    with _INVOCATIONS_LOCK:
        registry = _load_invocations_unlocked()
        previous = registry.get(idempotency_key, {})
        registry[idempotency_key] = {
            **previous,
            "idempotency_key": idempotency_key,
            "workflow_id": task.get("metadata", {}).get("workflow_id"),
            "node_id": task.get("metadata", {}).get("nodeId"),
            "backend": task.get("metadata", {}).get("backend"),
            "a2a_task_id": task.get("id"),
            "status": task.get("status", {}).get("state"),
            "created_at": previous.get("created_at") or _timestamp(),
            "updated_at": _timestamp(),
            "result_artifact_created": previous.get("result_artifact_created", False),
            "task": copy.deepcopy(task),
        }
        _write_invocations_unlocked(registry)


def _load_invocation_record(idempotency_key: str) -> dict[str, Any] | None:
    with _INVOCATIONS_LOCK:
        return copy.deepcopy(_load_invocations_unlocked().get(idempotency_key))


def _load_invocations_unlocked() -> dict[str, Any]:
    path = _invocation_registry_file()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _write_invocations_unlocked(registry: dict[str, Any]) -> None:
    path = _invocation_registry_file()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(registry, handle, ensure_ascii=False, indent=2)
    os.replace(temp_path, path)


def _invocation_registry_file() -> str:
    return os.environ.get(
        "A2A_INVOCATION_REGISTRY_FILE",
        "temporal-data/a2a-invocations.json",
    )


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
