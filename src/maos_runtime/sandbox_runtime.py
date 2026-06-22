import asyncio
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from maos_runtime.a2a_runtime import complete_task_from_agent_callback
from maos_runtime.dag_workflow import ACTIVITIES, JsonDagWorkflow, normalize_graph, validate_graph


TASK_QUEUE = "json-control-flow-visualization-task-queue"
WORKFLOW_LIST_QUERY = 'WorkflowType = "JsonDagWorkflow"'
WORKFLOW_LIST_LIMIT = 200
WORKFLOW_QUERY_TIMEOUT_SECONDS = 3
WORKFLOW_RESULT_TIMEOUT_SECONDS = 3
COMPLETED_WORKFLOW_DISPLAY_LIMIT = 10
API_RESULT_TEXT_LIMIT = 1200
API_RESULT_LIST_LIMIT = 3
DEFAULT_TEMPORAL_HOST = "127.0.0.1"
DEFAULT_TEMPORAL_PORT = 7233
DEFAULT_TEMPORAL_UI_PORT = 8233
DEFAULT_TEMPORAL_DB_FILE = Path("temporal-data/temporal.db")


class TemporalTaskService:
    """Sandbox 微服务与 Temporal 之间的桥接层。

    这里不是重新实现一套内存任务管理器。Web/API 线程调用本类的同步方法，
    内部事件循环负责连接 Temporal、启动 workflow、查询状态，并把 Agent
    callback 转成 workflow signal。
    """

    def __init__(
        self,
        *,
        temporal_address: str | None = None,
        temporal_namespace: str = "default",
        temporal_host: str = DEFAULT_TEMPORAL_HOST,
        temporal_port: int = DEFAULT_TEMPORAL_PORT,
        temporal_ui: bool = True,
        temporal_ui_port: int = DEFAULT_TEMPORAL_UI_PORT,
        temporal_db_file: str | Path | None = DEFAULT_TEMPORAL_DB_FILE,
    ) -> None:
        self._lock = threading.Lock()
        self._runtime_status = "starting"
        self._error: str | None = None
        self._loop_ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: Client | None = None
        self._env: WorkflowEnvironment | None = None
        self._worker: Worker | None = None
        self._temporal_address = temporal_address
        self._temporal_namespace = temporal_namespace
        self._temporal_host = temporal_host
        self._temporal_port = temporal_port
        self._temporal_ui = temporal_ui
        self._temporal_ui_port = temporal_ui_port
        self._temporal_db_file = None if temporal_db_file is None else Path(temporal_db_file)
        self._temporal_mode = "external" if temporal_address else "embedded-persistent-dev"
        self._service_started_at = time.time()
        self._show_workflow_history = _truthy_env("SANDBOX_SHOW_WORKFLOW_HISTORY")
        self._temporal_target = (
            temporal_address if temporal_address else f"{temporal_host}:{temporal_port}"
        )
        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()

    def submit_batch(self, graphs: list[dict[str, Any]]) -> list[str]:
        if not graphs:
            raise ValueError("No graphs submitted")
        for graph in graphs:
            validate_graph(graph)
        return self._run_coro(self._submit_batch(graphs), timeout=30)

    def snapshot(self) -> dict[str, Any]:
        if not self._is_ready():
            return self._empty_snapshot()
        return self._run_coro(self._snapshot(), timeout=60)

    def task_snapshot(self, task_id: str) -> dict[str, Any]:
        if not self._is_ready():
            raise KeyError(task_id)
        return self._run_coro(self._task_snapshot(task_id), timeout=30)

    def runtime_info(self) -> dict[str, Any]:
        with self._lock:
            return {
                "runtime_status": self._runtime_status,
                "error": self._error,
                "temporal": self._temporal_info(),
            }

    def handle_agent_callback(self, callback: dict[str, Any]) -> dict[str, Any]:
        # Agent/Simulator 的回调先规范化为 A2A event，再 signal 给对应 workflow。
        if not self._is_ready():
            raise RuntimeError("Temporal runtime is not ready")
        event = complete_task_from_agent_callback(callback)
        self._run_coro(self._signal_agent_event(event), timeout=30)
        return event

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start_temporal())
        self._loop_ready.set()
        self._loop.run_forever()

    async def _start_temporal(self) -> None:
        try:
            if self._temporal_address:
                self._client = await Client.connect(
                    self._temporal_address,
                    namespace=self._temporal_namespace,
                )
            else:
                db_filename = None
                if self._temporal_db_file is not None:
                    self._temporal_db_file.parent.mkdir(parents=True, exist_ok=True)
                    db_filename = str(self._temporal_db_file)
                self._env = await WorkflowEnvironment.start_local(
                    namespace=self._temporal_namespace,
                    ip=self._temporal_host,
                    port=self._temporal_port,
                    ui=self._temporal_ui,
                    ui_port=self._temporal_ui_port if self._temporal_ui else None,
                    dev_server_database_filename=db_filename,
                )
                self._client = self._env.client
            self._worker = Worker(
                self._client,
                task_queue=TASK_QUEUE,
                workflows=[JsonDagWorkflow],
                activities=ACTIVITIES,
            )
            await self._worker.__aenter__()
            with self._lock:
                self._runtime_status = "ready"
        except Exception as exc:
            with self._lock:
                self._runtime_status = "failed"
                self._error = str(exc)
            self._loop_ready.set()
            raise

    async def _submit_batch(self, graphs: list[dict[str, Any]]) -> list[str]:
        if self._client is None:
            raise RuntimeError("Temporal client is not ready")

        task_ids: list[str] = []
        for graph in graphs:
            # 每个任务图对应一个 Temporal workflow；长等待和高并发由 Temporal 承担。
            task_id = _task_id_for_graph(graph)
            await self._client.start_workflow(
                JsonDagWorkflow.run,
                graph,
                id=task_id,
                task_queue=TASK_QUEUE,
                memo={
                    "service": "persistent-execution-sandbox",
                    "graph_id": graph["id"],
                    "graph_name": graph.get("name", graph["id"]),
                },
                static_summary=f"{graph.get('name', graph['id'])} ({graph['id']})",
            )
            task_ids.append(task_id)
        return task_ids

    async def _signal_agent_event(self, event: dict[str, Any]) -> None:
        workflow_id = event.get("workflow_id")
        if not workflow_id:
            raise ValueError("Agent callback did not include workflow_id")
        if self._client is None:
            raise RuntimeError("Temporal client is not ready")
        handle = self._client.get_workflow_handle(workflow_id)
        await handle.signal(JsonDagWorkflow.agent_node_completed, event)

    async def _snapshot(self) -> dict[str, Any]:
        if self._client is None:
            return self._empty_snapshot()
        executions = await self._list_dag_workflows()
        tasks = list(
            await asyncio.gather(
                *(self._task_from_execution(execution) for execution in executions)
            )
        )
        running_count = sum(
            1 for task in tasks if task["status"] in {"starting", "running"}
        )
        with self._lock:
            self._runtime_status = "running" if running_count else "ready"
            runtime_status = self._runtime_status
            error = self._error
        return {
            "runtime_status": runtime_status,
            "error": error,
            "temporal": self._temporal_info(),
            "tasks": tasks,
        }

    async def _task_snapshot(self, task_id: str) -> dict[str, Any]:
        if self._client is None:
            raise KeyError(task_id)
        handle = self._client.get_workflow_handle(task_id)
        try:
            description = await handle.describe()
        except Exception as exc:
            raise KeyError(task_id) from exc

        status = _status_from_raw_description(description)
        if status in {"completed", "failed", "cancelled", "terminated", "timed_out"}:
            return await self._closed_task_from_handle(task_id, handle, status)
        return await self._running_task_from_handle(task_id, handle)

    async def _list_dag_workflows(self) -> list[Any]:
        assert self._client is not None
        executions: list[Any] = []
        try:
            iterator = self._client.list_workflows(
                WORKFLOW_LIST_QUERY,
                limit=WORKFLOW_LIST_LIMIT,
            )
            async for execution in iterator:
                executions.append(execution)
        except Exception:
            iterator = self._client.list_workflows(limit=WORKFLOW_LIST_LIMIT)
            async for execution in iterator:
                if execution.workflow_type == "JsonDagWorkflow":
                    executions.append(execution)
        executions = sorted(executions, key=lambda item: item.start_time, reverse=True)
        if not self._show_workflow_history:
            executions = self._visible_recent_executions(executions)
        return executions

    def _visible_recent_executions(self, executions: list[Any]) -> list[Any]:
        visible: list[Any] = []
        completed_count = 0
        completed_limit = _completed_workflow_display_limit()
        for execution in executions:
            status = _status_from_execution(execution)
            is_recently_submitted = (
                execution.start_time.timestamp() >= self._service_started_at - 5
            )
            if status == "running" or is_recently_submitted:
                visible.append(execution)
                if status == "completed":
                    completed_count += 1
                continue
            if status == "completed" and completed_count < completed_limit:
                visible.append(execution)
                completed_count += 1
        return visible

    async def _task_from_execution(self, execution: Any) -> dict[str, Any]:
        assert self._client is not None
        handle = self._client.get_workflow_handle(execution.id, run_id=execution.run_id)
        status = _status_from_execution(execution)
        if status in {"completed", "failed", "cancelled", "terminated", "timed_out"}:
            task = await self._closed_task_from_handle(execution.id, handle, status)
        else:
            task = await self._running_task_from_handle(execution.id, handle)
        task["submitted_at"] = execution.start_time.timestamp()
        task["workflow_id"] = execution.id
        task["task_id"] = execution.id
        return task

    async def _running_task_from_handle(self, task_id: str, handle: Any) -> dict[str, Any]:
        try:
            state = await asyncio.wait_for(
                handle.query(JsonDagWorkflow.graph_state),
                timeout=WORKFLOW_QUERY_TIMEOUT_SECONDS,
            )
            status = _normalize_task_status(state.get("workflow_status", "running"))
            if status in {"completed", "failed", "cancelled", "terminated", "timed_out"}:
                try:
                    result = await asyncio.wait_for(
                        handle.result(),
                        timeout=WORKFLOW_RESULT_TIMEOUT_SECONDS,
                    )
                    return _task_from_state(
                        task_id,
                        result.get("state") or state,
                        status=result.get("status", status),
                        result=_slim_result_for_api(result),
                    )
                except Exception as exc:
                    return _task_from_state(
                        task_id,
                        state,
                        status="failed",
                        error=str(exc),
                    )
            return _task_from_state(task_id, state, status=status)
        except Exception as exc:
            return _minimal_task(task_id, status="running", error=str(exc))

    async def _closed_task_from_handle(
        self,
        task_id: str,
        handle: Any,
        status: str,
    ) -> dict[str, Any]:
        try:
            result = await asyncio.wait_for(
                handle.result(),
                timeout=WORKFLOW_RESULT_TIMEOUT_SECONDS,
            )
            state = result.get("state") or _state_from_result(result)
            return _task_from_state(
                task_id,
                state,
                status=result.get("status", status),
                result=_slim_result_for_api(result),
            )
        except Exception as exc:
            return _minimal_task(task_id, status="failed", error=str(exc))

    def _run_coro(self, coro: Any, timeout: float) -> Any:
        self._loop_ready.wait(timeout=60)
        if self._loop is None:
            raise RuntimeError("Temporal runtime loop is not ready")
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    def _is_ready(self) -> bool:
        self._loop_ready.wait(timeout=60)
        with self._lock:
            return self._runtime_status != "failed" and self._loop is not None

    def _empty_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "runtime_status": self._runtime_status,
                "error": self._error,
                "temporal": self._temporal_info(),
                "tasks": [],
            }

    def _temporal_info(self) -> dict[str, Any]:
        return {
            "mode": self._temporal_mode,
            "target": self._temporal_target,
            "namespace": self._temporal_namespace,
            "db_file": (
                str(self._temporal_db_file)
                if self._temporal_mode != "external" and self._temporal_db_file
                else None
            ),
            "ui": self._temporal_ui if self._temporal_mode != "external" else None,
            "ui_port": self._temporal_ui_port if self._temporal_ui else None,
            "task_queue": TASK_QUEUE,
        }


# Preferred name for the Temporal-backed control-flow sandbox.
ControlFlowTaskService = TemporalTaskService

# Backwards-compatible name used by older imports.
DagTaskManager = TemporalTaskService


def preview_state(graph: dict[str, Any]) -> dict[str, Any]:
    spec = normalize_graph(graph)
    return {
        "graph_id": spec.get("id", ""),
        "graph_name": spec.get("name", spec["id"]),
        "graph_type": spec.get("graph_type", "control_flow"),
        "workflow_status": "queued",
        "levels": spec["levels"],
        "edges": spec["edges"],
        "nodes": [
            {
                "id": node["id"],
                "label": node.get("label", node["id"]),
                "type": node.get("type", "agent"),
                "operation": node.get("operation", "merge"),
                "deps": node.get("deps", []),
                "join": node.get("join", spec.get("default_join", "all")),
                "status": "pending",
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "planned_duration_seconds": None,
                "elapsed_seconds": 0,
                "heartbeat_count": 0,
                "last_heartbeat_at": None,
                "simulator_job_id": None,
                "agent_service_task_id": None,
                "hermes_job_id": None,
                "hermes_prompt": None,
                "agent_status": None,
                "backend": _node_backend_for_display(node),
                "completion_mode": None,
                "a2a_task_id": None,
                "a2a_state": None,
                "agent_name": None,
                "summary": "",
                "visits": 0,
                "max_visits": int(node.get("max_visits", node.get("max_attempts", 1))),
                "current_instance_id": None,
                "instances": [],
            }
            for node in spec["nodes"]
        ],
        "instances": [],
    }


def build_preview_levels(nodes: list[dict[str, Any]]) -> list[list[str]]:
    remaining = {node["id"]: set(node.get("deps", [])) for node in nodes}
    completed: set[str] = set()
    levels: list[list[str]] = []
    while remaining:
        ready = sorted(
            node_id
            for node_id, deps in remaining.items()
            if deps.issubset(completed)
        )
        if not ready:
            return [sorted(remaining)]
        levels.append(ready)
        completed.update(ready)
        for node_id in ready:
            remaining.pop(node_id)
    return levels


def _node_backend_for_display(node: dict[str, Any]) -> str:
    node_type = str(node.get("type") or ("condition" if node.get("operation") == "condition" else "agent")).lower()
    if node_type in {"condition", "decision", "router", "branch"}:
        return "control-flow"
    agent = node.get("agent")
    agent_config = agent if isinstance(agent, dict) else {}
    backend = (
        agent_config.get("backend")
        or node.get("backend")
        or node.get("runtime")
        or "simulator"
    )
    normalized = str(backend).lower().replace("_", "-")
    if normalized in {"agent-service", "agentservice", "real-agent", "multica-daemon"}:
        return "multica"
    if normalized in {"sim", "mock", "simulator"}:
        return "simulator"
    if normalized in {"hermes", "hermes-oneshot", "direct-hermes", "hermes-direct"}:
        return "hermes"
    return normalized


def _task_id_for_graph(graph: dict[str, Any]) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", graph["id"]).strip("-") or "graph"
    return f"task-{slug}-{uuid.uuid4()}"


def _task_from_state(
    task_id: str,
    state: dict[str, Any],
    status: str,
    result: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "graph_id": state.get("graph_id", task_id),
        "graph_name": state.get("graph_name", task_id),
        "submitted_at": None,
        "status": _normalize_task_status(status),
        "state": state,
        "result": result,
        "error": error,
        "workflow_id": task_id,
    }


def _minimal_task(
    task_id: str,
    status: str,
    error: str | None = None,
) -> dict[str, Any]:
    state = {
        "graph_id": task_id,
        "graph_name": task_id,
        "workflow_status": status,
        "levels": [],
        "edges": [],
        "nodes": [],
    }
    return _task_from_state(task_id, state, status=status, error=error)


def _state_from_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "graph_id": result.get("graph_id", ""),
        "graph_name": result.get("graph_name", ""),
        "workflow_status": result.get("status", "completed"),
        "levels": [],
        "edges": [],
        "nodes": [],
    }


def _slim_result_for_api(result: dict[str, Any] | None) -> dict[str, Any] | None:
    if result is None:
        return None
    if not isinstance(result, dict):
        return {"value": _truncate_api_value(result)}

    slim: dict[str, Any] = {}
    for key in ("graph_id", "graph_name", "graph_type", "status", "node_count", "instance_count", "error"):
        if key in result:
            slim[key] = _truncate_api_value(result.get(key))
    if isinstance(result.get("state"), dict):
        slim["state"] = _slim_state_for_api(result["state"])

    node_results = result.get("results")
    if isinstance(node_results, dict):
        slim["results"] = {
            str(node_id): _slim_result_payload(payload)
            for node_id, payload in node_results.items()
        }
    instance_results = result.get("instance_results")
    if isinstance(instance_results, dict):
        slim["instance_results"] = _slim_instance_results_for_api(instance_results)
    return slim


def _slim_state_for_api(state: dict[str, Any]) -> dict[str, Any]:
    slim = dict(state)
    nodes = slim.get("nodes")
    if isinstance(nodes, list):
        slim["nodes"] = [
            {
                **node,
                "summary": _truncate_api_text(node.get("summary")),
                "hermes_prompt": _truncate_api_text(node.get("hermes_prompt")),
            }
            if isinstance(node, dict)
            else node
            for node in nodes
        ]
    instances = slim.get("instances")
    if isinstance(instances, list):
        slim["instances"] = instances[-50:]
    instance_results = slim.get("instance_results")
    if isinstance(instance_results, dict):
        slim["instance_results"] = _slim_instance_results_for_api(instance_results)
    return slim


def _slim_instance_results_for_api(instance_results: dict[str, Any]) -> dict[str, Any]:
    return {
        str(instance_id): _slim_result_payload(payload)
        for instance_id, payload in instance_results.items()
    }


def _slim_result_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return _truncate_api_value(payload)

    allowed_keys = (
        "status",
        "error",
        "message",
        "artifact",
        "decision",
        "reason",
        "structured_output",
        "required_changes",
        "confidence",
        "route",
        "order_status",
        "payment_status",
        "risk",
        "percent",
        "count",
        "tracking_number",
        "channel",
        "reservation_id",
        "agent_backend",
        "agent_key",
        "requested_agent_key",
        "context_policy",
        "runtime_profile",
        "execution_mode",
        "agent_id",
        "agent_name",
        "agent_service_task_id",
        "hermes_job_id",
        "hermes_command",
        "hermes_workdir",
        "title",
        "latest_comment",
        "stdout",
        "comments",
        "runs",
        "messages",
        "trace",
    )
    slim: dict[str, Any] = {}
    for key in allowed_keys:
        if key not in payload:
            continue
        if key in {"comments", "runs", "messages", "trace"}:
            slim[key] = _slim_api_list(payload.get(key))
        else:
            slim[key] = _truncate_api_value(payload.get(key))
    omitted_keys = sorted(set(payload) - set(slim))
    if omitted_keys:
        slim["_omitted_keys"] = omitted_keys[:20]
    return slim


def _slim_api_list(value: Any) -> list[Any]:
    if not isinstance(value, list):
        return []
    return [_truncate_api_value(item) for item in value[-API_RESULT_LIST_LIMIT:]]


def _truncate_api_value(value: Any) -> Any:
    if isinstance(value, str):
        return _truncate_api_text(value)
    if isinstance(value, list):
        return [_truncate_api_value(item) for item in value[-API_RESULT_LIST_LIMIT:]]
    if isinstance(value, dict):
        return {
            str(key): _truncate_api_value(item)
            for key, item in value.items()
            if str(key) not in {"raw", "history", "full_text", "transcript"}
        }
    return value


def _truncate_api_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    if len(value) <= API_RESULT_TEXT_LIMIT:
        return value
    return value[:API_RESULT_TEXT_LIMIT] + "...<truncated>"


def _status_from_execution(execution: Any) -> str:
    if execution.status is None:
        return "running"
    return _normalize_task_status(execution.status.name)


def _status_from_raw_description(description: Any) -> str:
    status = description.raw_description.workflow_execution_info.status
    status_name = status.name if hasattr(status, "name") else str(status)
    return _normalize_task_status(status_name)


def _normalize_task_status(status: str) -> str:
    normalized = status.lower().replace("workflow_execution_status_", "")
    return {
        "running": "running",
        "completed": "completed",
        "failed": "failed",
        "canceled": "cancelled",
        "cancelled": "cancelled",
        "terminated": "terminated",
        "timed_out": "timed_out",
        "timeout": "timed_out",
    }.get(normalized, normalized)


def _truthy_env(name: str) -> bool:
    value = os.environ.get(name, "")
    return value.lower() in {"1", "true", "yes", "on"}


def _completed_workflow_display_limit() -> int:
    value = os.environ.get("SANDBOX_COMPLETED_WORKFLOW_DISPLAY_LIMIT")
    if value is None:
        return COMPLETED_WORKFLOW_DISPLAY_LIMIT
    try:
        return max(0, int(value))
    except ValueError:
        return COMPLETED_WORKFLOW_DISPLAY_LIMIT
