import copy
import concurrent.futures
import hashlib
import json
import os
import re
import subprocess
import threading
import time
import uuid
from typing import Any

from maos_runtime.a2a_constants import (
    HERMES_BACKEND,
    LIGHTWEIGHT_MULTICA_AGENT_KEY,
    LIGHTWEIGHT_RUNTIME_PROFILE,
    MULTICA_BACKEND,
    MULTICA_COMPLETED_STATUSES,
    MULTICA_FAILED_STATUSES,
    MULTICA_JOB_BACKEND,
    MULTICA_JOB_COMPLETED_STATUSES,
    MULTICA_JOB_FAILED_STATUSES,
    PROVIDED_CONTEXT_ONLY,
    SIMULATOR_BACKEND,
)
from maos_runtime.a2a_provider_base import AgentRuntimeProvider
from maos_runtime.a2a_provider_registry import (
    list_provider_backends,
    normalize_backend,
    provider_for_backend,
    register_provider,
)
from maos_runtime.a2a_task_store import (
    TASKS as _TASKS,
    TASKS_LOCK as _TASKS_LOCK,
    request_idempotency_key as _request_idempotency_key,
    store_idempotent_task as _store_idempotent_task,
    store_task as _store_task,
    task_for_idempotency_key as _task_for_idempotency_key,
)
from maos_runtime.http_json_client import request_json as _request_json
from maos_runtime.runtime_config import (
    agent_fetch_run_messages as _agent_fetch_run_messages,
    agent_message_text_limit as _agent_message_text_limit,
    agent_poll_seconds as _agent_poll_seconds,
    agent_result_recent_comments as _agent_result_recent_comments,
    agent_result_text_limit as _agent_result_text_limit,
    agent_service_api_base as _agent_service_api_base,
    multica_job_api_base as _multica_job_api_base,
    multica_job_token as _multica_job_token,
    multica_job_workspace_id as _multica_job_workspace_id,
    multica_job_workspace_slug as _multica_job_workspace_slug,
    sandbox_api_base as _sandbox_api_base,
    simulator_api_base as _simulator_api_base,
)


_HERMES_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=int(os.environ.get("HERMES_PROVIDER_MAX_WORKERS", "4")),
    thread_name_prefix="maos-hermes",
)
_HERMES_JOBS: dict[str, dict[str, Any]] = {}
_HERMES_JOBS_LOCK = threading.Lock()
_MULTICA_JOB_BY_IDEMPOTENCY: dict[str, dict[str, Any]] = {}
_MULTICA_JOB_LOCK = threading.Lock()


class SimulatorProvider(AgentRuntimeProvider):
    """测试用 provider：把节点派发给本地 simulator_service。"""

    backend = SIMULATOR_BACKEND

    def agent_card(self, node: dict[str, Any]) -> dict[str, Any]:
        return _simulator_agent_card(node)

    def send(self, request: dict[str, Any]) -> dict[str, Any]:
        return _send_simulator_message(request)

    def poll(self, task_id: str, request: dict[str, Any]) -> dict[str, Any]:
        a2a_task = _poll_simulator_task(task_id, request.get("workflow_id"))
        return {
            "done": a2a_task["status"]["state"] in {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED"},
            "task": a2a_task,
            "event": None,
        }


class MulticaProvider(AgentRuntimeProvider):
    """真实 Multica provider：创建 Multica Task，并由 workflow 持久轮询完成状态。"""

    backend = MULTICA_BACKEND

    def agent_card(self, node: dict[str, Any]) -> dict[str, Any]:
        return _multica_agent_card(node)

    def send(self, request: dict[str, Any]) -> dict[str, Any]:
        return _send_multica_message(request)

    def poll(self, task_id: str, request: dict[str, Any]) -> dict[str, Any]:
        return _poll_multica_task(task_id)


class MulticaJobProvider(AgentRuntimeProvider):
    """Multica Native Job provider：每个 DAG 节点对应一个 native job。"""

    backend = MULTICA_JOB_BACKEND

    def agent_card(self, node: dict[str, Any]) -> dict[str, Any]:
        return _multica_job_agent_card(node)

    def send(self, request: dict[str, Any]) -> dict[str, Any]:
        return _send_multica_job_message(request)

    def poll(self, task_id: str, request: dict[str, Any]) -> dict[str, Any]:
        return _poll_multica_job_task(task_id)


class HermesOneshotProvider(AgentRuntimeProvider):
    """直连 Hermes provider：绕过 Multica，适合短任务或受控的一次性 Agent 调用。"""

    backend = HERMES_BACKEND

    def agent_card(self, node: dict[str, Any]) -> dict[str, Any]:
        return _hermes_agent_card(node)

    def send(self, request: dict[str, Any]) -> dict[str, Any]:
        return _send_hermes_message(request)

    def poll(self, task_id: str, request: dict[str, Any]) -> dict[str, Any]:
        return _poll_hermes_task(task_id)


def register_agent_provider(
    provider: AgentRuntimeProvider,
    *,
    aliases: list[str] | tuple[str, ...] = (),
    replace: bool = False,
) -> None:
    """注册一个 Agent runtime provider。

    外部扩展只需要实现 AgentRuntimeProvider，然后调用这个函数注册 backend。
    """

    register_provider(provider, aliases=aliases, replace=replace)


def registered_agent_backends() -> list[str]:
    return list_provider_backends()


def _register_default_providers() -> None:
    register_agent_provider(SimulatorProvider(), replace=True)
    register_agent_provider(MulticaProvider(), replace=True)
    register_agent_provider(MulticaJobProvider(), replace=True)
    register_agent_provider(HermesOneshotProvider(), replace=True)


_register_default_providers()


def get_agent_card(node: dict[str, Any]) -> dict[str, Any]:
    return _provider_for_node(node).agent_card(node)


def send_message(request: dict[str, Any]) -> dict[str, Any]:
    # Temporal activity retry 时可能再次进入这里；先用 idempotency key 复用已创建任务。
    message = request["message"]
    payload = _first_data_part(message)
    node = payload["node"]
    existing_task = _task_for_idempotency_key(_request_idempotency_key(request))
    if existing_task:
        return {"task": existing_task}
    return _provider_for_node(node).send(request)


def poll_task(request: dict[str, Any]) -> dict[str, Any]:
    # workflow 每次被 Temporal 拉起时只做一次短轮询，未完成则继续 durable sleep。
    task_id = request["id"]
    task_hint = request.get("task")
    with _TASKS_LOCK:
        if task_id not in _TASKS and task_hint:
            _TASKS[task_id] = {"task": copy.deepcopy(task_hint), "result_artifact_created": False}
        record = _TASKS[task_id]
        task = record["task"]
    metadata = task.get("metadata", {})
    backend = metadata.get("backend", SIMULATOR_BACKEND)
    return _provider_for_backend(backend).poll(task_id, request)


def get_task(request: dict[str, Any]) -> dict[str, Any]:
    return poll_task(request)["task"]


def complete_task_from_agent_callback(callback: dict[str, Any]) -> dict[str, Any]:
    if callback.get("a2a_task"):
        return _event_from_a2a_task(callback, callback["a2a_task"])

    task_id = callback["a2a_task_id"]
    if callback.get("idempotency_key"):
        _task_for_idempotency_key(str(callback["idempotency_key"]))
    with _TASKS_LOCK:
        record = _TASKS[task_id]
        task = record["task"]
        metadata = task["metadata"]
        job = callback.get("job", {})
        metadata["heartbeatCount"] += 1
        metadata["elapsedSeconds"] = job.get("elapsed_seconds", 0)
        metadata["lastHeartbeatAt"] = _timestamp()

        if callback["status"] == "completed":
            if not record["result_artifact_created"]:
                result = callback["result"]
                task["artifacts"] = [_artifact_from_result(result, metadata)]
                task["status"] = {
                    "state": "TASK_STATE_COMPLETED",
                    "message": _agent_message(
                        task_id,
                        task["contextId"],
                        {
                            "status": "completed",
                            "node_id": metadata["nodeId"],
                            "simulator_job_id": metadata.get("simulatorJobId"),
                        },
                    ),
                    "timestamp": _timestamp(),
                }
                metadata["finishedAt"] = job.get("finished_at")
                record["result_artifact_created"] = True
        elif callback["status"] == "failed":
            task["status"] = {
                "state": "TASK_STATE_FAILED",
                "message": _agent_message(
                    task_id,
                    task["contextId"],
                    {
                        "status": "failed",
                        "node_id": metadata["nodeId"],
                        "error": callback.get("error"),
                    },
                ),
                "timestamp": _timestamp(),
            }
            metadata["error"] = callback.get("error")
            metadata["finishedAt"] = job.get("finished_at")
        else:
            task["status"]["message"] = _agent_message(
                task_id,
                task["contextId"],
                {
                    "status": "working",
                    "node_id": metadata["nodeId"],
                    "simulator_job_id": metadata.get("simulatorJobId"),
                    "elapsed_seconds": metadata["elapsedSeconds"],
                    "planned_duration_seconds": metadata.get("plannedDurationSeconds"),
                },
            )

        completed_task = copy.deepcopy(task)

    _store_idempotent_task(completed_task)
    return _event_from_a2a_task(callback, completed_task)


def extract_result_from_task(task: dict[str, Any]) -> dict[str, Any]:
    if task["status"]["state"] != "TASK_STATE_COMPLETED":
        raise ValueError(f"A2A task is not completed: {task['status']['state']}")
    for artifact in task.get("artifacts", []):
        if artifact.get("name") == "dag-node-result":
            return _first_data_part({"parts": artifact["parts"]})
    raise ValueError(f"A2A task {task['id']} completed without result artifact")


def _send_simulator_message(request: dict[str, Any]) -> dict[str, Any]:
    message = request["message"]
    request_metadata = request.get("metadata", {})
    payload = _first_data_part(message)
    node = payload["node"]
    idempotency_key = _request_idempotency_key(request)
    task_id = _stable_runtime_id("a2a-task", node["id"], idempotency_key)
    context_id = message.get("contextId") or f"context-{uuid.uuid4()}"
    workflow_id = request_metadata["workflow_id"]
    dependency_results = _dependency_results_from_artifacts(
        payload.get("dependency_artifacts", [])
    )
    simulator_job = _request_json(
        _simulator_api_base(),
        "POST",
        "/api/simulator/jobs",
        {
            "job_id": _stable_runtime_id("sim", node["id"], idempotency_key),
            "idempotency_key": idempotency_key,
            "node": node,
            "dependency_results": dependency_results,
            "graph_input": payload.get("graph_input", {}),
            "callback": {
                "url": f"{_sandbox_api_base()}/api/agent-callbacks",
                "payload": {
                    "workflow_id": workflow_id,
                    "node_id": node["id"],
                    "a2a_task_id": task_id,
                    "context_id": context_id,
                    "idempotency_key": idempotency_key,
                },
            },
        },
    )["job"]
    now = _timestamp()
    task = {
        "id": task_id,
        "contextId": context_id,
        "status": {
            "state": "TASK_STATE_WORKING",
            "message": _agent_message(
                task_id,
                context_id,
                {
                    "status": "accepted",
                    "node_id": node["id"],
                    "simulator_job_id": simulator_job["job_id"],
                    "planned_duration_seconds": simulator_job["planned_duration_seconds"],
                },
            ),
            "timestamp": now,
        },
        "artifacts": [],
        "history": [message],
        "metadata": {
            "agentCard": get_agent_card(node),
            "backend": SIMULATOR_BACKEND,
            "idempotencyKey": idempotency_key,
            "completionMode": "callback",
            "nodeId": node["id"],
            "workflow_id": workflow_id,
            "operation": node.get("operation", "merge"),
            "referenceTaskIds": message.get("referenceTaskIds", []),
            "simulatorJobId": simulator_job["job_id"],
            "plannedDurationSeconds": simulator_job["planned_duration_seconds"],
            "startedAt": simulator_job["started_at"],
            "lastHeartbeatAt": now,
            "heartbeatCount": 0,
            "callbackMode": "agent-service-calls-sandbox-then-sandbox-signals-workflow",
            "transport": {
                "simulatorApiBase": _simulator_api_base(),
                "createJob": "POST /api/simulator/jobs",
                "callback": "POST /api/agent-callbacks",
            },
        },
    }
    _store_task(task)
    return {"task": copy.deepcopy(task)}


def _send_multica_message(request: dict[str, Any]) -> dict[str, Any]:
    message = request["message"]
    request_metadata = request.get("metadata", {})
    payload = _first_data_part(message)
    node = payload["node"]
    agent = _node_agent_config(node)
    idempotency_key = _request_idempotency_key(request)
    task_id = _stable_runtime_id("a2a-task", node["id"], idempotency_key)
    context_id = message.get("contextId") or f"context-{uuid.uuid4()}"
    workflow_id = request_metadata["workflow_id"]
    dependency_results = _dependency_results_from_artifacts(
        payload.get("dependency_artifacts", [])
    )
    context_policy = _multica_context_policy(node)
    runtime_profile = _multica_runtime_profile(node)
    execution_mode = _multica_execution_mode(node)
    requested_agent_key = agent.get("agent_key") or node.get("agent_key")
    dispatch_agent_key = _multica_dispatch_agent_key(node)

    create_payload = {
        "title": _multica_title(node, workflow_id),
        "description": _multica_description(
            node,
            dependency_results,
            payload.get("graph_input", {}),
            workflow_id,
            task_id,
        ),
        "agent_key": dispatch_agent_key,
        "agent_id": (
            None
            if _is_lightweight_multica_node(node)
            else agent.get("agent_id") or node.get("agent_id")
        ),
        "agent_name": _multica_requested_agent_name(node),
        "priority": agent.get("priority") or node.get("priority") or "medium",
        "status": agent.get("status") or "todo",
        "project_id": agent.get("project_id") or node.get("project_id"),
        "parent_id": agent.get("parent_id") or node.get("parent_id"),
        "allow_duplicate": bool(agent.get("allow_duplicate", True)),
        "metadata": _multica_metadata(
            node,
            workflow_id,
            task_id,
            context_id,
            dependency_results,
            payload.get("graph_input", {}),
            context_policy,
            runtime_profile,
            execution_mode,
            dispatch_agent_key,
            requested_agent_key,
            idempotency_key,
        ),
    }
    create_payload = {key: value for key, value in create_payload.items() if value is not None}
    multica_task = _find_multica_task_by_idempotency_key(idempotency_key)
    if multica_task is None:
        response = _request_json(_agent_service_api_base(), "POST", "/tasks", create_payload)
        multica_task = response.get("data", response)
    multica_task_id = _task_identifier(multica_task)
    if not multica_task_id:
        raise RuntimeError(f"AgentService task response did not contain an id: {multica_task!r}")

    now = _timestamp()
    poll_seconds = float(agent.get("poll_seconds", node.get("poll_seconds", _agent_poll_seconds())))
    task = {
        "id": task_id,
        "contextId": context_id,
        "status": {
            "state": "TASK_STATE_WORKING",
            "message": _agent_message(
                task_id,
                context_id,
                {
                    "status": "accepted",
                    "node_id": node["id"],
                    "agent_service_task_id": multica_task_id,
                    "agent_status": multica_task.get("status"),
                },
            ),
            "timestamp": now,
        },
        "artifacts": [],
        "history": [message],
        "metadata": {
            "agentCard": get_agent_card(node),
            "backend": MULTICA_BACKEND,
            "idempotencyKey": idempotency_key,
            "completionMode": "polling",
            "nodeId": node["id"],
            "workflow_id": workflow_id,
            "operation": node.get("operation", "agent_task"),
            "referenceTaskIds": message.get("referenceTaskIds", []),
            "agentServiceTaskId": multica_task_id,
            "agentServiceBase": _agent_service_api_base(),
            "agentKey": create_payload.get("agent_key"),
            "requestedAgentKey": requested_agent_key,
            "agentId": create_payload.get("agent_id"),
            "agentName": create_payload.get("agent_name"),
            "contextPolicy": context_policy,
            "runtimeProfile": runtime_profile,
            "executionMode": execution_mode,
            "agentStatus": multica_task.get("status"),
            "pollSeconds": poll_seconds,
            "plannedDurationSeconds": None,
            "startedAt": time.time(),
            "lastHeartbeatAt": now,
            "heartbeatCount": 0,
            "callbackMode": "temporal-durable-polling-until-agentservice-exposes-push-events",
            "transport": {
                "agentServiceApiBase": _agent_service_api_base(),
                "createTask": "POST /tasks",
                "getTask": "GET /tasks/{task_id}",
                "comments": "GET /tasks/{task_id}/comments",
                "runs": "GET /tasks/{task_id}/runs",
                "runMessages": "GET /runs/{run_id}/messages",
            },
        },
    }
    _store_task(task)
    return {"task": copy.deepcopy(task)}


def _send_multica_job_message(request: dict[str, Any]) -> dict[str, Any]:
    message = request["message"]
    request_metadata = request.get("metadata", {})
    payload = _first_data_part(message)
    node = payload["node"]
    agent = _node_agent_config(node)
    agent_id = agent.get("agent_id") or node.get("agent_id")
    if not agent_id:
        raise RuntimeError(
            f"Multica native job node {node['id']} requires agent.agent_id or node.agent_id"
        )

    idempotency_key = _request_idempotency_key(request)
    task_id = _stable_runtime_id("a2a-task", node["id"], idempotency_key)
    context_id = message.get("contextId") or f"context-{uuid.uuid4()}"
    workflow_id = request_metadata["workflow_id"]
    dependency_results = _dependency_results_from_artifacts(
        payload.get("dependency_artifacts", [])
    )
    graph_input = payload.get("graph_input", {})
    prompt = _multica_job_prompt(
        node,
        dependency_results,
        graph_input,
        workflow_id,
        task_id,
        context_id,
    )
    create_payload: dict[str, Any] = {
        "agent_id": str(agent_id),
        "prompt": prompt,
    }
    project_id = agent.get("project_id") or node.get("project_id")
    if project_id:
        create_payload["project_id"] = str(project_id)
    attachment_ids = agent.get("attachment_ids") or node.get("attachment_ids")
    if attachment_ids:
        create_payload["attachment_ids"] = [str(item) for item in attachment_ids]

    multica_job = _find_multica_job_by_idempotency_key(idempotency_key)
    if multica_job is None:
        multica_job = _request_multica_job_api("POST", "/api/jobs", create_payload)
        if idempotency_key:
            with _MULTICA_JOB_LOCK:
                _MULTICA_JOB_BY_IDEMPOTENCY[idempotency_key] = copy.deepcopy(multica_job)
    multica_job_id = _job_identifier(multica_job)
    if not multica_job_id:
        raise RuntimeError(f"Multica Job response did not contain a job_id: {multica_job!r}")

    now = _timestamp()
    poll_seconds = float(agent.get("poll_seconds", node.get("poll_seconds", _agent_poll_seconds())))
    task = {
        "id": task_id,
        "contextId": context_id,
        "status": {
            "state": "TASK_STATE_WORKING",
            "message": _agent_message(
                task_id,
                context_id,
                {
                    "status": "accepted",
                    "node_id": node["id"],
                    "multica_job_id": multica_job_id,
                    "agent_status": multica_job.get("status"),
                },
            ),
            "timestamp": now,
        },
        "artifacts": [],
        "history": [message],
        "metadata": {
            "agentCard": get_agent_card(node),
            "backend": MULTICA_JOB_BACKEND,
            "idempotencyKey": idempotency_key,
            "completionMode": "polling",
            "nodeId": node["id"],
            "workflow_id": workflow_id,
            "operation": node.get("operation", "agent_task"),
            "referenceTaskIds": message.get("referenceTaskIds", []),
            "multicaJobId": multica_job_id,
            "multicaJobApiBase": _multica_job_api_base(),
            "agentId": str(agent_id),
            "agentName": agent.get("agent_name") or node.get("agent_name"),
            "contextPolicy": _multica_context_policy(node),
            "runtimeProfile": "native_job",
            "executionMode": "multica_native_job",
            "agentStatus": multica_job.get("status"),
            "pollSeconds": poll_seconds,
            "plannedDurationSeconds": None,
            "startedAt": time.time(),
            "lastHeartbeatAt": now,
            "heartbeatCount": 0,
            "callbackMode": "temporal-durable-polling-multica-native-job",
            "transport": {
                "multicaJobApiBase": _multica_job_api_base(),
                "createJob": "POST /api/jobs",
                "getExecution": "GET /api/jobs/{job_id}/execution",
                "cancelJob": "POST /api/jobs/{job_id}/cancel",
            },
        },
    }
    _store_task(task)
    return {"task": copy.deepcopy(task)}


def _send_hermes_message(request: dict[str, Any]) -> dict[str, Any]:
    message = request["message"]
    request_metadata = request.get("metadata", {})
    payload = _first_data_part(message)
    node = payload["node"]
    agent = _node_agent_config(node)
    idempotency_key = _request_idempotency_key(request)
    task_id = _stable_runtime_id("a2a-task", node["id"], idempotency_key)
    context_id = message.get("contextId") or f"context-{uuid.uuid4()}"
    workflow_id = request_metadata["workflow_id"]
    dependency_results = _dependency_results_from_artifacts(
        payload.get("dependency_artifacts", [])
    )
    graph_input = payload.get("graph_input", {})
    runtime_profile = _hermes_runtime_profile(node)
    execution_mode = _hermes_execution_mode(node)
    context_policy = _hermes_context_policy(node)
    expected_output = _hermes_expected_output_hint(
        agent.get("prompt")
        or node.get("prompt")
        or node.get("description")
        or "请完成当前业务子任务，并返回最终结果。"
    )
    prompt = _hermes_prompt(
        node,
        dependency_results,
        graph_input,
        workflow_id,
        task_id,
        context_policy,
        runtime_profile,
    )
    timeout_seconds = float(
        agent.get(
            "timeout_seconds",
            node.get("timeout_seconds", os.environ.get("HERMES_PROVIDER_TIMEOUT_SECONDS", "300")),
        )
    )
    command, command_display = _hermes_command(node, prompt)
    workdir = _hermes_workdir(node)
    job_id = _stable_runtime_id("hermes-job", node["id"], idempotency_key)
    started_at = time.time()
    with _HERMES_JOBS_LOCK:
        if job_id in _HERMES_JOBS:
            started_at = float(_HERMES_JOBS[job_id].get("started_at", started_at))
        else:
            future = _HERMES_EXECUTOR.submit(
                _run_hermes_oneshot,
                command,
                workdir,
                timeout_seconds,
                _hermes_env(),
            )
            _HERMES_JOBS[job_id] = {
                "future": future,
                "job_id": job_id,
                "node_id": node["id"],
                "started_at": started_at,
                "timeout_seconds": timeout_seconds,
                "command": command_display,
                "workdir": workdir,
                "prompt": prompt,
                "result": None,
                "error": None,
            }

    now = _timestamp()
    poll_seconds = float(agent.get("poll_seconds", node.get("poll_seconds", _hermes_poll_seconds())))
    task = {
        "id": task_id,
        "contextId": context_id,
        "status": {
            "state": "TASK_STATE_WORKING",
            "message": _agent_message(
                task_id,
                context_id,
                {
                    "status": "accepted",
                    "node_id": node["id"],
                    "hermes_job_id": job_id,
                    "runtime": HERMES_BACKEND,
                },
            ),
            "timestamp": now,
        },
        "artifacts": [],
        "history": [message],
        "metadata": {
            "agentCard": get_agent_card(node),
            "backend": HERMES_BACKEND,
            "idempotencyKey": idempotency_key,
            "completionMode": "polling",
            "nodeId": node["id"],
            "workflow_id": workflow_id,
            "operation": node.get("operation", "agent_task"),
            "referenceTaskIds": message.get("referenceTaskIds", []),
            "hermesJobId": job_id,
            "hermesCommand": command_display,
            "hermesWorkdir": workdir,
            "hermesPrompt": prompt,
            "agentKey": agent.get("agent_key") or node.get("agent_key") or "hermes",
            "requestedAgentKey": agent.get("agent_key") or node.get("agent_key") or "hermes",
            "contextPolicy": context_policy,
            "runtimeProfile": runtime_profile,
            "executionMode": execution_mode,
            "expectedOutputFormat": expected_output.get("format"),
            "pollSeconds": poll_seconds,
            "plannedDurationSeconds": None,
            "startedAt": started_at,
            "lastHeartbeatAt": now,
            "heartbeatCount": 0,
            "callbackMode": "temporal-durable-polling-local-hermes-oneshot",
            "transport": {
                "runtime": "hermes oneshot",
                "command": command_display,
                "workdir": workdir,
            },
        },
    }
    _store_task(task)
    return {"task": copy.deepcopy(task)}


def _poll_simulator_task(task_id: str, workflow_id: str | None = None) -> dict[str, Any]:
    with _TASKS_LOCK:
        task = _TASKS[task_id]["task"]
    metadata = task["metadata"]
    job = _request_json(
        _simulator_api_base(),
        "GET",
        f"/api/simulator/jobs/{metadata['simulatorJobId']}",
    )
    return complete_task_from_agent_callback(
        {
            "workflow_id": workflow_id,
            "node_id": metadata["nodeId"],
            "a2a_task_id": task_id,
            "context_id": task["contextId"],
            "status": job["status"],
            "job": job,
            "result": job.get("result"),
            "error": job.get("error"),
        }
    )["a2a_task"]


def _poll_hermes_task(task_id: str) -> dict[str, Any]:
    with _TASKS_LOCK:
        record = _TASKS[task_id]
        task = record["task"]
        metadata = task["metadata"]
    job_id = metadata["hermesJobId"]
    with _HERMES_JOBS_LOCK:
        job = _HERMES_JOBS.get(job_id)
    now = _timestamp()
    elapsed = round(time.time() - float(metadata.get("startedAt", time.time())), 2)

    with _TASKS_LOCK:
        metadata["heartbeatCount"] += 1
        metadata["elapsedSeconds"] = elapsed
        metadata["lastHeartbeatAt"] = now
        task["status"]["timestamp"] = now
        task["status"]["message"] = _agent_message(
            task_id,
            task["contextId"],
            {
                "status": "working",
                "node_id": metadata["nodeId"],
                "hermes_job_id": job_id,
                "elapsed_seconds": elapsed,
            },
        )

    if job is None:
        return _fail_hermes_task(task_id, f"Hermes job {job_id} was not found", elapsed)

    future = job["future"]
    if not future.done():
        with _TASKS_LOCK:
            current_task = copy.deepcopy(_TASKS[task_id]["task"])
        return {"done": False, "task": current_task, "event": None}

    if job.get("result") is None and job.get("error") is None:
        try:
            job["result"] = future.result()
        except Exception as exc:
            job["error"] = str(exc)

    if job.get("error"):
        return _fail_hermes_task(task_id, str(job["error"]), elapsed)
    return _complete_hermes_task(task_id, job, elapsed)


def _complete_hermes_task(
    task_id: str,
    job: dict[str, Any],
    elapsed: float,
) -> dict[str, Any]:
    output = str(job.get("result") or "").strip()
    with _TASKS_LOCK:
        metadata_for_validation = copy.deepcopy(_TASKS[task_id]["task"]["metadata"])
    validation_error = _hermes_output_validation_error(metadata_for_validation, output)
    if validation_error:
        return _fail_hermes_task(task_id, validation_error, elapsed)

    with _TASKS_LOCK:
        record = _TASKS[task_id]
        task = record["task"]
        metadata = task["metadata"]
        metadata["elapsedSeconds"] = elapsed
        metadata["finishedAt"] = _timestamp()
        if not record["result_artifact_created"]:
            result = _hermes_result(metadata, job, elapsed)
            task["artifacts"] = [_artifact_from_result(result, metadata)]
            record["result_artifact_created"] = True
        task["status"] = {
            "state": "TASK_STATE_COMPLETED",
            "message": _agent_message(
                task_id,
                task["contextId"],
                {
                    "status": "completed",
                    "node_id": metadata["nodeId"],
                    "hermes_job_id": metadata["hermesJobId"],
                },
            ),
            "timestamp": _timestamp(),
        }
        completed_task = copy.deepcopy(task)
    _store_idempotent_task(completed_task)
    event = _event_from_a2a_task(
        {
            "workflow_id": _metadata_string(metadata, "workflow_id"),
            "node_id": metadata["nodeId"],
            "a2a_task_id": task_id,
            "status": "completed",
        },
        completed_task,
    )
    return {"done": True, "task": completed_task, "event": event}


def _fail_hermes_task(
    task_id: str,
    error: str,
    elapsed: float,
) -> dict[str, Any]:
    with _TASKS_LOCK:
        task = _TASKS[task_id]["task"]
        metadata = task["metadata"]
        metadata["elapsedSeconds"] = elapsed
        metadata["finishedAt"] = _timestamp()
        metadata["error"] = error
        task["status"] = {
            "state": "TASK_STATE_FAILED",
            "message": _agent_message(
                task_id,
                task["contextId"],
                {
                    "status": "failed",
                    "node_id": metadata["nodeId"],
                    "hermes_job_id": metadata.get("hermesJobId"),
                    "error": error,
                },
            ),
            "timestamp": _timestamp(),
        }
        failed_task = copy.deepcopy(task)
    _store_idempotent_task(failed_task)
    event = _event_from_a2a_task(
        {
            "workflow_id": _metadata_string(metadata, "workflow_id"),
            "node_id": metadata["nodeId"],
            "a2a_task_id": task_id,
            "status": "failed",
            "error": error,
        },
        failed_task,
    )
    return {"done": True, "task": failed_task, "event": event}


def _poll_multica_task(task_id: str) -> dict[str, Any]:
    with _TASKS_LOCK:
        record = _TASKS[task_id]
        task = record["task"]
        metadata = task["metadata"]
    multica_task_id = metadata["agentServiceTaskId"]
    task_response = _request_json(
        _agent_service_api_base(),
        "GET",
        f"/tasks/{multica_task_id}",
    )
    multica_task = task_response.get("data", task_response)
    status = str(multica_task.get("status") or "").lower()
    now = _timestamp()
    elapsed = round(time.time() - float(metadata.get("startedAt", time.time())), 2)

    with _TASKS_LOCK:
        metadata["heartbeatCount"] += 1
        metadata["elapsedSeconds"] = elapsed
        metadata["lastHeartbeatAt"] = now
        metadata["agentStatus"] = status or multica_task.get("status")
        task["status"]["timestamp"] = now
        task["status"]["message"] = _agent_message(
            task_id,
            task["contextId"],
            {
                "status": "working",
                "node_id": metadata["nodeId"],
                "agent_service_task_id": multica_task_id,
                "agent_status": status,
                "elapsed_seconds": elapsed,
            },
        )

    if status in MULTICA_COMPLETED_STATUSES:
        return _complete_multica_task(task_id, multica_task, elapsed)
    if status in MULTICA_FAILED_STATUSES:
        return _fail_multica_task(task_id, multica_task, elapsed)

    with _TASKS_LOCK:
        current_task = copy.deepcopy(_TASKS[task_id]["task"])
    return {"done": False, "task": current_task, "event": None}


def _poll_multica_job_task(task_id: str) -> dict[str, Any]:
    with _TASKS_LOCK:
        record = _TASKS[task_id]
        task = record["task"]
        metadata = task["metadata"]
    job_id = metadata["multicaJobId"]
    execution = _request_multica_job_api("GET", f"/api/jobs/{job_id}/execution")
    status = str(execution.get("status") or "").lower()
    now = _timestamp()
    elapsed = round(time.time() - float(metadata.get("startedAt", time.time())), 2)

    with _TASKS_LOCK:
        metadata["heartbeatCount"] += 1
        metadata["elapsedSeconds"] = elapsed
        metadata["lastHeartbeatAt"] = now
        metadata["agentStatus"] = status or execution.get("status")
        task["status"]["timestamp"] = now
        task["status"]["message"] = _agent_message(
            task_id,
            task["contextId"],
            {
                "status": "working",
                "node_id": metadata["nodeId"],
                "multica_job_id": job_id,
                "agent_status": status,
                "elapsed_seconds": elapsed,
            },
        )

    if status in MULTICA_JOB_COMPLETED_STATUSES:
        return _complete_multica_job_task(task_id, execution, elapsed)
    if status in MULTICA_JOB_FAILED_STATUSES:
        return _fail_multica_job_task(task_id, execution, elapsed)

    with _TASKS_LOCK:
        current_task = copy.deepcopy(_TASKS[task_id]["task"])
    return {"done": False, "task": current_task, "event": None}


def _complete_multica_job_task(
    task_id: str,
    execution: dict[str, Any],
    elapsed: float,
) -> dict[str, Any]:
    with _TASKS_LOCK:
        record = _TASKS[task_id]
        task = record["task"]
        metadata = task["metadata"]
        metadata["elapsedSeconds"] = elapsed
        metadata["finishedAt"] = execution.get("completed_at") or _timestamp()
        metadata["agentStatus"] = execution.get("status")
        if not record["result_artifact_created"]:
            result = _multica_job_result(metadata, execution, elapsed)
            task["artifacts"] = [_artifact_from_result(result, metadata)]
            record["result_artifact_created"] = True
        task["status"] = {
            "state": "TASK_STATE_COMPLETED",
            "message": _agent_message(
                task_id,
                task["contextId"],
                {
                    "status": "completed",
                    "node_id": metadata["nodeId"],
                    "multica_job_id": metadata["multicaJobId"],
                    "agent_status": execution.get("status"),
                },
            ),
            "timestamp": _timestamp(),
        }
        completed_task = copy.deepcopy(task)
    _store_idempotent_task(completed_task)
    event = _event_from_a2a_task(
        {
            "workflow_id": _metadata_string(metadata, "workflow_id"),
            "node_id": metadata["nodeId"],
            "a2a_task_id": task_id,
            "status": "completed",
        },
        completed_task,
    )
    return {"done": True, "task": completed_task, "event": event}


def _fail_multica_job_task(
    task_id: str,
    execution: dict[str, Any],
    elapsed: float,
) -> dict[str, Any]:
    error = (
        execution.get("error")
        or execution.get("failure_reason")
        or f"Multica job ended with status {execution.get('status')}"
    )
    with _TASKS_LOCK:
        task = _TASKS[task_id]["task"]
        metadata = task["metadata"]
        metadata["elapsedSeconds"] = elapsed
        metadata["finishedAt"] = execution.get("completed_at") or _timestamp()
        metadata["error"] = str(error)
        metadata["agentStatus"] = execution.get("status")
        task["status"] = {
            "state": "TASK_STATE_FAILED",
            "message": _agent_message(
                task_id,
                task["contextId"],
                {
                    "status": "failed",
                    "node_id": metadata["nodeId"],
                    "multica_job_id": metadata["multicaJobId"],
                    "error": str(error),
                },
            ),
            "timestamp": _timestamp(),
        }
        failed_task = copy.deepcopy(task)
    _store_idempotent_task(failed_task)
    event = _event_from_a2a_task(
        {
            "workflow_id": _metadata_string(metadata, "workflow_id"),
            "node_id": metadata["nodeId"],
            "a2a_task_id": task_id,
            "status": "failed",
            "error": str(error),
        },
        failed_task,
    )
    return {"done": True, "task": failed_task, "event": event}


def _complete_multica_task(
    task_id: str,
    multica_task: dict[str, Any],
    elapsed: float,
) -> dict[str, Any]:
    comments = _safe_agent_service_get(
        f"/tasks/{_task_identifier(multica_task)}/comments",
        {"recent": str(_agent_result_recent_comments())},
    )
    runs = _safe_agent_service_get(f"/tasks/{_task_identifier(multica_task)}/runs")
    messages = None
    if _agent_fetch_run_messages():
        messages = _latest_run_messages(_task_identifier(multica_task), runs)
    with _TASKS_LOCK:
        record = _TASKS[task_id]
        task = record["task"]
        metadata = task["metadata"]
        metadata["elapsedSeconds"] = elapsed
        metadata["finishedAt"] = _timestamp()
        metadata["agentStatus"] = multica_task.get("status")
        if not record["result_artifact_created"]:
            result = _multica_result(metadata, multica_task, comments, runs, messages, elapsed)
            task["artifacts"] = [_artifact_from_result(result, metadata)]
            record["result_artifact_created"] = True
        task["status"] = {
            "state": "TASK_STATE_COMPLETED",
            "message": _agent_message(
                task_id,
                task["contextId"],
                {
                    "status": "completed",
                    "node_id": metadata["nodeId"],
                    "agent_service_task_id": metadata["agentServiceTaskId"],
                    "agent_status": multica_task.get("status"),
                },
            ),
            "timestamp": _timestamp(),
        }
        completed_task = copy.deepcopy(task)
    _store_idempotent_task(completed_task)
    event = _event_from_a2a_task(
        {
            "workflow_id": _metadata_string(metadata, "workflow_id"),
            "node_id": metadata["nodeId"],
            "a2a_task_id": task_id,
            "status": "completed",
        },
        completed_task,
    )
    return {"done": True, "task": completed_task, "event": event}


def _fail_multica_task(
    task_id: str,
    multica_task: dict[str, Any],
    elapsed: float,
) -> dict[str, Any]:
    error = f"Multica task ended with status {multica_task.get('status')}"
    with _TASKS_LOCK:
        task = _TASKS[task_id]["task"]
        metadata = task["metadata"]
        metadata["elapsedSeconds"] = elapsed
        metadata["finishedAt"] = _timestamp()
        metadata["error"] = error
        metadata["agentStatus"] = multica_task.get("status")
        task["status"] = {
            "state": "TASK_STATE_FAILED",
            "message": _agent_message(
                task_id,
                task["contextId"],
                {
                    "status": "failed",
                    "node_id": metadata["nodeId"],
                    "agent_service_task_id": metadata["agentServiceTaskId"],
                    "error": error,
                },
            ),
            "timestamp": _timestamp(),
        }
        failed_task = copy.deepcopy(task)
    _store_idempotent_task(failed_task)
    event = _event_from_a2a_task(
        {
            "workflow_id": _metadata_string(metadata, "workflow_id"),
            "node_id": metadata["nodeId"],
            "a2a_task_id": task_id,
            "status": "failed",
            "error": error,
        },
        failed_task,
    )
    return {"done": True, "task": failed_task, "event": event}


def _multica_result(
    metadata: dict[str, Any],
    multica_task: dict[str, Any],
    comments: dict[str, Any] | None,
    runs: dict[str, Any] | None,
    messages: dict[str, Any] | None,
    elapsed: float,
) -> dict[str, Any]:
    latest_comment_raw = _latest_comment_raw_text(comments)
    latest_comment = _truncate_text(latest_comment_raw, _agent_result_text_limit())
    structured_output = _extract_structured_agent_output(latest_comment_raw)
    payload = {
        "status": multica_task.get("status"),
        "agent_backend": MULTICA_BACKEND,
        "agent_key": metadata.get("agentKey"),
        "requested_agent_key": metadata.get("requestedAgentKey"),
        "context_policy": metadata.get("contextPolicy"),
        "runtime_profile": metadata.get("runtimeProfile"),
        "execution_mode": metadata.get("executionMode"),
        "agent_id": metadata.get("agentId"),
        "agent_name": metadata.get("agentName"),
        "agent_service_task_id": metadata["agentServiceTaskId"],
        "title": multica_task.get("title"),
        "latest_comment": latest_comment,
        "comments": _compact_comments(comments),
        "runs": _compact_runs(runs),
        "messages": _compact_messages(messages),
    }
    if structured_output:
        payload["structured_output"] = structured_output
        for key, value in structured_output.items():
            if key not in payload:
                payload[key] = value
    return {
        "node": metadata["nodeId"],
        "operation": metadata["operation"],
        "duration_seconds": elapsed,
        "payload": payload,
    }


def _multica_job_result(
    metadata: dict[str, Any],
    execution: dict[str, Any],
    elapsed: float,
) -> dict[str, Any]:
    output_raw = execution.get("final_reply")
    if output_raw is None:
        output_raw = execution.get("result")
    output = _truncate_text(None if output_raw is None else str(output_raw), _agent_result_text_limit())
    structured_output = _extract_structured_agent_output(None if output_raw is None else str(output_raw))
    payload = {
        "status": execution.get("status"),
        "agent_backend": MULTICA_JOB_BACKEND,
        "agent_id": metadata.get("agentId"),
        "agent_name": metadata.get("agentName"),
        "context_policy": metadata.get("contextPolicy"),
        "runtime_profile": metadata.get("runtimeProfile"),
        "execution_mode": metadata.get("executionMode"),
        "multica_job_id": metadata["multicaJobId"],
        "latest_comment": output,
        "final_reply": output,
        "result": output,
        "messages": _compact_messages(execution),
        "created_at": execution.get("created_at"),
        "started_at": execution.get("started_at"),
        "completed_at": execution.get("completed_at"),
    }
    if structured_output:
        payload["structured_output"] = structured_output
        for key, value in structured_output.items():
            if key not in payload:
                payload[key] = value
    return {
        "node": metadata["nodeId"],
        "operation": metadata["operation"],
        "duration_seconds": elapsed,
        "payload": payload,
    }


def _hermes_result(
    metadata: dict[str, Any],
    job: dict[str, Any],
    elapsed: float,
) -> dict[str, Any]:
    output = str(job.get("result") or "").strip()
    latest_comment = _truncate_text(output, _agent_result_text_limit())
    structured_output = _extract_structured_agent_output(output)
    payload = {
        "status": "completed",
        "agent_backend": HERMES_BACKEND,
        "agent_key": metadata.get("agentKey"),
        "requested_agent_key": metadata.get("requestedAgentKey"),
        "context_policy": metadata.get("contextPolicy"),
        "runtime_profile": metadata.get("runtimeProfile"),
        "execution_mode": metadata.get("executionMode"),
        "hermes_job_id": metadata.get("hermesJobId"),
        "hermes_command": metadata.get("hermesCommand"),
        "hermes_workdir": metadata.get("hermesWorkdir"),
        "latest_comment": latest_comment,
        "stdout": latest_comment,
        "trace": [
            {
                "type": "runtime.submit",
                "title": "Submitted Hermes one-shot runtime job",
                "timestamp": _timestamp_from_epoch(job.get("started_at")),
            },
            {
                "type": "runtime.complete",
                "title": "Hermes one-shot returned final output",
                "timestamp": _timestamp(),
                "duration_seconds": elapsed,
            },
        ],
    }
    if structured_output:
        payload["structured_output"] = structured_output
        for key, value in structured_output.items():
            if key not in payload:
                payload[key] = value
    return {
        "node": metadata["nodeId"],
        "operation": metadata["operation"],
        "duration_seconds": elapsed,
        "payload": payload,
    }


def _hermes_output_validation_error(metadata: dict[str, Any], output: str) -> str | None:
    if not output:
        return "Hermes returned an empty final output"
    if _looks_like_hermes_clarification(output):
        return (
            "Hermes did not execute the node task; it returned a clarification/tool request "
            f"instead. Output preview: {_truncate_text(output, 240)}"
        )
    if metadata.get("expectedOutputFormat") == "json_object" and not _json_object_from_text(output):
        return (
            "Hermes output did not contain a valid JSON object even though the node requested "
            f"JSON. Output preview: {_truncate_text(output, 240)}"
        )
    return None


def _looks_like_hermes_clarification(output: str) -> bool:
    text = output.strip()
    lower = text.lower()
    clarification_markers = [
        "请告诉我您需要",
        "请告诉我您想要",
        "请告诉我您",
        "请提供您想要",
        "请提供实际的图片",
        "请提供图像",
        "请提供图片",
        "我没有看到具体的任务内容",
        "没有提供具体的任务内容",
        "没有提供具体任务内容",
        "内容似乎是空的",
        "需要更多信息来理解",
        "provide an image",
        "image url",
        "vision_analyze",
    ]
    if any(marker in text or marker in lower for marker in clarification_markers):
        return True
    question_like = ["您需要我帮助您完成什么", "您想要实现什么目标", "或者直接告诉我任务"]
    return any(marker in text for marker in question_like)


def _artifact_from_result(
    result: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "artifactId": f"artifact-{uuid.uuid4()}",
        "name": "dag-node-result",
        "description": f"Result artifact for {metadata['nodeId']}",
        "parts": [{"data": result, "mediaType": "application/json"}],
        "metadata": {
            "nodeId": metadata["nodeId"],
            "operation": metadata["operation"],
            "producedByAgent": metadata["agentCard"]["name"],
            "backend": metadata.get("backend", SIMULATOR_BACKEND),
            "simulatorJobId": metadata.get("simulatorJobId"),
            "agentServiceTaskId": metadata.get("agentServiceTaskId"),
            "multicaJobId": metadata.get("multicaJobId"),
            "hermesJobId": metadata.get("hermesJobId"),
            "contextPolicy": metadata.get("contextPolicy"),
            "runtimeProfile": metadata.get("runtimeProfile"),
            "executionMode": metadata.get("executionMode"),
        },
    }


def _dependency_results_from_artifacts(
    artifacts: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    dependency_results: dict[str, dict[str, Any]] = {}
    for artifact in artifacts:
        if artifact.get("name") != "dag-node-result":
            continue
        result = _first_data_part({"parts": artifact.get("parts", [])})
        dependency_results[result["node"]] = result
    return dependency_results


def _first_data_part(message: dict[str, Any]) -> dict[str, Any]:
    for part in message.get("parts", []):
        if "data" in part:
            return part["data"]
    raise ValueError("A2A message does not contain a data part")


def _agent_message(task_id: str, context_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "messageId": f"msg-{uuid.uuid4()}",
        "contextId": context_id,
        "taskId": task_id,
        "role": "ROLE_AGENT",
        "parts": [{"data": data, "mediaType": "application/json"}],
        "metadata": {"runtime": "sandbox-agent-runtime"},
    }


def _event_from_a2a_task(callback: dict[str, Any], completed_task: dict[str, Any]) -> dict[str, Any]:
    return {
        "workflow_id": callback.get("workflow_id"),
        "node_id": callback["node_id"],
        "a2a_task_id": completed_task["id"],
        "status": callback["status"],
        "error": callback.get("error"),
        "a2a_state": completed_task["status"]["state"],
        "a2a_task": completed_task,
    }


def _stable_runtime_id(prefix: str, node_id: str, idempotency_key: str | None) -> str:
    if not idempotency_key:
        return f"{prefix}-{node_id}-{uuid.uuid4()}"
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:20]
    safe_node_id = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(node_id)).strip("-") or "node"
    return f"{prefix}-{safe_node_id}-{digest}"


def _simulator_agent_card(node: dict[str, Any]) -> dict[str, Any]:
    operation = node.get("operation", "merge")
    return {
        "name": f"{node['id']}-simulator-agent",
        "description": (
            f"Simulator agent for control-flow node {node['id']}; dependencies are exchanged "
            "with A2A messages and artifacts"
        ),
        "supportedInterfaces": [
            {
                "transport": "JSONRPC",
                "url": "local://sandbox-agent-runtime/message:send",
            }
        ],
        "provider": {"organization": "MAOS Sandbox Runtime"},
        "version": "1.0.0",
        "capabilities": {
            "streaming": False,
            "pushNotifications": False,
            "extendedAgentCard": False,
        },
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "skills": [
            {
                "id": operation,
                "name": operation,
                "description": f"Executes the {operation} control-flow operation",
                "inputModes": ["application/json"],
                "outputModes": ["application/json"],
            }
        ],
        "protocolVersions": ["1.0"],
    }


def _multica_agent_card(node: dict[str, Any]) -> dict[str, Any]:
    agent = _node_agent_config(node)
    agent_name = agent.get("agent_name") or agent.get("agent_key") or agent.get("agent_id") or "multica-agent"
    return {
        "name": str(agent_name),
        "description": f"Multica-backed real Agent Service for control-flow node {node['id']}",
        "supportedInterfaces": [
            {
                "transport": "HTTP+AgentService",
                "url": _agent_service_api_base(),
            }
        ],
        "provider": {"organization": "Multica"},
        "version": "0.1.0",
        "capabilities": {
            "streaming": True,
            "pushNotifications": False,
            "extendedAgentCard": False,
        },
        "defaultInputModes": ["text/markdown", "application/json"],
        "defaultOutputModes": ["text/markdown", "application/json"],
        "skills": [
            {
                "id": str(agent.get("agent_key") or node.get("operation", "agent_task")),
                "name": str(agent_name),
                "description": "Executes a real Multica task and reports status through AgentService",
                "inputModes": ["text/markdown", "application/json"],
                "outputModes": ["text/markdown", "application/json"],
            }
        ],
        "protocolVersions": ["1.0-adapter"],
    }


def _multica_job_agent_card(node: dict[str, Any]) -> dict[str, Any]:
    agent = _node_agent_config(node)
    agent_name = agent.get("agent_name") or agent.get("agent_id") or "multica-native-job-agent"
    return {
        "name": str(agent_name),
        "description": f"Multica native Job API agent for control-flow node {node['id']}",
        "supportedInterfaces": [
            {
                "transport": "HTTP+MulticaJobAPI",
                "url": _multica_job_api_base(),
            }
        ],
        "provider": {"organization": "Multica"},
        "version": "0.1.0-native-job",
        "capabilities": {
            "streaming": True,
            "pushNotifications": False,
            "extendedAgentCard": False,
        },
        "defaultInputModes": ["text/markdown", "application/json"],
        "defaultOutputModes": ["text/markdown", "application/json"],
        "skills": [
            {
                "id": str(agent.get("agent_id") or node.get("operation", "agent_task")),
                "name": str(agent_name),
                "description": "Executes a Multica native job and returns the job final reply as the node result",
                "inputModes": ["text/markdown", "application/json"],
                "outputModes": ["text/markdown", "application/json"],
            }
        ],
        "protocolVersions": ["1.0-native-job-adapter"],
    }


def _hermes_agent_card(node: dict[str, Any]) -> dict[str, Any]:
    agent = _node_agent_config(node)
    agent_name = agent.get("agent_name") or agent.get("agent_key") or "hermes-oneshot-agent"
    return {
        "name": str(agent_name),
        "description": f"Direct Hermes one-shot Agent runtime for control-flow node {node['id']}",
        "supportedInterfaces": [
            {
                "transport": "local-process",
                "url": _hermes_bin(),
            }
        ],
        "provider": {"organization": "Hermes Agent Runtime"},
        "version": "0.16.0-adapter",
        "capabilities": {
            "streaming": False,
            "pushNotifications": False,
            "extendedAgentCard": False,
        },
        "defaultInputModes": ["text/markdown", "application/json"],
        "defaultOutputModes": ["text/markdown", "application/json"],
        "skills": [
            {
                "id": str(agent.get("agent_key") or node.get("operation", "agent_task")),
                "name": str(agent_name),
                "description": "Executes a local Hermes one-shot invocation and returns stdout as the node result",
                "inputModes": ["text/markdown", "application/json"],
                "outputModes": ["text/markdown", "application/json"],
            }
        ],
        "protocolVersions": ["1.0-adapter"],
    }


def _node_agent_config(node: dict[str, Any]) -> dict[str, Any]:
    agent = node.get("agent", {})
    return agent if isinstance(agent, dict) else {}


def _provider_for_node(node: dict[str, Any]) -> AgentRuntimeProvider:
    return _provider_for_backend(_node_backend(node), node)


def _provider_for_backend(
    backend: str,
    node: dict[str, Any] | None = None,
) -> AgentRuntimeProvider:
    node_id = node["id"] if node else None
    return provider_for_backend(backend, node_id=node_id)


def _node_backend(node: dict[str, Any]) -> str:
    agent = _node_agent_config(node)
    backend = (
        agent.get("backend")
        or node.get("backend")
        or node.get("runtime")
        or os.environ.get("DEFAULT_AGENT_BACKEND")
        or SIMULATOR_BACKEND
    )
    return normalize_backend(str(backend))


def _multica_context_policy(node: dict[str, Any]) -> str:
    agent = _node_agent_config(node)
    value = (
        agent.get("context_policy")
        or node.get("context_policy")
        or os.environ.get("DEFAULT_MULTICA_CONTEXT_POLICY")
        or PROVIDED_CONTEXT_ONLY
    )
    return str(value).lower().replace("-", "_")


def _multica_runtime_profile(node: dict[str, Any]) -> str:
    agent = _node_agent_config(node)
    value = (
        agent.get("runtime_profile")
        or agent.get("execution_profile")
        or node.get("runtime_profile")
        or os.environ.get("DEFAULT_MULTICA_RUNTIME_PROFILE")
    )
    if value:
        return str(value).lower().replace("-", "_")
    return "maos_compact_agent"


def _multica_execution_mode(node: dict[str, Any]) -> str:
    agent = _node_agent_config(node)
    value = agent.get("execution_mode") or node.get("execution_mode")
    if value:
        return str(value).lower().replace("-", "_")
    if _is_lightweight_multica_node(node):
        return "lightweight_hermes_oneshot"
    return "multica"


def _is_lightweight_multica_node(node: dict[str, Any]) -> bool:
    agent = _node_agent_config(node)
    execution_mode = str(agent.get("execution_mode") or node.get("execution_mode") or "").lower()
    execution_mode = execution_mode.replace("-", "_")
    runtime_profile = _multica_runtime_profile(node)
    if execution_mode in {"multica", "full_multica", "codex_cli"}:
        return False
    if execution_mode in {"lightweight", "lightweight_hermes_oneshot", "hermes", "hermes_oneshot"}:
        return True
    if runtime_profile in {"full", "full_multica", "codex", "codex_cli", "maos_compact", "maos_compact_agent"}:
        return False
    if runtime_profile in {"lightweight", "hermes", "hermes_oneshot", PROVIDED_CONTEXT_ONLY}:
        return True
    return False


def _multica_dispatch_agent_key(node: dict[str, Any]) -> str | None:
    agent = _node_agent_config(node)
    if not _is_lightweight_multica_node(node):
        return agent.get("agent_key") or node.get("agent_key")
    return (
        agent.get("dispatch_agent_key")
        or os.environ.get("LIGHTWEIGHT_MULTICA_AGENT_KEY")
        or LIGHTWEIGHT_MULTICA_AGENT_KEY
    )


def _multica_requested_agent_name(node: dict[str, Any]) -> str | None:
    agent = _node_agent_config(node)
    value = agent.get("agent_name") or node.get("agent_name")
    return None if value is None else str(value)


def _multica_title(node: dict[str, Any], workflow_id: str) -> str:
    title = _node_agent_config(node).get("title") or node.get("title") or node.get("label") or node["id"]
    return f"[MAOS:{workflow_id}] {title}"


def _multica_description(
    node: dict[str, Any],
    dependency_results: dict[str, dict[str, Any]],
    graph_input: dict[str, Any],
    workflow_id: str,
    a2a_task_id: str,
) -> str:
    context_policy = _multica_context_policy(node)
    runtime_profile = _multica_runtime_profile(node)
    instruction = (
        _node_agent_config(node).get("prompt")
        or node.get("prompt")
        or node.get("description")
        or "Execute this control-flow node and return the result as a concise comment."
    )
    context = {
        "workflow_id": workflow_id,
        "node_id": node["id"],
        "a2a_task_id": a2a_task_id,
        "context_policy": context_policy,
        "runtime_profile": runtime_profile,
        "node": _node_context_payload(node),
        "graph_input": graph_input,
        "dependency_results": dependency_results,
    }
    policy_text = _context_policy_instructions(context_policy, runtime_profile)
    return (
        "# MAOS Control-Flow Node Task\n\n"
        "You are executing one node in a persistent multi-agent control-flow graph. "
        "Use the dependency results as upstream context. When finished, add a concise result comment "
        "and move the Multica issue to in_review or done.\n\n"
        "## Context Policy\n\n"
        f"{policy_text}\n\n"
        "## Node Instruction\n\n"
        f"{instruction}\n\n"
        "## A2A Context Payload\n\n"
        "```json\n"
        f"{_safe_json(context, _multica_description_limit(node))}\n"
        "```\n"
    )


def _multica_job_prompt(
    node: dict[str, Any],
    dependency_results: dict[str, dict[str, Any]],
    graph_input: dict[str, Any],
    workflow_id: str,
    a2a_task_id: str,
    context_id: str,
) -> str:
    agent = _node_agent_config(node)
    instruction = (
        agent.get("prompt")
        or node.get("prompt")
        or node.get("description")
        or "Execute this control-flow node and return the final result."
    )
    context = {
        "workflow_id": workflow_id,
        "node_id": node["id"],
        "a2a_task_id": a2a_task_id,
        "context_id": context_id,
        "node": _node_context_payload(node),
        "graph_input": graph_input,
        "dependency_results": dependency_results,
    }
    return (
        "# MAOS DAG Node Native Job\n\n"
        "You are executing one node in a MAOS DAG as a Multica native job. "
        "The whole DAG is tracked by MAOS/Temporal; this job is only the current node.\n\n"
        "Do not call Multica issue commands. Do not create or update issues. "
        "Do not add issue comments or change issue status. Return the node result directly "
        "as your final assistant output; MAOS will read it from the Job API final reply.\n\n"
        "## Node Instruction\n\n"
        f"{instruction}\n\n"
        "## A2A Context Payload\n\n"
        "```json\n"
        f"{_safe_json(context, _multica_job_prompt_limit(node))}\n"
        "```\n"
    )


def _context_policy_instructions(context_policy: str, runtime_profile: str) -> str:
    if context_policy == PROVIDED_CONTEXT_ONLY:
        return (
            f"Policy: {context_policy}; runtime profile: {runtime_profile}. "
            "Use only the A2A Context Payload in this task. Do not inspect repositories, "
            "workspace files, previous runs, comments, metadata, external documents, or web pages. "
            "Do not use tool-heavy coding-agent capabilities unless the node instruction explicitly "
            "requires repository work. Produce the node result from the provided payload."
        )
    return (
        f"Policy: {context_policy}; runtime profile: {runtime_profile}. "
        "Use the A2A Context Payload as the authoritative task input. Read external workspace "
        "or repository context only when the node instruction explicitly requires it."
    )


def _node_context_payload(node: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key in (
        "id",
        "label",
        "type",
        "operation",
        "deps",
        "join",
        "max_visits",
        "params",
        "timeout_seconds",
    ):
        if key in node:
            payload[key] = node[key]
    agent = _node_agent_config(node)
    if agent:
        payload["agent"] = {
            key: agent[key]
            for key in (
                "backend",
                "agent_key",
                "agent_id",
                "agent_name",
                "context_policy",
                "runtime_profile",
                "execution_profile",
                "execution_mode",
            )
            if key in agent
        }
    return payload


def _hermes_task_context(node: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key in (
        "id",
        "label",
        "type",
        "operation",
        "deps",
        "join",
        "max_visits",
        "params",
    ):
        if key in node:
            payload[key] = node[key]
    return payload


def _hermes_expected_output_hint(instruction: str) -> dict[str, Any]:
    lower = instruction.lower()
    wants_json = "json" in lower or "字段包含" in instruction or "字段包括" in instruction
    return {
        "format": "json_object" if wants_json else "structured_chinese_text",
        "do_not_ask_clarifying_questions": True,
        "continue_with_reasonable_assumptions": True,
    }


def _hermes_dependency_context(
    dependency_results: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    compact: dict[str, dict[str, Any]] = {}
    for node_id, result in dependency_results.items():
        payload = result.get("payload", {}) if isinstance(result, dict) else {}
        if not isinstance(payload, dict):
            payload = {}
        useful_payload = {}
        for key in (
            "structured_output",
            "decision",
            "reason",
            "latest_comment",
            "stdout",
            "status",
            "risk_level",
        ):
            if key in payload:
                useful_payload[key] = payload[key]
        for key, value in payload.items():
            if key.startswith("hermes_") or key in {
                "trace",
                "comments",
                "runs",
                "messages",
                "agent_backend",
                "agent_key",
                "requested_agent_key",
                "context_policy",
                "runtime_profile",
                "execution_mode",
            }:
                continue
            if key not in useful_payload:
                useful_payload[key] = value
        compact[node_id] = {
            "node": result.get("node", node_id) if isinstance(result, dict) else node_id,
            "operation": result.get("operation") if isinstance(result, dict) else None,
            "duration_seconds": result.get("duration_seconds") if isinstance(result, dict) else None,
            "payload": useful_payload,
        }
    return compact


def _multica_metadata(
    node: dict[str, Any],
    workflow_id: str,
    task_id: str,
    context_id: str,
    dependency_results: dict[str, dict[str, Any]],
    graph_input: dict[str, Any],
    context_policy: str,
    runtime_profile: str,
    execution_mode: str,
    dispatch_agent_key: str | None,
    requested_agent_key: Any,
    idempotency_key: str | None,
) -> dict[str, str | int | float | bool]:
    agent = _node_agent_config(node)
    dependency_json = _safe_json(dependency_results)
    graph_input_json = _safe_json(graph_input)
    metadata: dict[str, str | int | float | bool] = {
        "maos_task": True,
        "maos_compact_bootstrap": _maos_compact_bootstrap_enabled(agent, node, runtime_profile),
        "maos_backend": MULTICA_BACKEND,
        "execution_mode": execution_mode,
        "runtime_profile": runtime_profile,
        "context_policy": context_policy,
        "comment_history_policy": str(
            agent.get("comment_history_policy")
            or node.get("comment_history_policy")
            or "disabled"
        ),
        "metadata_policy": str(
            agent.get("metadata_policy")
            or node.get("metadata_policy")
            or "disabled"
        ),
        "skill_loading_policy": str(
            agent.get("skill_loading_policy")
            or node.get("skill_loading_policy")
            or "none"
        ),
        "maos_allowed_skill_slugs": _allowed_skill_slugs(agent, node),
        "tool_policy": str(agent.get("tool_policy", "no_external_tools")),
        "workflow_id": workflow_id,
        "node_id": node["id"],
        "a2a_task_id": task_id,
        "idempotency_key": idempotency_key or "",
        "context_id": context_id,
        "operation": str(node.get("operation", "agent_task")),
        "dispatch_agent_key": dispatch_agent_key or "",
        "requested_agent_key": "" if requested_agent_key is None else str(requested_agent_key),
        "requested_agent_name": _multica_requested_agent_name(node) or "",
        "dependency_node_ids": ",".join(sorted(dependency_results)),
        "dependency_result_bytes": len(dependency_json.encode("utf-8")),
        "graph_input_bytes": len(graph_input_json.encode("utf-8")),
    }
    if _include_payload_metadata(node):
        metadata["dependency_results_json"] = _safe_json(
            dependency_results,
            _payload_metadata_limit(node),
        )
        metadata["graph_input_json"] = _safe_json(graph_input, _payload_metadata_limit(node))
    return metadata


def _find_multica_task_by_idempotency_key(idempotency_key: str | None) -> dict[str, Any] | None:
    if not idempotency_key:
        return None
    response = _safe_agent_service_get("/tasks", {"limit": "200"})
    tasks = _records_from_response(response)
    for task in tasks:
        metadata = task.get("metadata") if isinstance(task, dict) else None
        if isinstance(metadata, dict) and metadata.get("idempotency_key") == idempotency_key:
            return task
    return None


def _find_multica_job_by_idempotency_key(idempotency_key: str | None) -> dict[str, Any] | None:
    if not idempotency_key:
        return None
    with _MULTICA_JOB_LOCK:
        job = _MULTICA_JOB_BY_IDEMPOTENCY.get(idempotency_key)
    return copy.deepcopy(job) if job else None


def _records_from_response(response: dict[str, Any] | list[Any] | None) -> list[dict[str, Any]]:
    if response is None:
        return []
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    for key in ("data", "issues", "tasks", "items", "results"):
        value = response.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _maos_compact_bootstrap_enabled(
    agent: dict[str, Any],
    node: dict[str, Any],
    runtime_profile: str,
) -> bool:
    value = agent.get("maos_compact_bootstrap", node.get("maos_compact_bootstrap"))
    if isinstance(value, bool):
        return value
    if value is not None:
        return str(value).lower() in {"1", "true", "yes", "on"}
    return runtime_profile in {"maos_compact", "maos_compact_agent"}


def _allowed_skill_slugs(agent: dict[str, Any], node: dict[str, Any]) -> str:
    value = agent.get("allowed_skill_slugs", node.get("allowed_skill_slugs", ""))
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    return str(value or "")


def _include_payload_metadata(node: dict[str, Any]) -> bool:
    agent = _node_agent_config(node)
    value = agent.get("include_payload_metadata", node.get("include_payload_metadata", False))
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "on"}


def _payload_metadata_limit(node: dict[str, Any]) -> int:
    agent = _node_agent_config(node)
    value = agent.get("payload_metadata_limit", node.get("payload_metadata_limit"))
    if value is None:
        value = os.environ.get("MULTICA_PAYLOAD_METADATA_LIMIT", "2000")
    return int(value)


def _hermes_context_policy(node: dict[str, Any]) -> str:
    agent = _node_agent_config(node)
    value = (
        agent.get("context_policy")
        or node.get("context_policy")
        or os.environ.get("DEFAULT_HERMES_CONTEXT_POLICY")
        or PROVIDED_CONTEXT_ONLY
    )
    return str(value).lower().replace("-", "_")


def _hermes_runtime_profile(node: dict[str, Any]) -> str:
    agent = _node_agent_config(node)
    value = (
        agent.get("runtime_profile")
        or agent.get("execution_profile")
        or node.get("runtime_profile")
        or os.environ.get("DEFAULT_HERMES_RUNTIME_PROFILE")
        or "hermes_oneshot"
    )
    return str(value).lower().replace("-", "_")


def _hermes_execution_mode(node: dict[str, Any]) -> str:
    agent = _node_agent_config(node)
    value = agent.get("execution_mode") or node.get("execution_mode") or "hermes_oneshot"
    return str(value).lower().replace("-", "_")


def _hermes_prompt(
    node: dict[str, Any],
    dependency_results: dict[str, dict[str, Any]],
    graph_input: dict[str, Any],
    workflow_id: str,
    a2a_task_id: str,
    context_policy: str,
    runtime_profile: str,
) -> str:
    instruction = (
        _node_agent_config(node).get("prompt")
        or node.get("prompt")
        or node.get("description")
        or "请完成当前业务子任务，并返回最终结果。"
    )
    task = {
        **_hermes_task_context(node),
        "instruction": instruction,
        "expected_output": _hermes_expected_output_hint(instruction),
    }
    dependency_context = _hermes_dependency_context(dependency_results)
    prompt_limit = _hermes_prompt_limit(node)
    a2a_message = {
        "kind": "message",
        "messageId": f"task-input-{node['id']}",
        "role": "user",
        "parts": [
            {
                "kind": "text",
                "text": instruction,
                "metadata": {"purpose": "primary-task-instruction"},
            },
            {
                "kind": "data",
                "data": {
                    "task": task,
                    "input": graph_input,
                    "upstream_result_node_ids": sorted(dependency_context),
                    "upstream_results": "See the upstream results section in this prompt.",
                },
                "metadata": {"purpose": "task-context"},
            },
        ],
    }
    output_hint = task["expected_output"]["format"]
    return (
        "# 请完成这个业务子任务\n\n"
        "你现在收到的是一个工作流节点任务。请把下面的“任务指令”当作唯一主任务并直接完成，"
        "不要解释 A2A 协议，不要询问任务是什么，不要要求用户补充图片或文件。\n"
        "如果信息不足，请基于已给输入做合理假设，并在最终结果里说明假设。\n\n"
        "## 任务指令（必须执行）\n\n"
        f"{instruction}\n\n"
        "## 原始业务输入\n\n"
        "```json\n"
        f"{_safe_json(graph_input, prompt_limit)}\n"
        "```\n\n"
        "## 上游节点结果\n\n"
        "```json\n"
        f"{_safe_json(dependency_context, prompt_limit)}\n"
        "```\n\n"
        "## A2A 消息载荷\n\n"
        "下面的 JSON 是同一任务的 A2A 结构化载荷，仅作为输入数据。"
        "请执行 `parts[0].text` / `parts[1].data.task.instruction`，不要把这段载荷当成要解释的对象。\n\n"
        "```json\n"
        f"{_safe_json(a2a_message, min(prompt_limit, 5000))}\n"
        "```\n\n"
        "## 执行边界\n\n"
        "- 只完成 `task.instruction` 描述的业务任务。\n"
        "- 不要创建新的任务、工作流、issue、脚本、测试文件或本地 API 请求。\n"
        "- 不要调用 http://127.0.0.1:8765、http://127.0.0.1:8767、http://127.0.0.1:8091。\n"
        "- 不要运行终端命令，不要读写仓库文件，除非任务指令明确要求修改文件。\n\n"
        "## 最终输出\n\n"
        f"- 期望输出格式：{output_hint}。\n"
        "- 如果任务指令要求 JSON，请只输出一个合法 JSON 对象，不要附加 markdown 代码块或解释。\n"
        "- 如果任务指令没有要求 JSON，请用中文输出结构化结果。\n"
        "- 不要输出对执行过程、工具、运行时或编排系统的说明。\n"
    )


def _hermes_command(node: dict[str, Any], prompt: str) -> tuple[list[str], str]:
    agent = _node_agent_config(node)
    max_prompt_chars = _hermes_command_prompt_limit()
    if len(prompt) > max_prompt_chars:
        raise RuntimeError(
            "Hermes prompt is too large for Windows command-line one-shot mode: "
            f"{len(prompt)} chars > {max_prompt_chars} chars. "
            "Reduce prompt_payload_limit or upstream context size."
        )
    args: list[str] = []
    if _truthy(agent.get("ignore_user_config", node.get("ignore_user_config", True))):
        args.append("--ignore-user-config")
    if _truthy(agent.get("ignore_rules", node.get("ignore_rules", True))):
        args.append("--ignore-rules")
    model = agent.get("model") or node.get("model") or os.environ.get("HERMES_PROVIDER_MODEL")
    provider = agent.get("provider") or node.get("provider") or os.environ.get("HERMES_PROVIDER_PROVIDER")
    toolsets = (
        agent.get("toolsets")
        or node.get("toolsets")
        or os.environ.get("HERMES_PROVIDER_TOOLSETS")
        or "search"
    )
    skills = agent.get("skills") or node.get("skills")
    if model:
        args.extend(["--model", str(model)])
    if provider:
        args.extend(["--provider", str(provider)])
    if toolsets:
        args.extend(["--toolsets", _csv_value(toolsets)])
    if skills:
        args.extend(["--skills", _csv_value(skills)])
    args.extend(["-z", prompt])

    hermes_bin = _resolved_hermes_bin(_hermes_bin())
    if hermes_bin.lower().endswith((".cmd", ".bat")):
        command = [os.environ.get("COMSPEC", "cmd.exe"), "/c", hermes_bin] + args
        display = f"cmd.exe /c {hermes_bin} {' '.join(args[:-1])} <prompt:{len(prompt)} chars>"
    else:
        command = [hermes_bin] + args
        display = f"{hermes_bin} {' '.join(args[:-1])} <prompt:{len(prompt)} chars>"
    return command, display


def _run_hermes_oneshot(
    command: list[str],
    workdir: str,
    timeout_seconds: float,
    env: dict[str, str],
) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=workdir,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Hermes binary not found: {command[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Hermes command timed out after {timeout_seconds}s") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(detail or f"Hermes command failed with exit code {completed.returncode}")
    return completed.stdout.strip()


def _hermes_env() -> dict[str, str]:
    env = os.environ.copy()
    git_bash = os.environ.get("HERMES_GIT_BASH_PATH") or r"D:\Program Files\Git\bin\bash.exe"
    if git_bash:
        env.setdefault("HERMES_GIT_BASH_PATH", git_bash)
    return env


def _hermes_bin() -> str:
    return os.environ.get("HERMES_BIN", r"D:\dev\MAOS\AgentRuntime\.venv\Scripts\hermes.exe")


def _resolved_hermes_bin(value: str) -> str:
    if not value.lower().endswith((".cmd", ".bat")):
        return value
    root = os.path.dirname(os.path.abspath(value))
    direct_exe = os.path.join(root, ".venv", "Scripts", "hermes.exe")
    if os.path.exists(direct_exe):
        return direct_exe
    return value


def _hermes_workdir(node: dict[str, Any]) -> str:
    agent = _node_agent_config(node)
    value = agent.get("workdir") or node.get("workdir") or os.environ.get("HERMES_WORKDIR")
    if value:
        return str(value)
    return os.path.dirname(os.path.abspath(__file__))


def _hermes_prompt_limit(node: dict[str, Any]) -> int:
    agent = _node_agent_config(node)
    value = agent.get("prompt_payload_limit", node.get("prompt_payload_limit"))
    if value is None:
        value = os.environ.get("HERMES_PROMPT_PAYLOAD_LIMIT", "12000")
    return int(value)


def _hermes_command_prompt_limit() -> int:
    return int(os.environ.get("HERMES_COMMAND_PROMPT_LIMIT", "26000"))


def _hermes_poll_seconds() -> float:
    return float(os.environ.get("HERMES_PROVIDER_POLL_SECONDS", "30"))


def _csv_value(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    return str(value)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "on"}


def _multica_description_limit(node: dict[str, Any]) -> int:
    agent = _node_agent_config(node)
    value = agent.get("description_payload_limit", node.get("description_payload_limit"))
    if value is None:
        value = os.environ.get("MULTICA_DESCRIPTION_PAYLOAD_LIMIT", "60000")
    return int(value)


def _multica_job_prompt_limit(node: dict[str, Any]) -> int:
    agent = _node_agent_config(node)
    value = agent.get("prompt_payload_limit", node.get("prompt_payload_limit"))
    if value is None:
        value = os.environ.get("MULTICA_JOB_PROMPT_PAYLOAD_LIMIT", "60000")
    return int(value)


def _safe_json(value: Any, limit: int = 8000) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if len(encoded) <= limit:
        return encoded
    return encoded[:limit] + "...<truncated>"


def _task_identifier(task: dict[str, Any] | None) -> str | None:
    if not isinstance(task, dict):
        return None
    value = task.get("id") or task.get("task_id") or task.get("issue_id")
    return None if value is None else str(value)


def _job_identifier(job: dict[str, Any] | None) -> str | None:
    if not isinstance(job, dict):
        return None
    value = job.get("job_id") or job.get("id") or job.get("task_id")
    return None if value is None else str(value)


def _request_multica_job_api(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    params: dict[str, str] | None = None,
) -> dict[str, Any]:
    return _request_json(
        _multica_job_api_base(),
        method,
        path,
        payload,
        params,
        headers=_multica_job_headers(),
    )


def _multica_job_headers() -> dict[str, str]:
    token = _multica_job_token()
    if not token:
        raise RuntimeError("MULTICA_JOB_TOKEN is required for backend=multica_job")
    authorization = token if token.lower().startswith("bearer ") else f"Bearer {token}"
    headers = {"Authorization": authorization}
    workspace_id = _multica_job_workspace_id()
    workspace_slug = _multica_job_workspace_slug()
    if workspace_id:
        headers["X-Workspace-ID"] = workspace_id
    elif workspace_slug:
        headers["X-Workspace-Slug"] = workspace_slug
    else:
        raise RuntimeError(
            "MULTICA_JOB_WORKSPACE_ID or MULTICA_JOB_WORKSPACE_SLUG is required for backend=multica_job"
        )
    return headers


def _latest_run_messages(task_id: str | None, runs: dict[str, Any] | None) -> dict[str, Any] | None:
    run_id = _latest_run_id(runs)
    if not task_id or not run_id:
        return None
    return _safe_agent_service_get(f"/runs/{run_id}/messages", {"issue_id": task_id})


def _latest_run_id(runs_response: dict[str, Any] | None) -> str | None:
    if not runs_response:
        return None
    runs = runs_response.get("data", runs_response)
    if isinstance(runs, dict):
        runs = runs.get("runs") or runs.get("items") or runs.get("data")
    if not isinstance(runs, list) or not runs:
        return None
    latest = max(
        runs,
        key=lambda item: item.get("created_at") or item.get("started_at") or item.get("updated_at") or "",
    )
    value = latest.get("id") or latest.get("task_id") or latest.get("run_id")
    return None if value is None else str(value)


def _latest_comment_text(comments_response: dict[str, Any] | None) -> str | None:
    return _truncate_text(_latest_comment_raw_text(comments_response), _agent_result_text_limit())


def _latest_comment_raw_text(comments_response: dict[str, Any] | None) -> str | None:
    comments = _response_list(comments_response)
    for comment in reversed(comments):
        text = _text_from_record(comment)
        if text:
            return text
    return None


def _extract_structured_agent_output(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    json_object = _json_object_from_text(text)
    if json_object:
        return _normalize_agent_decision_payload(json_object)
    return _extract_decision_from_text(text)


def _json_object_from_text(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    candidates = _json_candidates_from_text(text)
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except Exception:
            continue
        if isinstance(value, dict):
            return value
    return {}


def _json_candidates_from_text(text: str) -> list[str]:
    candidates: list[str] = []
    for match in re.finditer(r"```(?:json|JSON)?\s*(.*?)\s*```", text, re.DOTALL):
        fenced = match.group(1).strip()
        if fenced:
            candidates.extend(_balanced_json_objects(fenced))
    stripped = text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        candidates.append(stripped)
    candidates.extend(_balanced_json_objects(stripped))
    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            deduped.append(candidate)
    return deduped


def _balanced_json_objects(text: str) -> list[str]:
    objects: list[str] = []
    start: int | None = None
    depth = 0
    in_string = False
    escape = False
    for index, char in enumerate(text):
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == "{":
            if depth == 0:
                start = index
            depth += 1
        elif char == "}" and depth:
            depth -= 1
            if depth == 0 and start is not None:
                objects.append(text[start : index + 1].strip())
                start = None
    return objects


def _normalize_agent_decision_payload(value: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(value)
    decision = (
        normalized.get("decision")
        or normalized.get("route")
        or normalized.get("next_step")
        or normalized.get("下一步")
        or normalized.get("结论")
        or normalized.get("决定")
    )
    if decision is not None:
        normalized["decision"] = _normalize_decision_value(decision)
    return normalized


def _extract_decision_from_text(text: str) -> dict[str, Any]:
    lower = text.lower()
    if "needs_revision" in lower or "need_revision" in lower or "requires_revision" in lower:
        return {"decision": "needs_revision"}
    if "approved" in lower or "approve" in lower:
        return {"decision": "approved"}
    if "需要补充" in text or "需要修改" in text or "需要返工" in text or "不通过" in text:
        return {"decision": "needs_revision"}
    if "通过" in text or "批准" in text or "同意进入" in text:
        return {"decision": "approved"}
    return {}


def _normalize_decision_value(value: Any) -> str:
    text = str(value).strip().lower()
    text = text.replace("-", "_").replace(" ", "_")
    mapping = {
        "approve": "approved",
        "approved": "approved",
        "pass": "approved",
        "passed": "approved",
        "go": "approved",
        "通过": "approved",
        "批准": "approved",
        "同意": "approved",
        "needs_revision": "needs_revision",
        "need_revision": "needs_revision",
        "requires_revision": "needs_revision",
        "revise": "needs_revision",
        "revision": "needs_revision",
        "retry": "needs_revision",
        "需要补充": "needs_revision",
        "需要修改": "needs_revision",
        "需要返工": "needs_revision",
        "不通过": "needs_revision",
    }
    return mapping.get(text, text)


def _compact_comments(comments_response: dict[str, Any] | None) -> list[dict[str, Any]]:
    records = _response_list(comments_response)
    compact = []
    for record in records[-5:]:
        compact.append(
            {
                "id": record.get("id"),
                "author": record.get("author") or record.get("author_name") or record.get("user_name"),
                "created_at": record.get("created_at"),
                "text": _truncate_text(_text_from_record(record), _agent_result_text_limit()),
            }
        )
    return compact


def _compact_runs(runs_response: dict[str, Any] | None) -> list[dict[str, Any]]:
    records = _response_list(runs_response)
    compact = []
    for record in records[-5:]:
        compact.append(
            {
                "id": record.get("id") or record.get("run_id") or record.get("task_id"),
                "status": record.get("status") or record.get("state"),
                "created_at": record.get("created_at"),
                "started_at": record.get("started_at"),
                "finished_at": record.get("finished_at"),
            }
        )
    return compact


def _compact_messages(messages_response: dict[str, Any] | None) -> list[dict[str, Any]]:
    records = _response_list(messages_response)
    compact = []
    for record in records[-10:]:
        compact.append(
            {
                "id": record.get("id") or record.get("message_id"),
                "sequence": record.get("sequence") or record.get("seq") or record.get("sequence_number"),
                "role": record.get("role") or record.get("author") or record.get("type"),
                "type": record.get("type"),
                "tool": record.get("tool"),
                "created_at": record.get("created_at"),
                "text": _truncate_text(_text_from_record(record), _agent_message_text_limit()),
            }
        )
    return compact


def _response_list(response: dict[str, Any] | list[Any] | None) -> list[dict[str, Any]]:
    if response is None:
        return []
    value: Any = response
    if isinstance(value, dict):
        value = value.get("data", value)
    if isinstance(value, dict):
        for key in ("comments", "messages", "runs", "items", "issues"):
            if isinstance(value.get(key), list):
                value = value[key]
                break
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _text_from_record(record: dict[str, Any]) -> str | None:
    for key in ("content", "text", "body", "message", "summary", "answer"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    value = record.get("parts")
    if isinstance(value, list):
        pieces = []
        for part in value:
            if isinstance(part, dict):
                text = part.get("text") or part.get("content")
                if isinstance(text, str) and text.strip():
                    pieces.append(text.strip())
        if pieces:
            return "\n".join(pieces)
    return None


def _truncate_text(text: str | None, limit: int) -> str | None:
    if text is None or len(text) <= limit:
        return text
    return text[:limit] + "...<truncated>"


def _metadata_string(metadata: dict[str, Any], key: str) -> str | None:
    value = metadata.get(key)
    return None if value is None else str(value)


def _safe_agent_service_get(path: str, params: dict[str, str] | None = None) -> dict[str, Any] | None:
    try:
        return _request_json(_agent_service_api_base(), "GET", path, params=params)
    except Exception:
        return None


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _timestamp_from_epoch(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(value)))
    except Exception:
        return None
