import ast
import asyncio
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

from maos_runtime.a2a_runtime import extract_result_from_task, poll_task, send_message


ACTIVITY_TIMEOUT_SECONDS = 90
DISPATCH_RETRY_POLICY = RetryPolicy(maximum_attempts=5)
DEFAULT_NODE_TIMEOUT_SECONDS = 24 * 60 * 60
DEFAULT_MAX_NODE_VISITS = 1
DEFAULT_MAX_TOTAL_VISITS = 100


@activity.defn
async def dispatch_agent_node(activity_input: dict[str, Any]) -> dict[str, Any]:
    node = activity_input["node"]
    dependency_executions = activity_input["dependency_executions"]
    dependency_artifacts = [
        artifact
        for execution in dependency_executions.values()
        for artifact in execution["a2a_task"].get("artifacts", [])
    ]
    reference_task_ids = [
        execution["a2a_task"]["id"] for execution in dependency_executions.values()
    ]
    activity.heartbeat({"phase": "agent-invoke", "node_id": node["id"]})
    message = {
        "messageId": f"temporal-msg-{node['id']}-{node.get('_visit', 1)}",
        "contextId": activity_input["context_id"],
        "role": "ROLE_USER",
        "parts": [
            {
                "data": {
                    "node": _public_node_spec(node),
                    "dependency_artifacts": dependency_artifacts,
                    "graph_input": activity_input.get("graph_input", {}),
                    "control_flow": {
                        "visit": node.get("_visit", 1),
                        "instance_id": node.get("_instance_id"),
                        "triggered_by": sorted(dependency_executions),
                    },
                },
                "mediaType": "application/json",
            }
        ],
        "referenceTaskIds": reference_task_ids,
        "metadata": {
            "protocol": "A2A",
            "purpose": "dependency-data-transfer",
            "operation": node.get("operation", "merge"),
            "visit": node.get("_visit", 1),
            "instance_id": node.get("_instance_id"),
        },
    }
    return send_message(
        {
            "message": message,
            "configuration": {"returnImmediately": True},
            "metadata": {
                "binding": "local-jsonrpc-style",
                "workflow_id": activity_input["workflow_id"],
                "node_id": node["id"],
                "instance_id": node.get("_instance_id"),
                "idempotency_key": activity_input["idempotency_key"],
            },
        }
    )


@activity.defn
async def poll_agent_node(activity_input: dict[str, Any]) -> dict[str, Any]:
    activity.heartbeat(
        {
            "phase": "agent-status-poll",
            "node_id": activity_input["node_id"],
            "a2a_task_id": activity_input["id"],
        }
    )
    return poll_task(activity_input)


@workflow.defn
class JsonDagWorkflow:
    """Temporal workflow for JSON control-flow graphs.

    The historic class name is intentionally kept so existing visibility queries
    and old workflow histories remain readable. New JSON can use explicit
    `edges`, conditional `when` expressions, and bounded loops.
    """

    def __init__(self) -> None:
        self._graph_id = ""
        self._graph_name = ""
        self._graph_type = "control_flow"
        self._workflow_status = "pending"
        self._nodes: dict[str, dict[str, Any]] = {}
        self._levels: list[list[str]] = []
        self._edges: list[dict[str, Any]] = []
        self._agent_events: dict[str, dict[str, Any]] = {}
        self._visits: dict[str, int] = {}
        self._instances: list[dict[str, Any]] = []
        self._latest_completed: dict[str, dict[str, Any]] = {}
        self._all_results: dict[str, Any] = {}

    @workflow.run
    async def run(self, graph: dict[str, Any]) -> dict[str, Any]:
        spec = normalize_graph(graph)
        self._initialize_graph(spec)
        self._workflow_status = "running"

        node_specs = {node["id"]: node for node in spec["nodes"]}
        outgoing = _outgoing_edges(spec["edges"])
        predecessor_ids = _predecessor_ids(spec["edges"])
        arrivals: dict[str, dict[str, dict[str, Any]]] = {
            node_id: {} for node_id in node_specs
        }
        running: dict[str, asyncio.Task] = {}
        failure_error: str | None = None
        total_visits = 0
        max_total_visits = int(spec.get("max_total_visits", DEFAULT_MAX_TOTAL_VISITS))

        for node_id in spec["start_nodes"]:
            arrivals[node_id]["__start__"] = self._start_execution()

        while True:
            ready_nodes = [
                node_id
                for node_id in sorted(node_specs)
                if failure_error is None
                and node_id not in running
                and self._node_is_ready(node_specs[node_id], arrivals[node_id], predecessor_ids)
            ]

            if not ready_nodes and not running:
                blocked_by_visit_limit = [
                    node_id
                    for node_id, node in sorted(node_specs.items())
                    if arrivals[node_id]
                    and self._visits.get(node_id, 0) >= _node_max_visits(node)
                ]
                if blocked_by_visit_limit and failure_error is None:
                    node_id = blocked_by_visit_limit[0]
                    failure_error = (
                        f"Node {node_id} received another control-flow arrival but "
                        f"already reached max_visits={_node_max_visits(node_specs[node_id])}"
                    )
                if failure_error is not None:
                    self._workflow_status = "failed"
                    return self._workflow_result(spec, "failed", failure_error)
                self._workflow_status = "completed"
                return self._workflow_result(spec, "completed")

            for node_id in ready_nodes:
                total_visits += 1
                if total_visits > max_total_visits:
                    failure_error = (
                        f"Control-flow graph exceeded max_total_visits={max_total_visits}; "
                        "check loop exit conditions"
                    )
                    break

                visit = self._visits.get(node_id, 0) + 1
                max_visits = _node_max_visits(node_specs[node_id])
                if visit > max_visits:
                    failure_error = (
                        f"Node {node_id} exceeded max_visits={max_visits}; "
                        "check branch conditions"
                    )
                    break

                self._visits[node_id] = visit
                instance_id = f"{node_id}#{visit}"
                node_state = self._nodes[node_id]
                node_state["visits"] = visit
                node_state["current_instance_id"] = instance_id
                node_state["max_visits"] = max_visits
                dependency_executions = self._dependency_executions_for_node(
                    node_specs[node_id],
                    arrivals[node_id],
                )
                arrivals[node_id] = {}
                running[node_id] = asyncio.create_task(
                    self._execute_node(
                        {
                            **node_specs[node_id],
                            "_visit": visit,
                            "_instance_id": instance_id,
                        },
                        dependency_executions,
                        spec.get("input", {}),
                    )
                )

            if failure_error is not None and not running:
                self._workflow_status = "failed"
                return self._workflow_result(spec, "failed", failure_error)

            if not running:
                continue

            done_tasks, _ = await workflow.wait(
                running.values(),
                return_when="FIRST_COMPLETED",
            )

            finished_ids = [
                node_id
                for node_id, task in running.items()
                if task in done_tasks
            ]
            for node_id in sorted(finished_ids):
                try:
                    execution = await running.pop(node_id)
                except Exception as exc:
                    execution = self._failed_execution(node_specs[node_id], str(exc))
                    self._mark_node_failed(node_id, str(exc))

                if execution.get("status") == "failed":
                    failure_error = execution.get("error") or "Agent execution failed"
                    self._workflow_status = "failed"
                    continue

                self._latest_completed[node_id] = execution
                instance_id = execution.get("instance_id") or f"{node_id}#{self._visits[node_id]}"
                self._all_results[instance_id] = execution["result"]["payload"]
                self._all_results[node_id] = execution["result"]["payload"]

                outgoing_edges = outgoing.get(node_id, [])
                taken_edges = 0
                for edge in outgoing_edges:
                    if self._edge_is_enabled(edge, node_id, execution, spec):
                        to_node = edge["to"]
                        arrivals[to_node][node_id] = execution
                        self._record_edge_taken(edge)
                        taken_edges += 1
                    else:
                        self._record_edge_skipped(edge)
                if (
                    outgoing_edges
                    and taken_edges == 0
                    and all(edge.get("when") for edge in outgoing_edges)
                ):
                    failure_error = (
                        f"Node {node_id} completed but no outgoing branch condition matched"
                    )
                    self._workflow_status = "failed"

            if failure_error is not None and not running:
                return self._workflow_result(spec, "failed", failure_error)

    @workflow.signal
    def agent_node_completed(self, event: dict[str, Any]) -> None:
        node_id = event.get("node_id")
        if not node_id:
            return
        self._agent_events[node_id] = event
        node_state = self._nodes.get(node_id)
        if node_state:
            node_state["a2a_state"] = event.get("a2a_state", "TASK_STATE_COMPLETED")
            node_state["last_heartbeat_at"] = workflow.now().isoformat()
            node_state["heartbeat_count"] += 1
            node_state["summary"] = "agent callback received"
            self._update_latest_instance(node_id, {"summary": "agent callback received"})

    @workflow.query
    def graph_state(self) -> dict[str, Any]:
        return {
            "graph_id": self._graph_id,
            "graph_name": self._graph_name,
            "graph_type": self._graph_type,
            "workflow_status": self._workflow_status,
            "levels": self._levels,
            "edges": self._edges,
            "nodes": list(self._nodes.values()),
            "instances": list(self._instances),
            "results": {
                node_id: execution["result"]["payload"]
                for node_id, execution in self._latest_completed.items()
                if "result" in execution
            },
            "instance_results": dict(self._all_results),
        }

    def _initialize_graph(self, spec: dict[str, Any]) -> None:
        self._graph_id = spec["id"]
        self._graph_name = spec.get("name", spec["id"])
        self._graph_type = spec.get("graph_type", "control_flow")
        self._levels = spec["levels"]
        self._edges = [
            {
                "from": edge["from"],
                "to": edge["to"],
                "label": edge.get("label") or _edge_label(edge),
                "when": edge.get("when"),
                "taken_count": 0,
                "skipped_count": 0,
            }
            for edge in spec["edges"]
        ]
        self._visits = {node["id"]: 0 for node in spec["nodes"]}
        self._nodes = {
            node["id"]: {
                "id": node["id"],
                "label": node.get("label", node["id"]),
                "type": _node_type(node),
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
                "multica_job_id": None,
                "hermes_job_id": None,
                "hermes_prompt": None,
                "agent_status": None,
                "backend": _node_backend_for_display(node),
                "completion_mode": None,
                "a2a_task_id": None,
                "a2a_state": None,
                "agent_input_payload": None,
                "agent_name": None,
                "summary": "",
                "visits": 0,
                "max_visits": _node_max_visits(node),
                "current_instance_id": None,
                "instances": [],
            }
            for node in spec["nodes"]
        }

    def _node_is_ready(
        self,
        node_spec: dict[str, Any],
        arrivals: dict[str, dict[str, Any]],
        predecessor_ids: dict[str, set[str]],
    ) -> bool:
        if not arrivals:
            return False
        node_id = node_spec["id"]
        if self._visits.get(node_id, 0) >= _node_max_visits(node_spec):
            return False
        join = str(node_spec.get("join", "all")).lower()
        if join in {"any", "race", "first"}:
            return True
        required = predecessor_ids.get(node_id, set())
        if not required:
            return "__start__" in arrivals
        return required.issubset(set(arrivals))

    def _dependency_executions_for_node(
        self,
        node_spec: dict[str, Any],
        arrivals: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        dependencies = {
            node_id: execution
            for node_id, execution in arrivals.items()
            if node_id != "__start__"
        }
        for dep in node_spec.get("deps", []):
            if dep in self._latest_completed and dep not in dependencies:
                dependencies[dep] = self._latest_completed[dep]
        return dependencies

    async def _execute_node(
        self,
        node_spec: dict[str, Any],
        dependency_executions: dict[str, Any],
        graph_input: dict[str, Any],
    ) -> dict[str, Any]:
        if _is_condition_node(node_spec):
            return await self._execute_condition_node(
                node_spec,
                dependency_executions,
                graph_input,
            )
        return await self._execute_agent_node(
            node_spec,
            dependency_executions,
            graph_input,
        )

    async def _execute_condition_node(
        self,
        node_spec: dict[str, Any],
        dependency_executions: dict[str, Any],
        graph_input: dict[str, Any],
    ) -> dict[str, Any]:
        node_id = node_spec["id"]
        node_state = self._nodes[node_id]
        now = workflow.now().isoformat()
        node_state["status"] = "running"
        node_state["started_at"] = now
        self._record_instance_start(node_spec, "condition", now)

        payload = {
            "node": node_id,
            "operation": node_spec.get("operation", "condition"),
            "status": "completed",
            "decision": _resolve_value(node_spec.get("decision", "evaluated"), graph_input, self._all_results),
            "visit": node_spec.get("_visit", 1),
            "dependency_node_ids": sorted(dependency_executions),
        }
        if isinstance(node_spec.get("params"), dict):
            payload.update(_resolve_value(node_spec["params"], graph_input, self._all_results))

        finished_at = workflow.now().isoformat()
        result = {
            "node": node_id,
            "operation": node_spec.get("operation", "condition"),
            "duration_seconds": 0,
            "payload": payload,
        }
        node_state["status"] = "completed"
        node_state["finished_at"] = finished_at
        node_state["duration_seconds"] = 0
        node_state["elapsed_seconds"] = 0
        node_state["summary"] = f"condition evaluated; visit={node_spec.get('_visit', 1)}"
        self._finish_latest_instance(node_id, "completed", finished_at, node_state["summary"])
        return {
            "status": "completed",
            "result": result,
            "a2a_task": {
                "id": f"control-flow-{node_spec['_instance_id']}",
                "artifacts": [
                    {
                        "name": "dag-node-result",
                        "parts": [{"data": result, "mediaType": "application/json"}],
                    }
                ],
                "metadata": {"backend": "control-flow", "nodeId": node_id},
                "status": {"state": "TASK_STATE_COMPLETED"},
            },
            "instance_id": node_spec["_instance_id"],
        }

    async def _execute_agent_node(
        self,
        node_spec: dict[str, Any],
        dependency_executions: dict[str, Any],
        graph_input: dict[str, Any],
    ) -> dict[str, Any]:
        # Agent 节点只在派发和轮询时短暂占用 worker。真正等待 Agent 工作时，
        # workflow 状态由 Temporal history 持久化，worker 线程可以释放。
        node_state = self._nodes[node_spec["id"]]
        node_state["status"] = "running"
        node_state["started_at"] = workflow.now().isoformat()
        self._record_instance_start(node_spec, "agent", node_state["started_at"])
        a2a_task: dict[str, Any] | None = None

        try:
            send_response = await workflow.execute_activity(
                dispatch_agent_node,
                {
                    "node": node_spec,
                    "dependency_executions": dependency_executions,
                    "graph_input": graph_input,
                    "context_id": workflow.info().workflow_id,
                    "workflow_id": workflow.info().workflow_id,
                    "idempotency_key": (
                        f"{workflow.info().workflow_id}:"
                        f"{node_spec['_instance_id']}:dispatch"
                    ),
                },
                start_to_close_timeout=timedelta(seconds=ACTIVITY_TIMEOUT_SECONDS),
                heartbeat_timeout=timedelta(seconds=ACTIVITY_TIMEOUT_SECONDS),
                retry_policy=DISPATCH_RETRY_POLICY,
            )
            a2a_task = send_response["task"]
            a2a_metadata = a2a_task.get("metadata", {})
            node_state["agent_input_payload"] = _agent_input_payload(a2a_task)
            node_state["a2a_task_id"] = a2a_task["id"]
            node_state["backend"] = a2a_metadata.get("backend")
            node_state["completion_mode"] = a2a_metadata.get("completionMode")
            node_state["simulator_job_id"] = a2a_metadata.get("simulatorJobId")
            node_state["agent_service_task_id"] = a2a_metadata.get("agentServiceTaskId")
            node_state["multica_job_id"] = a2a_metadata.get("multicaJobId")
            node_state["hermes_job_id"] = a2a_metadata.get("hermesJobId")
            node_state["hermes_prompt"] = a2a_metadata.get("hermesPrompt")
            node_state["agent_status"] = a2a_metadata.get("agentStatus")
            node_state["a2a_state"] = a2a_task["status"]["state"]
            node_state["agent_name"] = a2a_metadata.get("agentCard", {}).get("name")
            node_state["planned_duration_seconds"] = a2a_metadata.get(
                "plannedDurationSeconds"
            )
            node_state["status"] = "suspended"
            node_state["summary"] = (
                f"workflow is durably waiting for agent callback from "
                f"{node_state['agent_name']}"
            )
            self._update_latest_instance(
                node_spec["id"],
                {
                    "status": "suspended",
                    "backend": node_state["backend"],
                    "completion_mode": node_state["completion_mode"],
                    "agent_name": node_state["agent_name"],
                    "a2a_task_id": node_state["a2a_task_id"],
                    "agent_input_payload": node_state["agent_input_payload"],
                    "agent_service_task_id": node_state["agent_service_task_id"],
                    "multica_job_id": node_state["multica_job_id"],
                    "simulator_job_id": node_state["simulator_job_id"],
                    "hermes_job_id": node_state["hermes_job_id"],
                    "summary": node_state["summary"],
                },
            )
            timeout_seconds = float(
                node_spec.get("timeout_seconds", DEFAULT_NODE_TIMEOUT_SECONDS)
            )
            if a2a_metadata.get("completionMode") == "polling":
                event = await self._poll_until_agent_complete(
                    node_spec,
                    a2a_task,
                    node_state,
                    timeout_seconds,
                )
            else:
                await workflow.wait_condition(
                    lambda: node_spec["id"] in self._agent_events,
                    timeout=timedelta(seconds=timeout_seconds),
                )
                event = self._agent_events.pop(node_spec["id"])
            if event.get("status") == "failed":
                raise RuntimeError(event.get("error") or "Agent execution failed")

            a2a_task = event["a2a_task"]
            a2a_metadata = a2a_task.get("metadata", {})
            node_state["a2a_state"] = a2a_task["status"]["state"]
            node_state["elapsed_seconds"] = a2a_metadata.get("elapsedSeconds", 0)
            node_state["heartbeat_count"] = a2a_metadata.get(
                "heartbeatCount",
                node_state["heartbeat_count"],
            )
            node_state["last_heartbeat_at"] = a2a_metadata.get(
                "lastHeartbeatAt",
                workflow.now().isoformat(),
            )
            if a2a_task["status"]["state"] == "TASK_STATE_FAILED":
                status_message = a2a_task["status"].get("message", {})
                raise RuntimeError(str(status_message))
            result = extract_result_from_task(a2a_task)
        except Exception as exc:
            error = str(exc)
            self._mark_node_failed(node_spec["id"], error)
            self._workflow_status = "failed"
            return self._failed_execution(node_spec, error, a2a_task)

        node_state["status"] = "completed"
        node_state["finished_at"] = workflow.now().isoformat()
        node_state["duration_seconds"] = result["duration_seconds"]
        node_state["elapsed_seconds"] = result["duration_seconds"]
        node_state["summary"] = _summarize_result(result)
        result_payload = result.get("payload", {})
        self._update_latest_instance(
            node_spec["id"],
            {
                "backend": node_state.get("backend"),
                "completion_mode": node_state.get("completion_mode"),
                "agent_name": node_state.get("agent_name"),
                "a2a_task_id": node_state.get("a2a_task_id"),
                "agent_service_task_id": (
                    result_payload.get("agent_service_task_id")
                    or node_state.get("agent_service_task_id")
                ),
                "multica_job_id": (
                    result_payload.get("multica_job_id")
                    or node_state.get("multica_job_id")
                ),
                "simulator_job_id": node_state.get("simulator_job_id"),
                "hermes_job_id": (
                    result_payload.get("hermes_job_id")
                    or node_state.get("hermes_job_id")
                ),
                "agent_status": result_payload.get("status") or node_state.get("agent_status"),
                "elapsed_seconds": node_state.get("elapsed_seconds"),
                "heartbeat_count": node_state.get("heartbeat_count"),
                "last_heartbeat_at": node_state.get("last_heartbeat_at"),
            },
        )
        self._finish_latest_instance(
            node_spec["id"],
            "completed",
            node_state["finished_at"],
            node_state["summary"],
        )
        return {
            "status": "completed",
            "result": result,
            "a2a_task": a2a_task,
            "instance_id": node_spec["_instance_id"],
        }

    def _edge_is_enabled(
        self,
        edge: dict[str, Any],
        from_node_id: str,
        execution: dict[str, Any],
        spec: dict[str, Any],
    ) -> bool:
        condition = edge.get("when")
        if condition in (None, "", True):
            return True
        if condition is False:
            return False
        context = {
            "input": spec.get("input", {}),
            "results": self._all_results,
            "deps": self._all_results,
            "last": execution["result"]["payload"],
            "result": execution["result"]["payload"],
            "node": {"id": from_node_id, "visit": self._visits.get(from_node_id, 0)},
            "visits": self._visits,
            "attempts": self._visits,
        }
        return bool(_safe_eval_condition(str(condition), context))

    def _workflow_result(
        self,
        spec: dict[str, Any],
        status: str,
        error: str | None = None,
    ) -> dict[str, Any]:
        return {
            "graph_id": spec["id"],
            "graph_name": spec.get("name", spec["id"]),
            "graph_type": spec.get("graph_type", "control_flow"),
            "status": status,
            "node_count": len(spec["nodes"]),
            "instance_count": len(self._instances),
            "error": error,
            "results": {
                node_id: execution["result"]["payload"]
                for node_id, execution in self._latest_completed.items()
                if "result" in execution
            },
            "instance_results": dict(self._all_results),
            "state": self.graph_state(),
        }

    def _failed_execution(
        self,
        node_spec: dict[str, Any],
        error: str,
        a2a_task: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        node_state = self._nodes.get(node_spec["id"], {})
        duration_seconds = float(node_state.get("elapsed_seconds") or 0)
        return {
            "status": "failed",
            "error": error,
            "instance_id": node_spec.get("_instance_id"),
            "result": {
                "node": node_spec["id"],
                "operation": node_spec.get("operation", "agent_task"),
                "duration_seconds": duration_seconds,
                "payload": {
                    "status": "failed",
                    "error": error,
                    "agent_backend": node_state.get("backend"),
                    "agent_name": node_state.get("agent_name"),
                    "agent_service_task_id": node_state.get("agent_service_task_id"),
                    "multica_job_id": node_state.get("multica_job_id"),
                    "a2a_task_id": node_state.get("a2a_task_id"),
                },
            },
            "a2a_task": a2a_task or {"id": node_state.get("a2a_task_id"), "artifacts": []},
        }

    def _mark_node_failed(self, node_id: str, error: str) -> None:
        node_state = self._nodes.get(node_id)
        if not node_state:
            return
        node_state["status"] = "failed"
        node_state["finished_at"] = workflow.now().isoformat()
        node_state["duration_seconds"] = node_state.get("elapsed_seconds") or 0
        node_state["summary"] = error
        self._finish_latest_instance(node_id, "failed", node_state["finished_at"], error)

    async def _poll_until_agent_complete(
        self,
        node_spec: dict[str, Any],
        a2a_task: dict[str, Any],
        node_state: dict[str, Any],
        timeout_seconds: float,
    ) -> dict[str, Any]:
        a2a_metadata = a2a_task.get("metadata", {})
        poll_seconds = float(a2a_metadata.get("pollSeconds", 30))
        deadline = workflow.now() + timedelta(seconds=timeout_seconds)

        while True:
            poll_response = await workflow.execute_activity(
                poll_agent_node,
                {
                    "id": a2a_task["id"],
                    "task": a2a_task,
                    "node_id": node_spec["id"],
                    "workflow_id": workflow.info().workflow_id,
                },
                start_to_close_timeout=timedelta(seconds=ACTIVITY_TIMEOUT_SECONDS),
                heartbeat_timeout=timedelta(seconds=ACTIVITY_TIMEOUT_SECONDS),
            )
            latest_task = (
                (poll_response.get("event") or {}).get("a2a_task")
                or poll_response.get("task")
                or a2a_task
            )
            a2a_task = latest_task
            self._update_node_from_a2a_task(node_state, latest_task)

            if poll_response.get("done"):
                event = poll_response.get("event")
                if event:
                    return event
                return {
                    "workflow_id": workflow.info().workflow_id,
                    "node_id": node_spec["id"],
                    "a2a_task_id": latest_task["id"],
                    "status": (
                        "completed"
                        if latest_task["status"]["state"] == "TASK_STATE_COMPLETED"
                        else "failed"
                    ),
                    "a2a_state": latest_task["status"]["state"],
                    "a2a_task": latest_task,
                }

            remaining_seconds = (deadline - workflow.now()).total_seconds()
            if remaining_seconds <= 0:
                raise TimeoutError(
                    f"Agent node {node_spec['id']} did not complete within "
                    f"{timeout_seconds} seconds"
                )
            await workflow.sleep(timedelta(seconds=min(poll_seconds, remaining_seconds)))

    def _update_node_from_a2a_task(
        self,
        node_state: dict[str, Any],
        a2a_task: dict[str, Any],
    ) -> None:
        a2a_metadata = a2a_task.get("metadata", {})
        node_state["a2a_state"] = a2a_task["status"]["state"]
        node_state["elapsed_seconds"] = a2a_metadata.get(
            "elapsedSeconds",
            node_state["elapsed_seconds"],
        )
        node_state["heartbeat_count"] = a2a_metadata.get(
            "heartbeatCount",
            node_state["heartbeat_count"],
        )
        node_state["last_heartbeat_at"] = a2a_metadata.get(
            "lastHeartbeatAt",
            workflow.now().isoformat(),
        )
        node_state["agent_status"] = a2a_metadata.get(
            "agentStatus",
            node_state.get("agent_status"),
        )
        node_state["summary"] = (
            f"workflow is durably polling {node_state.get('agent_name') or 'agent'}; "
            f"agent status={node_state.get('agent_status') or 'unknown'}"
        )
        self._update_latest_instance(
            node_state["id"],
            {
                "status": node_state["status"],
                "elapsed_seconds": node_state["elapsed_seconds"],
                "heartbeat_count": node_state["heartbeat_count"],
                "last_heartbeat_at": node_state["last_heartbeat_at"],
                "agent_status": node_state["agent_status"],
                "summary": node_state["summary"],
            },
        )

    def _record_instance_start(
        self,
        node_spec: dict[str, Any],
        kind: str,
        started_at: str,
    ) -> None:
        instance = {
            "id": node_spec["_instance_id"],
            "node_id": node_spec["id"],
            "visit": node_spec.get("_visit", 1),
            "kind": kind,
            "status": "running",
            "started_at": started_at,
            "finished_at": None,
            "backend": self._nodes[node_spec["id"]].get("backend"),
            "agent_name": None,
            "a2a_task_id": None,
            "summary": "",
        }
        self._instances.append(instance)
        self._nodes[node_spec["id"]]["instances"] = self._recent_node_instances(node_spec["id"])

    def _update_latest_instance(self, node_id: str, patch: dict[str, Any]) -> None:
        for instance in reversed(self._instances):
            if instance["node_id"] == node_id:
                instance.update(patch)
                break
        if node_id in self._nodes:
            self._nodes[node_id]["instances"] = self._recent_node_instances(node_id)

    def _finish_latest_instance(
        self,
        node_id: str,
        status: str,
        finished_at: str,
        summary: str,
    ) -> None:
        self._update_latest_instance(
            node_id,
            {
                "status": status,
                "finished_at": finished_at,
                "summary": summary,
            },
        )

    def _recent_node_instances(self, node_id: str) -> list[dict[str, Any]]:
        return [
            instance
            for instance in self._instances
            if instance["node_id"] == node_id
        ][-6:]

    def _record_edge_taken(self, edge: dict[str, Any]) -> None:
        for state_edge in self._edges:
            if state_edge["from"] == edge["from"] and state_edge["to"] == edge["to"]:
                state_edge["taken_count"] += 1
                return

    def _record_edge_skipped(self, edge: dict[str, Any]) -> None:
        for state_edge in self._edges:
            if state_edge["from"] == edge["from"] and state_edge["to"] == edge["to"]:
                state_edge["skipped_count"] += 1
                return

    def _start_execution(self) -> dict[str, Any]:
        return {
            "status": "completed",
            "result": {
                "node": "__start__",
                "operation": "start",
                "duration_seconds": 0,
                "payload": {"status": "completed"},
            },
            "a2a_task": {"id": "__start__", "artifacts": []},
        }


JsonControlFlowWorkflow = JsonDagWorkflow


def validate_graph(graph: dict[str, Any]) -> None:
    normalize_graph(graph)


def normalize_graph(graph: dict[str, Any]) -> dict[str, Any]:
    if not graph.get("id"):
        raise ValueError("Graph must define an id")
    if not isinstance(graph.get("nodes"), list) or not graph["nodes"]:
        raise ValueError("Graph must define a non-empty nodes list")

    seen: set[str] = set()
    for node in graph["nodes"]:
        node_id = node.get("id")
        if not node_id:
            raise ValueError("Every node must define an id")
        if node_id in seen:
            raise ValueError(f"Duplicate node id: {node_id}")
        seen.add(node_id)

    explicit_edges = isinstance(graph.get("edges"), list) and bool(graph.get("edges"))
    default_join = graph.get("default_join") or ("any" if explicit_edges else "all")
    edges = _normalize_edges(graph, seen)
    start_nodes = _start_nodes(graph, seen, edges)
    if not start_nodes:
        raise ValueError("Control-flow graph has no start nodes")

    levels = _layout_levels(graph["nodes"], edges, start_nodes, allow_cycles=explicit_edges)
    return {
        **graph,
        "graph_type": graph.get("graph_type") or graph.get("type") or (
            "control_flow" if explicit_edges else "dag"
        ),
        "default_join": default_join,
        "edges": edges,
        "start_nodes": start_nodes,
        "levels": levels,
        "max_total_visits": graph.get("max_total_visits", DEFAULT_MAX_TOTAL_VISITS),
    }


def _normalize_edges(graph: dict[str, Any], node_ids: set[str]) -> list[dict[str, Any]]:
    if isinstance(graph.get("edges"), list) and graph["edges"]:
        edges = []
        for index, edge in enumerate(graph["edges"]):
            from_node = edge.get("from")
            to_node = edge.get("to")
            if from_node not in node_ids:
                raise ValueError(f"Edge {index} references missing from node: {from_node}")
            if to_node not in node_ids:
                raise ValueError(f"Edge {index} references missing to node: {to_node}")
            edges.append(
                {
                    "from": from_node,
                    "to": to_node,
                    "when": edge.get("when"),
                    "label": edge.get("label"),
                    "kind": edge.get("kind", "control"),
                }
            )
        for node in graph["nodes"]:
            for dep in node.get("deps", []):
                if dep not in node_ids:
                    raise ValueError(f"Node {node['id']} depends on missing node {dep}")
        return edges

    edges = []
    for node in graph["nodes"]:
        for dep in node.get("deps", []):
            if dep not in node_ids:
                raise ValueError(f"Node {node['id']} depends on missing node {dep}")
            edges.append({"from": dep, "to": node["id"], "when": None, "label": None, "kind": "dependency"})
    _assert_acyclic([node["id"] for node in graph["nodes"]], edges)
    return edges


def _start_nodes(graph: dict[str, Any], node_ids: set[str], edges: list[dict[str, Any]]) -> list[str]:
    configured = graph.get("start") or graph.get("start_nodes")
    if isinstance(configured, str):
        configured = [configured]
    if configured:
        missing = [node_id for node_id in configured if node_id not in node_ids]
        if missing:
            raise ValueError(f"Start nodes do not exist: {', '.join(missing)}")
        return sorted(configured)
    incoming = {edge["to"] for edge in edges}
    starts = [node["id"] for node in graph["nodes"] if node["id"] not in incoming]
    if starts:
        return starts
    return [graph["nodes"][0]["id"]]


def _assert_acyclic(node_ids: list[str], edges: list[dict[str, Any]]) -> None:
    remaining = {node_id: set() for node_id in node_ids}
    for edge in edges:
        remaining[edge["to"]].add(edge["from"])
    completed: set[str] = set()
    while remaining:
        ready = sorted(node_id for node_id, deps in remaining.items() if deps.issubset(completed))
        if not ready:
            cycle_nodes = ", ".join(sorted(remaining))
            raise ValueError(f"DAG contains a cycle involving: {cycle_nodes}")
        completed.update(ready)
        for node_id in ready:
            remaining.pop(node_id)


def _layout_levels(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    start_nodes: list[str],
    *,
    allow_cycles: bool,
) -> list[list[str]]:
    node_ids = [node["id"] for node in nodes]
    if not allow_cycles:
        return _build_levels_from_edges(node_ids, edges)
    try:
        return _build_levels_from_edges(node_ids, edges)
    except ValueError:
        ordered: list[str] = []
        seen: set[str] = set()
        adjacency = _outgoing_edges(edges)
        queue = list(start_nodes)
        while queue:
            node_id = queue.pop(0)
            if node_id in seen:
                continue
            seen.add(node_id)
            ordered.append(node_id)
            for edge in adjacency.get(node_id, []):
                if edge["to"] not in seen:
                    queue.append(edge["to"])
        ordered.extend(node_id for node_id in node_ids if node_id not in seen)
        return [[node_id] for node_id in ordered]


def _build_levels_from_edges(node_ids: list[str], edges: list[dict[str, Any]]) -> list[list[str]]:
    remaining = {node_id: set() for node_id in node_ids}
    for edge in edges:
        remaining[edge["to"]].add(edge["from"])
    completed: set[str] = set()
    levels: list[list[str]] = []
    while remaining:
        ready = sorted(node_id for node_id, deps in remaining.items() if deps.issubset(completed))
        if not ready:
            cycle_nodes = ", ".join(sorted(remaining))
            raise ValueError(f"Graph contains a cycle involving: {cycle_nodes}")
        levels.append(ready)
        completed.update(ready)
        for node_id in ready:
            remaining.pop(node_id)
    return levels


def _outgoing_edges(edges: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    outgoing: dict[str, list[dict[str, Any]]] = {}
    for edge in edges:
        outgoing.setdefault(edge["from"], []).append(edge)
    return outgoing


def _predecessor_ids(edges: list[dict[str, Any]]) -> dict[str, set[str]]:
    predecessors: dict[str, set[str]] = {}
    for edge in edges:
        predecessors.setdefault(edge["to"], set()).add(edge["from"])
    return predecessors


def _node_type(node: dict[str, Any]) -> str:
    return str(node.get("type") or ("condition" if node.get("operation") == "condition" else "agent"))


def _is_condition_node(node: dict[str, Any]) -> bool:
    return _node_type(node).lower() in {"condition", "decision", "router", "branch"}


def _node_backend_for_display(node: dict[str, Any]) -> str:
    if _is_condition_node(node):
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
    if normalized in {"multica-job", "multica-native-job", "native-job"}:
        return "multica_job"
    if normalized in {"sim", "mock", "simulator"}:
        return "simulator"
    if normalized in {"hermes", "hermes-oneshot", "direct-hermes", "hermes-direct"}:
        return "hermes"
    return normalized


def _agent_input_payload(a2a_task: dict[str, Any]) -> dict[str, Any] | None:
    for message in a2a_task.get("history", []):
        for part in message.get("parts", []):
            data = part.get("data")
            if isinstance(data, dict):
                return data
    return None


def _node_max_visits(node: dict[str, Any]) -> int:
    value = node.get("max_visits", node.get("max_attempts", DEFAULT_MAX_NODE_VISITS))
    return max(1, int(value))


def _edge_label(edge: dict[str, Any]) -> str:
    if edge.get("label"):
        return str(edge["label"])
    if edge.get("when"):
        return str(edge["when"])
    return ""


def _public_node_spec(node: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in node.items() if not key.startswith("_")}


def _resolve_value(value: Any, graph_input: dict[str, Any], results: dict[str, Any]) -> Any:
    if isinstance(value, str):
        if value.startswith("$input."):
            return _path_get(graph_input, value.removeprefix("$input."))
        if value.startswith("$deps."):
            return _path_get(results, value.removeprefix("$deps."))
        return value
    if isinstance(value, list):
        return [_resolve_value(item, graph_input, results) for item in value]
    if isinstance(value, dict):
        return {key: _resolve_value(item, graph_input, results) for key, item in value.items()}
    return value


def _path_get(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            current = current[int(part)]
        else:
            return None
    return current


def _safe_eval_condition(expression: str, context: dict[str, Any]) -> Any:
    tree = ast.parse(expression, mode="eval")
    return _eval_ast(tree.body, _to_attr_context(context))


def _eval_ast(node: ast.AST, context: dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in context:
            return context[node.id]
        if node.id in {"true", "True"}:
            return True
        if node.id in {"false", "False"}:
            return False
        if node.id in {"null", "None"}:
            return None
        raise ValueError(f"Unknown condition variable: {node.id}")
    if isinstance(node, ast.Attribute):
        value = _eval_ast(node.value, context)
        if isinstance(value, dict):
            return value.get(node.attr)
        return getattr(value, node.attr, None)
    if isinstance(node, ast.Subscript):
        value = _eval_ast(node.value, context)
        key = _eval_ast(node.slice, context)
        return value[key]
    if isinstance(node, ast.List):
        return [_eval_ast(item, context) for item in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(_eval_ast(item, context) for item in node.elts)
    if isinstance(node, ast.Dict):
        return {
            _eval_ast(key, context): _eval_ast(value, context)
            for key, value in zip(node.keys, node.values)
        }
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            for value_node in node.values:
                value = _eval_ast(value_node, context)
                if not value:
                    return value
            return value
        if isinstance(node.op, ast.Or):
            for value_node in node.values:
                value = _eval_ast(value_node, context)
                if value:
                    return value
            return value
    if isinstance(node, ast.UnaryOp):
        operand = _eval_ast(node.operand, context)
        if isinstance(node.op, ast.Not):
            return not operand
        if isinstance(node.op, ast.USub):
            return -operand
    if isinstance(node, ast.BinOp):
        left = _eval_ast(node.left, context)
        right = _eval_ast(node.right, context)
        return _eval_binop(node.op, left, right)
    if isinstance(node, ast.Compare):
        left = _eval_ast(node.left, context)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval_ast(comparator, context)
            if not _eval_compare(op, left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise ValueError("Condition functions must be simple names")
        fn_name = node.func.id
        functions = {
            "len": len,
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "min": min,
            "max": max,
        }
        if fn_name not in functions:
            raise ValueError(f"Unsupported condition function: {fn_name}")
        args = [_eval_ast(arg, context) for arg in node.args]
        return functions[fn_name](*args)
    raise ValueError(f"Unsupported condition expression: {ast.dump(node)}")


def _eval_binop(op: ast.operator, left: Any, right: Any) -> Any:
    if isinstance(op, ast.Add):
        return left + right
    if isinstance(op, ast.Sub):
        return left - right
    if isinstance(op, ast.Mult):
        return left * right
    if isinstance(op, ast.Div):
        return left / right
    if isinstance(op, ast.FloorDiv):
        return left // right
    if isinstance(op, ast.Mod):
        return left % right
    raise ValueError(f"Unsupported operator: {type(op).__name__}")


def _eval_compare(op: ast.cmpop, left: Any, right: Any) -> bool:
    if isinstance(op, ast.Eq):
        return left == right
    if isinstance(op, ast.NotEq):
        return left != right
    if isinstance(op, ast.Lt):
        return left < right
    if isinstance(op, ast.LtE):
        return left <= right
    if isinstance(op, ast.Gt):
        return left > right
    if isinstance(op, ast.GtE):
        return left >= right
    if isinstance(op, ast.In):
        return left in right
    if isinstance(op, ast.NotIn):
        return left not in right
    if isinstance(op, ast.Is):
        return left is right
    if isinstance(op, ast.IsNot):
        return left is not right
    raise ValueError(f"Unsupported comparison: {type(op).__name__}")


def _to_attr_context(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _to_attr_context(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_attr_context(item) for item in value]
    return value


def _summarize_result(result: dict[str, Any]) -> str:
    payload = result["payload"]
    interesting_keys = [
        "status",
        "order_status",
        "payment_status",
        "risk",
        "percent",
        "count",
        "tracking_number",
        "channel",
        "reservation_id",
        "artifact",
        "decision",
        "message",
    ]
    summary_parts = [
        f"{key}={payload[key]}" for key in interesting_keys if key in payload
    ]
    if summary_parts:
        return ", ".join(summary_parts)
    return f"keys={','.join(sorted(payload.keys()))}"


ACTIVITIES = [dispatch_agent_node, poll_agent_node]
