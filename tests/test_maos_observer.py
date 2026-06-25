from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from sementic.maos_observer.app import create_observer_app
from sementic.maos_observer.summarize import export_tasks_payload, filter_tasks_by_status, summarize_task


def test_filter_tasks_active() -> None:
    tasks = [
        {"task_id": "a", "status": "running"},
        {"task_id": "b", "status": "completed"},
        {"task_id": "c", "status": "pending"},
    ]
    filtered = filter_tasks_by_status(tasks, "active")
    assert [task["task_id"] for task in filtered] == ["a", "c"]


def test_summarize_task_extracts_nodes_and_message() -> None:
    task = {
        "task_id": "task-1",
        "graph_id": "plan-1",
        "graph_name": "Demo",
        "status": "running",
        "submitted_at": 1_700_000_000.0,
        "state": {
            "graph_id": "plan-1",
            "graph_name": "Demo",
            "workflow_status": "running",
            "nodes": [
                {
                    "id": "intake",
                    "status": "completed",
                    "summary": "emit done",
                },
                {
                    "id": "run_task",
                    "status": "running",
                    "summary": "waiting agent",
                    "agent_name": "codex",
                    "backend": "multica_job",
                },
            ],
            "results": {
                "intake": {
                    "channel_id": "chan_1",
                    "user_message": "hello",
                    "sementic_event_id": "user:123",
                }
            },
        },
    }
    summary = summarize_task(task)
    assert summary["event_id"] == "user:123"
    assert summary["user_message"] == "hello"
    assert summary["channel_id"] == "chan_1"
    assert len(summary["nodes"]) == 2


def test_export_tasks_payload() -> None:
    payload = export_tasks_payload(
        {
            "runtime_status": "running",
            "tasks": [
                {"task_id": "task-1", "status": "running", "state": {"nodes": []}},
                {"task_id": "task-2", "status": "failed", "state": {"nodes": []}},
            ],
        },
        status="active",
    )
    assert payload["task_count"] == 1
    assert payload["tasks"][0]["task_id"] == "task-1"


def test_observer_api_lists_and_exports_tasks() -> None:
    executor = MagicMock()
    executor.runtime_info.return_value = {"runtime_status": "ready"}
    executor.tasks_snapshot.return_value = {
        "runtime_status": "ready",
        "temporal": {"target": "127.0.0.1:7233"},
        "tasks": [
            {
                "task_id": "task-abc",
                "graph_name": "Graph A",
                "status": "running",
                "state": {"nodes": [{"id": "run_task", "status": "running"}]},
            }
        ],
    }
    executor.task_snapshot.return_value = {
                "task_id": "task-abc",
                "graph_name": "Graph A",
                "status": "running",
                "state": {
                    "nodes": [{"id": "run_task", "status": "running"}],
                    "edges": [],
                },
            }

    client = TestClient(create_observer_app(maos_executor=executor))

    response = client.get("/api/tasks?status=active")
    assert response.status_code == 200
    body = response.json()
    assert body["task_count"] == 1
    assert body["tasks"][0]["task_id"] == "task-abc"

    detail = client.get("/api/tasks/task-abc")
    assert detail.status_code == 200
    assert detail.json()["graph_name"] == "Graph A"

    export_response = client.get("/api/tasks/export?status=active")
    assert export_response.status_code == 200
    assert export_response.headers["content-disposition"].startswith("attachment")
    assert export_response.json()["task_count"] == 1

    missing = client.get("/api/tasks/missing")
    executor.task_snapshot.side_effect = KeyError("missing")
    assert client.get("/api/tasks/missing").status_code == 404
