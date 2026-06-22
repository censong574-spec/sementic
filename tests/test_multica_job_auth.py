from __future__ import annotations

import pytest

from maos_runtime.a2a_runtime import (
    _multica_job_auth_from_graph_input,
    _multica_job_headers,
)


def test_auth_from_graph_input() -> None:
    auth = _multica_job_auth_from_graph_input(
        {
            "multica_token": "mdt_abc123",
            "workspace_id": "ws-1",
            "user_message": "hello",
        }
    )
    assert auth == {"token": "mdt_abc123", "workspace_id": "ws-1"}


def test_headers_use_graph_auth_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MULTICA_JOB_TOKEN", raising=False)
    monkeypatch.delenv("MULTICA_JOB_WORKSPACE_ID", raising=False)
    headers = _multica_job_headers(
        {"token": "mdt_plan_token", "workspace_id": "ws-plan"}
    )
    assert headers["Authorization"] == "Bearer mdt_plan_token"
    assert headers["X-Workspace-ID"] == "ws-plan"
