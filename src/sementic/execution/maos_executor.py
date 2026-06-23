from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Iterator

from maos_runtime.sandbox_runtime import TemporalTaskService

from sementic.config import MaosSettings
from sementic.task_graph import TaskGraphPlan

logger = logging.getLogger(__name__)


class MaosExecutor:
    """In-process MAOS Temporal worker + plan submitter."""

    def __init__(self, settings: MaosSettings | None = None) -> None:
        self.settings = settings or MaosSettings()
        self._service: TemporalTaskService | None = None

    def start(self) -> None:
        apply_maos_process_env(self.settings)
        logger.info(
            "starting MAOS Temporal worker temporal=%s namespace=%s",
            self.settings.temporal_address,
            self.settings.temporal_namespace,
        )
        self._service = TemporalTaskService(
            temporal_address=self.settings.temporal_address,
            temporal_namespace=self.settings.temporal_namespace,
            temporal_db_file=None,
            temporal_ui=False,
        )
        self._wait_until_ready()

    def submit_plan(self, plan: TaskGraphPlan) -> list[str]:
        if self._service is None:
            raise RuntimeError("MaosExecutor is not started")
        graph = plan.graph.model_dump(mode="json", by_alias=True)
        with multica_job_env_from_graph(graph):
            task_ids = self._service.submit_batch([graph])
        logger.info(
            "submitted plan_id=%s task_ids=%s",
            plan.plan_id,
            task_ids,
        )
        return task_ids

    def task_snapshot(self, task_id: str) -> dict[str, Any]:
        if self._service is None:
            raise RuntimeError("MaosExecutor is not started")
        return self._service.task_snapshot(task_id)

    def runtime_info(self) -> dict[str, Any]:
        if self._service is None:
            return {"runtime_status": "not_started"}
        return self._service.runtime_info()

    def _wait_until_ready(self) -> None:
        assert self._service is not None
        deadline = time.time() + self.settings.startup_timeout_seconds
        while time.time() < deadline:
            info = self._service.runtime_info()
            status = info.get("runtime_status")
            if status == "failed":
                raise RuntimeError(info.get("error") or "MAOS Temporal runtime failed to start")
            if status in {"ready", "running"}:
                logger.info("MAOS Temporal runtime ready status=%s", status)
                return
            time.sleep(0.5)
        raise TimeoutError(
            f"MAOS Temporal runtime not ready within {self.settings.startup_timeout_seconds}s"
        )


def apply_maos_process_env(settings: MaosSettings) -> None:
    os.environ.setdefault("MULTICA_JOB_API_BASE", settings.multica_job_api_base)
    if settings.multica_job_token:
        os.environ["MULTICA_JOB_TOKEN"] = settings.multica_job_token
    if settings.multica_job_workspace_id:
        os.environ["MULTICA_JOB_WORKSPACE_ID"] = settings.multica_job_workspace_id
    if settings.multica_job_workspace_slug:
        os.environ["MULTICA_JOB_WORKSPACE_SLUG"] = settings.multica_job_workspace_slug


@contextmanager
def multica_job_env_from_graph(graph: dict[str, Any]) -> Iterator[None]:
    """Per-plan Multica credentials from graph.input override process defaults."""
    graph_input = graph.get("input")
    if not isinstance(graph_input, dict):
        yield
        return

    saved: dict[str, str | None] = {}
    token = graph_input.get("multica_token")
    workspace_id = graph_input.get("workspace_id")
    if token:
        saved["MULTICA_JOB_TOKEN"] = os.environ.get("MULTICA_JOB_TOKEN")
        os.environ["MULTICA_JOB_TOKEN"] = str(token)
    if workspace_id:
        saved["MULTICA_JOB_WORKSPACE_ID"] = os.environ.get("MULTICA_JOB_WORKSPACE_ID")
        os.environ["MULTICA_JOB_WORKSPACE_ID"] = str(workspace_id)
        os.environ.pop("MULTICA_JOB_WORKSPACE_SLUG", None)
    try:
        yield
    finally:
        for key, previous in saved.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
