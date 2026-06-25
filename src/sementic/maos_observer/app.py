from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from sementic.execution.maos_executor import MaosExecutor
from sementic.maos_observer.summarize import export_tasks_payload, filter_tasks_by_status, summarize_task

_STATIC_DIR = Path(__file__).resolve().parent / "static"


def create_observer_app(*, maos_executor: MaosExecutor) -> FastAPI:
    app = FastAPI(
        title="sementic-maos-observer",
        description="Read-only MAOS workflow task monitor",
    )
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "sementic-maos-observer"}

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(_STATIC_DIR / "index.html")

    @app.get("/api/runtime")
    async def runtime() -> dict[str, Any]:
        return maos_executor.runtime_info()

    @app.get("/api/tasks")
    async def list_tasks(
        status: str = Query(default="active", description="active | all | running | failed | completed"),
    ) -> dict[str, Any]:
        snapshot = maos_executor.tasks_snapshot()
        tasks = snapshot.get("tasks") if isinstance(snapshot.get("tasks"), list) else []
        filtered = filter_tasks_by_status(tasks, status)
        return {
            "runtime_status": snapshot.get("runtime_status"),
            "temporal": snapshot.get("temporal"),
            "filter": status,
            "task_count": len(filtered),
            "tasks": [summarize_task(task) for task in filtered],
        }

    @app.get("/api/tasks/export")
    async def export_tasks(
        status: str = Query(default="active"),
    ) -> JSONResponse:
        snapshot = maos_executor.tasks_snapshot()
        payload = export_tasks_payload(snapshot, status=status)
        filename = f"maos-tasks-{status}.json"
        return JSONResponse(
            content=payload,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.get("/api/tasks/{task_id}")
    async def get_task(task_id: str) -> dict[str, Any]:
        try:
            task = maos_executor.task_snapshot(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"task not found: {task_id}") from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        summary = summarize_task(task)
        summary["state"] = task.get("state") if isinstance(task.get("state"), dict) else {}
        summary["result"] = task.get("result")
        summary["error"] = task.get("error")
        return summary

    return app
