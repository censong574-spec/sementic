from __future__ import annotations

import logging
import threading

import uvicorn

from sementic.config import MaosObserverSettings
from sementic.execution.maos_executor import MaosExecutor
from sementic.maos_observer.app import create_observer_app

logger = logging.getLogger(__name__)

OBSERVER_HOST = "0.0.0.0"


def start_observer_server_if_enabled(
    maos_executor: MaosExecutor,
    settings: MaosObserverSettings | None = None,
) -> threading.Thread | None:
    settings = settings or MaosObserverSettings()
    if not settings.enabled:
        logger.info("MAOS task observer disabled")
        return None

    app = create_observer_app(maos_executor=maos_executor)
    thread = threading.Thread(
        target=_run_uvicorn,
        name="maos-observer",
        args=(app, settings),
        daemon=True,
    )
    thread.start()
    logger.info(
        "MAOS task observer listening http://%s:%s",
        OBSERVER_HOST,
        settings.port,
    )
    return thread


def _run_uvicorn(app, settings: MaosObserverSettings) -> None:
    config = uvicorn.Config(
        app,
        host=OBSERVER_HOST,
        port=settings.port,
        log_level="info",
        access_log=True,
    )
    server = uvicorn.Server(config)
    server.run()
