from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis

from sementic.config import RedisSettings
from sementic.handler import MessageHandler, PlanMessageResponse
from sementic.im_models import IMMessageEvent
from sementic.intent_classifier import TaskIntentClassifier
from sementic.llm import create_intent_llm_client, create_llm_client
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore


def create_app(
    *,
    message_handler: MessageHandler | None = None,
    redis_client: Redis | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if message_handler is not None:
            app.state.message_handler = message_handler
            app.state.history_store = message_handler.history_store
            app.state.redis = redis_client
        else:
            redis_settings = RedisSettings()
            app.state.redis = Redis.from_url(redis_settings.url, decode_responses=True)
            app.state.history_store = RedisHistoryStore(app.state.redis, redis_settings)
            app.state.message_handler = MessageHandler(
                history_store=app.state.history_store,
                intent_classifier=TaskIntentClassifier(llm=_build_intent_llm_client()),
                planner=Planner(llm=_build_llm_client()),
            )
        try:
            yield
        finally:
            if app.state.redis is not None:
                await app.state.redis.aclose()

    app = FastAPI(
        title="sementic",
        description="IM message ingestion and LLM collaboration planner",
        lifespan=lifespan,
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/v1/im/messages", response_model=PlanMessageResponse)
    async def ingest_im_message(event: IMMessageEvent) -> PlanMessageResponse:
        handler: MessageHandler = app.state.message_handler
        try:
            return await handler.handle(event)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app


def _build_llm_client():
    return create_llm_client(provider="aliyun")


def _build_intent_llm_client():
    return create_intent_llm_client(provider="aliyun")


app = create_app()
