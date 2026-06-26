from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class MulticaChatClient:
    """Client for Multica native chat endpoints.

    The native chat API is intentionally unauthenticated. It derives workspace
    scope from the agent/session and enqueues context.type=native_chat tasks.
    """

    def __init__(
        self,
        *,
        api_base: str,
        timeout_seconds: float = 30.0,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._api_base = api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._http_client = http_client

    @property
    def api_base(self) -> str:
        return self._api_base

    async def create_session(self, *, agent_id: str, title: str = "") -> dict[str, Any]:
        payload = {"agent_id": agent_id}
        if title.strip():
            payload["title"] = title.strip()
        logger.info("multica chat create session agent_id=%s title=%s", agent_id, title)
        return await self._request_json("POST", "/api/chat/sessions", json=payload)

    async def send_message(self, *, session_id: str, content: str) -> dict[str, Any]:
        logger.info(
            "multica chat send message session_id=%s content_chars=%s",
            session_id,
            len(content or ""),
        )
        return await self._request_json(
            "POST",
            f"/api/chat/sessions/{session_id}/messages",
            json={"content": content},
        )

    async def get_pending_task(self, *, session_id: str) -> dict[str, Any]:
        return await self._request_json(
            "GET",
            f"/api/chat/sessions/{session_id}/pending-task",
        )

    async def list_messages(self, *, session_id: str) -> list[dict[str, Any]]:
        payload = await self._request_json(
            "GET",
            f"/api/chat/sessions/{session_id}/messages",
        )
        if not isinstance(payload, list):
            raise RuntimeError("Multica chat messages response must be a JSON array")
        return [item for item in payload if isinstance(item, dict)]

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.api_base}{path}"
        client = self._http_client or httpx.AsyncClient(
            timeout=self.timeout_seconds,
            trust_env=False,
        )
        close_client = self._http_client is None
        try:
            response = await client.request(method, url, json=json)
            response.raise_for_status()
            if not response.content:
                return {}
            return response.json()
        except httpx.HTTPStatusError as exc:
            body = exc.response.text[:500]
            logger.error(
                "multica chat api status error method=%s path=%s status=%s body=%s",
                method,
                path,
                exc.response.status_code,
                body,
            )
            raise
        finally:
            if close_client:
                await client.aclose()
