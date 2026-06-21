from __future__ import annotations

import logging
from typing import Any

import httpx

from sementic.config import MulticaSettings

logger = logging.getLogger(__name__)


class MulticaClient:
    """Workspace-scoped Multica API client using per-user daemon tokens."""

    def __init__(
        self,
        settings: MulticaSettings | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings or MulticaSettings()
        self._http_client = http_client

    @property
    def enabled(self) -> bool:
        return bool(self.settings.service_base.strip())

    @staticmethod
    def has_credentials(*, workspace_id: str | None, multica_token: str | None) -> bool:
        return bool((workspace_id or "").strip() and (multica_token or "").strip())

    def build_auth_headers(self, multica_token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {multica_token}"}

    async def list_runtimes(self, *, workspace_id: str, multica_token: str) -> list[dict[str, Any]]:
        if not self.enabled:
            return []

        base = self.settings.service_base.rstrip("/")
        url = f"{base}/im/mattermost/runtimes"
        client = self._http_client or httpx.AsyncClient(
            timeout=self.settings.timeout_seconds,
            trust_env=False,
        )
        close_client = self._http_client is None
        try:
            response = await client.get(
                url,
                params={"workspace_id": workspace_id},
                headers=self.build_auth_headers(multica_token),
            )
            response.raise_for_status()
            payload = response.json()
        finally:
            if close_client:
                await client.aclose()

        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            runtimes = data.get("runtimes")
            if isinstance(runtimes, list):
                return [item for item in runtimes if isinstance(item, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("runtimes"), list):
            return [item for item in payload["runtimes"] if isinstance(item, dict)]
        return []
