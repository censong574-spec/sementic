from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode

import httpx

from sementic.config import BotServiceSettings
from sementic.im_models import MentionRegistryItem, Ownership
from sementic.models import BotProfile

logger = logging.getLogger(__name__)


class BotServiceClient:
    def __init__(
        self,
        settings: BotServiceSettings | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings or BotServiceSettings()
        self._http_client = http_client

    @property
    def enabled(self) -> bool:
        return bool(self.settings.agents_url.strip())

    def build_agents_url(self, workspace_id: str) -> str:
        base = self.settings.agents_url.rstrip("/")
        query = urlencode({"workspace_id": workspace_id})
        return f"{base}?{query}"

    async def query_workspace_agents(
        self,
        workspace_id: str,
        *,
        multica_token: str,
    ) -> list[BotProfile]:
        workspace_id = (workspace_id or "").strip()
        token = (multica_token or "").strip()
        if not self.enabled or not workspace_id or not token:
            return []

        url = self.build_agents_url(workspace_id)
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Workspace-ID": workspace_id,
        }
        logger.info("bot service query url=%s", url)

        client = self._http_client or httpx.AsyncClient(
            timeout=self.settings.timeout_seconds,
            trust_env=False,
        )
        close_client = self._http_client is None
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            payload = response.json()
        finally:
            if close_client:
                await client.aclose()

        return _parse_workspace_agents(payload)

    async def resolve_workspace_agents_for_orchestration(
        self,
        *,
        workspace_id: str,
        sender_user_id: str,
        mentions: list[MentionRegistryItem],
        multica_token: str,
    ) -> list[BotProfile]:
        bots_by_id = {
            bot.bot_user_id: bot
            for bot in await self.query_workspace_agents(
                workspace_id,
                multica_token=multica_token,
            )
        }

        for mention in mentions:
            if mention.ownership != Ownership.MY_SYSTEM:
                continue
            bot = bots_by_id.get(mention.entity_id)
            if bot is None:
                bot = BotProfile(
                    bot_user_id=mention.entity_id,
                    display_name=mention.entity_id,
                    role="assistant",
                    expertise=["general"],
                    owner_user_id=sender_user_id,
                    share_scope="private",
                )
            bots_by_id[mention.entity_id] = bot

        return list(bots_by_id.values())


def _parse_workspace_agents(payload: Any) -> list[BotProfile]:
    if not isinstance(payload, list):
        raise RuntimeError("bot service expected JSON array of agents")

    bots: list[BotProfile] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        runtime_config = item.get("runtime_config") or {}
        if not isinstance(runtime_config, dict):
            runtime_config = {}

        bot_user_id = str(runtime_config.get("bot_id") or item.get("id") or "").strip()
        if not bot_user_id:
            continue

        name = str(item.get("name") or bot_user_id).strip()
        description = str(item.get("description") or "").strip()
        instructions = str(item.get("instructions") or "").strip()
        provider = str(runtime_config.get("provider") or "").strip()
        expertise = [provider] if provider else ["general"]
        status = str(item.get("status") or "offline").strip()
        owner_id = str(item.get("owner_id") or "").strip()

        bots.append(
            BotProfile(
                bot_user_id=bot_user_id,
                display_name=name,
                role=description or instructions or "assistant",
                expertise=expertise,
                owner_user_id=owner_id or None,
                share_scope=str(item.get("visibility") or "private"),
                multica_agent_id=str(item.get("id") or "").strip() or None,
                is_online=status != "offline",
            )
        )
    return bots
