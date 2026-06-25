from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode

import httpx

from sementic.config import BotServiceSettings
from sementic.im_internal_token import resolve_im_internal_token
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

    def build_agents_url(self, team_id: str, owner_user_id: str) -> str:
        base = self.settings.agents_url.rstrip("/")
        query = urlencode(
            {
                "team_id": team_id.strip(),
                "owner_user_id": owner_user_id.strip(),
            }
        )
        return f"{base}?{query}"

    async def query_mattermost_agents(
        self,
        team_id: str,
        owner_user_id: str,
    ) -> list[BotProfile]:
        team_id = (team_id or "").strip()
        owner_user_id = (owner_user_id or "").strip()
        if not self.enabled or not team_id or not owner_user_id:
            return []

        token = resolve_im_internal_token()
        if not token:
            logger.warning(
                "bot service: no IM internal token "
                "(MULTICA_IM_INTERNAL_TOKEN / HERMES_INTERNAL_BRIDGE_TOKEN / %s)",
                "/etc/mattermost-hermes-im-internal-token",
            )
            return []

        url = self.build_agents_url(team_id, owner_user_id)
        headers = {"Authorization": f"Bearer {token}"}
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

        return _parse_mattermost_im_agents(payload)

    async def resolve_workspace_agents_for_orchestration(
        self,
        *,
        team_id: str,
        owner_user_id: str,
        sender_user_id: str,
        mentions: list[MentionRegistryItem],
    ) -> list[BotProfile]:
        bots_by_id = {
            bot.bot_user_id: bot
            for bot in await self.query_mattermost_agents(team_id, owner_user_id)
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


def _parse_mattermost_im_agents(payload: Any) -> list[BotProfile]:
    if not isinstance(payload, dict):
        raise RuntimeError("bot service expected JSON object with agents array")

    items = payload.get("agents")
    if not isinstance(items, list):
        raise RuntimeError("bot service expected JSON object with agents array")

    bots: list[BotProfile] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        bot_user_id = str(item.get("bot_id") or "").strip()
        if not bot_user_id:
            continue

        name = str(item.get("agent_name") or item.get("bot_username") or bot_user_id).strip()
        provider = str(item.get("runtime_provider") or "").strip()
        expertise = [provider] if provider else ["general"]
        status = str(item.get("status") or "offline").strip()
        owner_id = str(item.get("owner_user_id") or "").strip()
        runtime_status = str(item.get("runtime_status") or "").strip()
        is_online = status != "offline" and runtime_status != "offline"

        bots.append(
            BotProfile(
                bot_user_id=bot_user_id,
                display_name=name,
                role=name or "assistant",
                expertise=expertise,
                owner_user_id=owner_id or None,
                share_scope="private",
                multica_agent_id=str(item.get("agent_id") or "").strip() or None,
                is_online=is_online,
            )
        )
    return bots
