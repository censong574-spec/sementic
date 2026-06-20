from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode

import httpx

from sementic.config import BotServiceSettings
from sementic.im_models import MentionRegistryItem, Ownership
from sementic.models import BotProfile

logger = logging.getLogger(__name__)

# 固定接口路径；环境变量只配置服务基址
BOTS_QUERY_PATH = "/api/v1/bots"


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
        return bool(self.settings.service_base.strip())

    def build_query_url(self, user_id: str) -> str:
        base = self.settings.service_base.rstrip("/")
        query = urlencode({"user_id": user_id})
        return f"{base}{BOTS_QUERY_PATH}?{query}"

    async def query_owned_bots(self, user_id: str) -> list[BotProfile]:
        if not self.enabled:
            return []

        url = self.build_query_url(user_id)
        logger.info("bot service query url=%s", url)

        # Bot service is on LAN/WSL; bypass Windows HTTP_PROXY (LLM calls keep proxy).
        client = self._http_client or httpx.AsyncClient(
            timeout=self.settings.timeout_seconds,
            trust_env=False,
        )
        close_client = self._http_client is None
        try:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
        finally:
            if close_client:
                await client.aclose()

        return _parse_owned_bots(user_id, payload)

    async def resolve_owned_bots_for_orchestration(
        self,
        *,
        sender_user_id: str,
        mentions: list[MentionRegistryItem],
    ) -> list[BotProfile]:
        bots_by_id = {
            bot.bot_user_id: bot
            for bot in await self.query_owned_bots(sender_user_id)
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


def _parse_owned_bots(user_id: str, payload: dict[str, Any]) -> list[BotProfile]:
    if payload.get("code") != 0:
        message = payload.get("message", "bot service error")
        raise RuntimeError(f"bot service returned code != 0: {message}")

    data = payload.get("data") or {}
    items = data.get("bots") or []
    bots: list[BotProfile] = []
    for item in items:
        bot_user_id = str(item.get("bot_user_id", "")).strip()
        if not bot_user_id:
            continue
        name = str(item.get("name", bot_user_id)).strip()
        description = str(item.get("description", "")).strip()
        bots.append(
            BotProfile(
                bot_user_id=bot_user_id,
                display_name=name,
                role=description or "assistant",
                expertise=["general"],
                owner_user_id=user_id,
                share_scope="private",
            )
        )
    return bots
