from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from redis import Redis

from sementic.config import ImEgressSettings, RedisSettings

logger = logging.getLogger(__name__)


class MattermostPostClient:
    def __init__(self, settings: ImEgressSettings | None = None) -> None:
        self.settings = settings or ImEgressSettings()
        self._cached_external_token: str | None = None

    @property
    def enabled(self) -> bool:
        return self.settings.enabled and bool(self.settings.url.strip())

    def post_reply(
        self,
        *,
        bot_user_id: str,
        channel_id: str,
        root_post_id: str,
        message: str,
    ) -> dict[str, Any] | None:
        if not self.enabled:
            logger.info("mattermost egress disabled; skip post channel=%s", channel_id)
            return None

        external_token = self._external_ingress_token()
        if external_token:
            return self._post_external_ingress(
                bot_user_id=bot_user_id,
                channel_id=channel_id,
                root_post_id=root_post_id,
                message=message,
                token=external_token,
            )

        token = self.settings.token_for_bot(bot_user_id)
        if not token:
            logger.warning(
                "no mattermost egress auth for bot_user_id=%s; "
                "set redis %s or bot token map",
                bot_user_id,
                self._redis_key(),
            )
            return None

        payload: dict[str, Any] = {
            "channel_id": channel_id,
            "root_id": root_post_id,
            "message": message,
        }
        url = f"{self.settings.url.rstrip('/')}/api/v4/posts"
        headers = {"Authorization": f"Bearer {token}"}

        return self._do_post(url, headers=headers, payload=payload, bot_user_id=bot_user_id)

    def _redis_key(self) -> str:
        key = os.getenv("INTERNAL_SERVICE_TOKEN", "").strip()
        return key or self.settings.external_ingress_redis_key

    def _external_ingress_token(self) -> str:
        if self._cached_external_token is not None:
            return self._cached_external_token

        redis_settings = RedisSettings()
        client: Redis | None = None
        try:
            client = Redis.from_url(redis_settings.url, decode_responses=True)
            token = (client.get(self._redis_key()) or "").strip()
            self._cached_external_token = token
            return token
        except Exception:
            logger.exception(
                "failed to load mattermost external ingress token from redis key=%s",
                self._redis_key(),
            )
            self._cached_external_token = ""
            return ""
        finally:
            if client is not None:
                client.close()

    def _post_external_ingress(
        self,
        *,
        bot_user_id: str,
        channel_id: str,
        root_post_id: str,
        message: str,
        token: str,
    ) -> dict[str, Any] | None:
        payload: dict[str, Any] = {
            "channel_id": channel_id,
            "user_id": bot_user_id,
            "message": message,
        }
        if root_post_id:
            payload["root_id"] = root_post_id

        url = f"{self.settings.url.rstrip('/')}/api/v4/posts?external_ingress=true"
        headers = {
            "X-MM-External-Ingress-Token": token,
            "X-MM-External-Post-As-User-Id": bot_user_id,
        }
        return self._do_post(url, headers=headers, payload=payload, bot_user_id=bot_user_id)

    def _do_post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        payload: dict[str, Any],
        bot_user_id: str,
    ) -> dict[str, Any] | None:
        with httpx.Client(timeout=self.settings.timeout_seconds, trust_env=False) as client:
            response = client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            body = response.json()
            logger.info(
                "mattermost egress posted bot_user_id=%s channel=%s post_id=%s",
                bot_user_id,
                payload.get("channel_id"),
                body.get("id"),
            )
            return body
