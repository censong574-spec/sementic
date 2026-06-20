from __future__ import annotations

import pytest

from sementic.bot_service import BOTS_QUERY_PATH, BotServiceClient
from sementic.config import BotServiceSettings


def test_build_query_url_uses_fixed_path() -> None:
    client = BotServiceClient(
        BotServiceSettings(service_base="http://127.0.0.1")
    )
    url = client.build_query_url("user_1")
    assert url == f"http://127.0.0.1{BOTS_QUERY_PATH}?user_id=user_1"


@pytest.mark.asyncio
async def test_query_owned_bots_from_service() -> None:
    async def handler(request):
        assert request.url.path == BOTS_QUERY_PATH
        assert request.url.params["user_id"] == "k7hgneowkbybj8zx1q9i6weccw"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "message": "ok",
                "data": {
                    "user_id": "k7hgneowkbybj8zx1q9i6weccw",
                    "total": 1,
                    "bots": [
                        {
                            "bot_user_id": "bot_liusong_code",
                            "name": "代码助手",
                            "description": "写 Python",
                        }
                    ],
                },
            },
        )

    import httpx

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = BotServiceClient(
            BotServiceSettings(service_base="http://127.0.0.1"),
            http_client=http_client,
        )
        bots = await client.query_owned_bots("k7hgneowkbybj8zx1q9i6weccw")

    assert len(bots) == 1
    assert bots[0].bot_user_id == "bot_liusong_code"
    assert bots[0].display_name == "代码助手"
    assert bots[0].owner_user_id == "k7hgneowkbybj8zx1q9i6weccw"
