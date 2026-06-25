from __future__ import annotations

import pytest

from sementic.bot_service import BotServiceClient
from sementic.config import BotServiceSettings

TEAM_ID = "b44bcfe0-c95a-4272-a65b-db535d7b1ac6"
OWNER_USER_ID = "dfmbtox46j8ymkforyt7qq64tr"
INTERNAL_TOKEN = "QyvTknDmmSWFYYOtJxmiJkbxuV6W1iPl-nmbIbHiS-k"

SAMPLE_AGENT = {
    "workspace_id": "d1cd810c-8c2b-4d58-a448-be52b675380e",
    "agent_id": "7630d83b-6b78-483d-bf79-35d9f256b753",
    "runtime_id": "e8702419-75ae-4572-a2c2-0a1c33a52fe0",
    "bot_id": "okmbzgnbdfycpqggohr6hnxcte",
    "bot_username": "managed-hermes-desktop-qbg4054",
    "owner_user_id": OWNER_USER_ID,
    "status": "active",
    "agent_name": "hermes-desktop-qbg4054",
    "runtime_provider": "hermes",
    "runtime_status": "online",
}


def test_build_agents_url() -> None:
    client = BotServiceClient(
        BotServiceSettings(agents_url="http://127.0.0.1:8080/api/im/mattermost/agents")
    )
    url = client.build_agents_url(TEAM_ID, OWNER_USER_ID)
    assert (
        url
        == f"http://127.0.0.1:8080/api/im/mattermost/agents?team_id={TEAM_ID}&owner_user_id={OWNER_USER_ID}"
    )


@pytest.mark.asyncio
async def test_query_mattermost_agents(monkeypatch: pytest.MonkeyPatch) -> None:
    import httpx

    monkeypatch.setenv("MULTICA_IM_INTERNAL_TOKEN", INTERNAL_TOKEN)

    async def handler(request):
        assert request.url.path == "/api/im/mattermost/agents"
        assert request.url.params["team_id"] == TEAM_ID
        assert request.url.params["owner_user_id"] == OWNER_USER_ID
        assert request.headers["authorization"] == f"Bearer {INTERNAL_TOKEN}"
        return httpx.Response(
            200,
            json={
                "team_id": TEAM_ID,
                "owner_user_id": OWNER_USER_ID,
                "agents": [SAMPLE_AGENT],
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = BotServiceClient(
            BotServiceSettings(agents_url="http://127.0.0.1:8080/api/im/mattermost/agents"),
            http_client=http_client,
        )
        bots = await client.query_mattermost_agents(TEAM_ID, OWNER_USER_ID)

    assert len(bots) == 1
    assert bots[0].bot_user_id == "okmbzgnbdfycpqggohr6hnxcte"
    assert bots[0].display_name == "hermes-desktop-qbg4054"
    assert bots[0].multica_agent_id == "7630d83b-6b78-483d-bf79-35d9f256b753"
    assert bots[0].owner_user_id == OWNER_USER_ID
    assert bots[0].is_online is True


@pytest.mark.asyncio
async def test_query_mattermost_agents_without_token_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MULTICA_IM_INTERNAL_TOKEN", raising=False)
    monkeypatch.delenv("HERMES_INTERNAL_BRIDGE_TOKEN", raising=False)
    client = BotServiceClient(
        BotServiceSettings(agents_url="http://127.0.0.1:8080/api/im/mattermost/agents")
    )
    bots = await client.query_mattermost_agents(TEAM_ID, OWNER_USER_ID)
    assert bots == []
