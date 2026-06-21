from __future__ import annotations

import pytest

from sementic.bot_service import BotServiceClient
from sementic.config import BotServiceSettings

SAMPLE_AGENT = {
    "id": "7630d83b-6b78-483d-bf79-35d9f256b753",
    "workspace_id": "d1cd810c-8c2b-4d58-a448-be52b675380e",
    "runtime_id": "e8702419-75ae-4572-a2c2-0a1c33a52fe0",
    "name": "hermes-desktop-qbg4054",
    "description": "local hermes agent managed from IM",
    "instructions": "You are an agent managed by the IM managed agents platform.",
    "runtime_config": {
        "bot_id": "okmbzgnbdfycpqggohr6hnxcte",
        "bot_username": "managed-hermes-desktop-qbg4054",
        "platform": "mattermost",
        "provider": "hermes",
    },
    "visibility": "workspace",
    "status": "offline",
    "owner_id": "1fd0d57b-f3eb-4ea1-b9dd-c2ddca20d29b",
}


def test_build_agents_url() -> None:
    client = BotServiceClient(
        BotServiceSettings(agents_url="http://127.0.0.1:8080/api/internal/agents")
    )
    url = client.build_agents_url("d1cd810c-8c2b-4d58-a448-be52b675380e")
    assert (
        url
        == "http://127.0.0.1:8080/api/internal/agents?workspace_id=d1cd810c-8c2b-4d58-a448-be52b675380e"
    )


@pytest.mark.asyncio
async def test_query_workspace_agents() -> None:
    import httpx

    async def handler(request):
        assert request.url.path == "/api/internal/agents"
        assert (
            request.url.params["workspace_id"]
            == "d1cd810c-8c2b-4d58-a448-be52b675380e"
        )
        return httpx.Response(200, json=[SAMPLE_AGENT])

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = BotServiceClient(
            BotServiceSettings(
                agents_url="http://127.0.0.1:8080/api/internal/agents"
            ),
            http_client=http_client,
        )
        bots = await client.query_workspace_agents(
            "d1cd810c-8c2b-4d58-a448-be52b675380e"
        )

    assert len(bots) == 1
    assert bots[0].bot_user_id == "okmbzgnbdfycpqggohr6hnxcte"
    assert bots[0].display_name == "hermes-desktop-qbg4054"
    assert bots[0].multica_agent_id == "7630d83b-6b78-483d-bf79-35d9f256b753"
    assert bots[0].owner_user_id == "1fd0d57b-f3eb-4ea1-b9dd-c2ddca20d29b"
    assert bots[0].is_online is False
