from __future__ import annotations

import pytest
from fakeredis.aioredis import FakeRedis

from sementic.direct_chat import DirectChatRouter, DirectChatSessionStore, direct_mention_target
from sementic.im_models import IMMessageEvent
from sementic.models import BotProfile


def _event() -> IMMessageEvent:
    return IMMessageEvent.model_validate(
        {
            "event_id": "evt-direct",
            "group_session_id": "group-1",
            "user_context": {
                "user_id": "user-1",
                "username": "User",
                "is_bot": False,
            },
            "message_context": {
                "msg_id": "post-1",
                "content": "@Bot run this",
                "mentions_registry": [
                    {"entity_id": "bot-1", "ownership": "MY_SYSTEM"},
                ],
            },
        }
    )


def _workspace_event() -> IMMessageEvent:
    return IMMessageEvent.model_validate(
        {
            "event_id": "evt-direct",
            "group_session_id": "group-1",
            "workspace_id": "workspace-1",
            "user_context": {
                "user_id": "user-1",
                "username": "User",
                "is_bot": False,
            },
            "message_context": {
                "msg_id": "post-1",
                "content": "@Bot run this",
                "mentions_registry": [
                    {"entity_id": "bot-1", "ownership": "MY_SYSTEM"},
                ],
            },
        }
    )


def _bot(bot_user_id: str = "bot-1") -> BotProfile:
    return BotProfile(
        bot_user_id=bot_user_id,
        display_name="Bot",
        role="assistant",
        multica_agent_id="agent-1",
        is_online=True,
    )


class FakeChatClient:
    def __init__(self) -> None:
        self.created = []
        self.sent = []

    async def create_session(self, *, agent_id: str, title: str = "") -> dict:
        self.created.append({"agent_id": agent_id, "title": title})
        return {"id": "session-created"}

    async def send_message(self, *, session_id: str, content: str) -> dict:
        self.sent.append({"session_id": session_id, "content": content})
        return {"task_id": f"task-{len(self.sent)}", "message_id": f"message-{len(self.sent)}"}


class FailingFirstSendChatClient(FakeChatClient):
    async def send_message(self, *, session_id: str, content: str) -> dict:
        if not self.sent:
            self.sent.append({"session_id": session_id, "content": content, "failed": True})
            raise RuntimeError("stale session")
        return await super().send_message(session_id=session_id, content=content)


@pytest.mark.asyncio
async def test_direct_chat_router_reuses_session() -> None:
    redis = FakeRedis(decode_responses=True)
    client = FakeChatClient()
    router = DirectChatRouter(
        client=client,
        session_store=DirectChatSessionStore(redis, ttl_seconds=60),
    )

    first = await router.maybe_submit(event=_event(), owned_bots=[_bot()])
    second = await router.maybe_submit(event=_event(), owned_bots=[_bot()])

    assert first is not None
    assert second is not None
    assert first.session_id == "session-created"
    assert second.session_id == "session-created"
    assert len(client.created) == 1
    assert client.sent == [
        {"session_id": "session-created", "content": "@Bot run this"},
        {"session_id": "session-created", "content": "@Bot run this"},
    ]


@pytest.mark.asyncio
async def test_direct_chat_router_recreates_stale_session_once() -> None:
    redis = FakeRedis(decode_responses=True)
    await redis.set("sementic:multica_chat_session:group-1:bot-1", "stale-session")
    client = FailingFirstSendChatClient()
    router = DirectChatRouter(
        client=client,
        session_store=DirectChatSessionStore(redis, ttl_seconds=60),
    )

    submission = await router.maybe_submit(event=_event(), owned_bots=[_bot()])

    assert submission is not None
    assert submission.session_id == "session-created"
    assert len(client.created) == 1
    assert client.sent == [
        {"session_id": "stale-session", "content": "@Bot run this", "failed": True},
        {"session_id": "session-created", "content": "@Bot run this"},
    ]
    assert await redis.get("sementic:multica_chat_session:group-1:bot-1") == "session-created"


@pytest.mark.asyncio
async def test_direct_chat_router_skips_workspace_fallback_agent_id() -> None:
    redis = FakeRedis(decode_responses=True)
    client = FakeChatClient()
    router = DirectChatRouter(
        client=client,
        session_store=DirectChatSessionStore(redis),
    )

    submission = await router.maybe_submit(
        event=_workspace_event(),
        owned_bots=[
            _bot().model_copy(
                update={
                    "multica_agent_id": "workspace-1",
                }
            )
        ],
    )

    assert submission is None
    assert client.created == []
    assert client.sent == []


def test_direct_mention_target_requires_single_mention() -> None:
    event = _event().model_copy(deep=True)
    event.message_context.mentions_registry.append(
        event.message_context.mentions_registry[0].model_copy(update={"entity_id": "bot-2"})
    )

    assert direct_mention_target(event, [_bot(), _bot("bot-2")]) is None
