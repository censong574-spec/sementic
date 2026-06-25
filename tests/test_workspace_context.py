from __future__ import annotations

import pytest

from sementic.handler import _inject_workspace_context_into_plan, _workspace_default_bot
from sementic.im_models import IMMessageEvent
from sementic.task_graph import TaskGraph, TaskGraphNode, TaskGraphPlan


def _workspace_event(**overrides) -> IMMessageEvent:
    base = {
        "event_id": "evt_workspace_1",
        "group_session_id": "room_workspace_001",
        "workspace_id": "d1cd810c-8c2b-4d58-a448-be52b675380e",
        "multica_token": "mdt_test_token",
        "user_context": {
            "user_id": "usr_liusong",
            "username": "liusong",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": "post_workspace_1",
            "content": "用C语言写一个51单片机点亮LED灯的简单代码",
            "mentions_registry": [],
        },
    }
    base.update(overrides)
    return IMMessageEvent.model_validate(base)


def test_im_event_lifts_top_level_workspace_fields() -> None:
    event = IMMessageEvent.model_validate(
        {
            "event_id": "evt_1",
            "group_session_id": "room_1",
            "team_id": "team-top",
            "workspace_id": "ws-top",
            "multica_token": "mdt-top",
            "user_context": {
                "user_id": "u1",
                "username": "liusong",
                "is_bot": False,
            },
            "message_context": {
                "msg_id": "p1",
                "content": "hello",
                "mentions_registry": [],
            },
        }
    )
    assert event.team_id == "team-top"
    assert event.workspace_id == "ws-top"
    assert event.multica_token == "mdt-top"
    assert event.user_context.team_id == "team-top"
    assert event.user_context.workspace_id == "ws-top"
    assert event.user_context.multica_token == "mdt-top"
    assert event.has_workspace_credentials is True
    assert event.can_query_im_agents is True


def test_workspace_default_bot_without_mentions() -> None:
    event = _workspace_event()
    bot = _workspace_default_bot(event, sender_user_id="usr_liusong", mentions=[])
    assert bot.bot_user_id == f"workspace:{event.workspace_id}"
    assert bot.owner_user_id == "usr_liusong"


def test_inject_workspace_context_into_plan() -> None:
    event = _workspace_event()
    plan = TaskGraphPlan(
        confidence=0.9,
        reply_to_user="ok",
        graph=TaskGraph(
            id="graph-1",
            input={"channel_id": event.group_session_id, "user_message": event.message_context.content},
            nodes=[TaskGraphNode(id="intake", operation="emit", params={})],
        ),
    )

    updated = _inject_workspace_context_into_plan(plan, event)
    assert updated.graph.input["workspace_id"] == event.workspace_id
    assert updated.graph.input["multica_token"] == event.multica_token
