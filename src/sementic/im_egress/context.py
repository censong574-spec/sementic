from __future__ import annotations

from sementic.im_egress.mm_ids import mattermost_post_id
from sementic.im_egress.models import EgressContext
from sementic.im_models import IMMessageEvent
from sementic.models import BotProfile
from sementic.task_graph import TaskGraphPlan


def build_egress_context(
    event: IMMessageEvent,
    plan: TaskGraphPlan,
    owned_bots: list[BotProfile],
    *,
    maos_task_id: str | None = None,
) -> EgressContext | None:
    run_task = next((node for node in plan.graph.nodes if node.operation == "agent_task"), None)
    if run_task is None or not run_task.agent:
        return None

    agent = run_task.agent
    agent_key = str(agent.get("agent_key") or agent.get("agent_id") or "").strip()
    bot = _resolve_bot(agent_key, owned_bots) if agent_key else None

    bot_user_id = bot.bot_user_id if bot else agent_key
    bot_username = (
        bot.display_name
        if bot
        else str(agent.get("agent_name") or agent_key or "assistant")
    )
    if not bot_user_id:
        return None

    root_post_id = mattermost_post_id(event.message_context.msg_id)
    return EgressContext(
        event_id=event.event_id,
        channel_id=event.group_session_id,
        root_post_id=root_post_id,
        bot_user_id=bot_user_id,
        bot_username=bot_username,
        plan_id=plan.plan_id,
        maos_task_id=maos_task_id,
    )


def _resolve_bot(agent_key: str, bots: list[BotProfile]) -> BotProfile | None:
    index: dict[str, BotProfile] = {}
    for bot in bots:
        index[bot.bot_user_id] = bot
        if bot.multica_agent_id:
            index[bot.multica_agent_id] = bot
        if bot.display_name:
            index[bot.display_name] = bot
            index[bot.display_name.lower()] = bot
    return index.get(agent_key) or index.get(agent_key.lower())
