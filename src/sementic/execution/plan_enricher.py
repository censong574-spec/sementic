from __future__ import annotations

from sementic.models import BotProfile
from sementic.task_graph import TaskGraphPlan


def enrich_plan_for_execution(
    plan: TaskGraphPlan,
    bots: list[BotProfile],
    *,
    event_id: str | None = None,
) -> TaskGraphPlan:
    """Prepare a planner graph for in-process MAOS / Temporal execution."""
    enriched = plan.model_copy(deep=True)
    enriched.graph.id = plan.plan_id

    graph_input = dict(enriched.graph.input)
    graph_input.setdefault("sementic_plan_id", plan.plan_id)
    if event_id:
        graph_input.setdefault("sementic_event_id", event_id)
    enriched.graph.input = graph_input

    bot_index = _bot_index(bots)
    for node in enriched.graph.nodes:
        if node.operation != "agent_task" or not node.agent:
            continue
        agent = dict(node.agent)
        agent["backend"] = "multica_job"
        agent_key = str(agent.get("agent_key") or agent.get("agent_id") or "").strip()
        bot = _resolve_bot(agent_key, bot_index)
        if bot and bot.multica_agent_id:
            agent["agent_id"] = bot.multica_agent_id
        if bot:
            agent.setdefault("agent_name", bot.display_name)
        elif agent_key:
            agent.setdefault("agent_name", agent_key)
        node.agent = agent

    return enriched


def _bot_index(bots: list[BotProfile]) -> dict[str, BotProfile]:
    index: dict[str, BotProfile] = {}
    for bot in bots:
        index[bot.bot_user_id] = bot
        if bot.multica_agent_id:
            index[bot.multica_agent_id] = bot
        if bot.display_name:
            index[bot.display_name] = bot
            index[bot.display_name.lower()] = bot
    return index


def _resolve_bot(agent_key: str, index: dict[str, BotProfile]) -> BotProfile | None:
    if not agent_key:
        return None
    return index.get(agent_key) or index.get(agent_key.lower())
