from __future__ import annotations

from sementic.execution.plan_enricher import enrich_plan_for_execution
from sementic.models import BotProfile
from sementic.task_graph import TaskGraph, TaskGraphNode, TaskGraphPlan


def _sample_plan() -> TaskGraphPlan:
    return TaskGraphPlan(
        confidence=0.9,
        reply_to_user="ok",
        graph=TaskGraph(
            id="graph-old",
            name="demo",
            input={"channel_id": "room_1"},
            start="run_agent",
            nodes=[
                TaskGraphNode(
                    id="run_agent",
                    operation="agent_task",
                    agent={
                        "backend": "multica_job",
                        "agent_key": "bot_jira_123",
                        "prompt": "do work",
                    },
                )
            ],
            edges=[],
        ),
    )


def test_enrich_plan_sets_graph_id_and_agent_id() -> None:
    plan = _sample_plan()
    bots = [
        BotProfile(
            bot_user_id="bot_jira_123",
            display_name="Jira Helper",
            role="assistant",
            multica_agent_id="agent-uuid-1",
            owner_user_id="user_1",
        )
    ]

    enriched = enrich_plan_for_execution(plan, bots, event_id="evt_1")

    assert enriched.graph.id == plan.plan_id
    assert enriched.graph.input["sementic_event_id"] == "evt_1"
    assert enriched.graph.input["sementic_plan_id"] == plan.plan_id
    agent = enriched.graph.nodes[0].agent
    assert agent is not None
    assert agent["agent_id"] == "agent-uuid-1"
    assert agent["backend"] == "multica_job"
    assert agent["agent_name"] == "Jira Helper"


def test_enrich_plan_sets_trace_id() -> None:
    plan = _sample_plan()
    bots = [
        BotProfile(
            bot_user_id="bot_jira_123",
            display_name="Jira Helper",
            role="assistant",
        )
    ]

    enriched = enrich_plan_for_execution(plan, bots, trace_id="tr_plan_1")

    assert enriched.graph.input["sementic_trace_id"] == "tr_plan_1"


def test_enrich_plan_sets_multica_job_backend_without_planner_backend() -> None:
    plan = TaskGraphPlan(
        confidence=0.8,
        reply_to_user="ok",
        graph=TaskGraph(
            id="graph-old",
            nodes=[
                TaskGraphNode(
                    id="run_agent",
                    operation="agent_task",
                    agent={"agent_key": "bot_jira_123", "prompt": "work"},
                ),
            ],
            edges=[],
        ),
    )
    bots = [
        BotProfile(
            bot_user_id="bot_jira_123",
            display_name="Jira Helper",
            role="assistant",
            multica_agent_id="agent-uuid-1",
            owner_user_id="user_1",
        )
    ]

    enriched = enrich_plan_for_execution(plan, bots)

    agent = enriched.graph.nodes[0].agent
    assert agent is not None
    assert agent["backend"] == "multica_job"
    assert agent["agent_id"] == "agent-uuid-1"


def test_enrich_plan_leaves_non_agent_nodes_untouched() -> None:
    plan = TaskGraphPlan(
        confidence=0.8,
        reply_to_user="ok",
        graph=TaskGraph(
            id="graph-old",
            nodes=[
                TaskGraphNode(id="emit_1", operation="emit", params={"message": "hi"}),
            ],
            edges=[],
        ),
    )

    enriched = enrich_plan_for_execution(plan, bots=[])

    assert enriched.graph.nodes[0].agent is None
