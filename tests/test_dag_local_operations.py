from __future__ import annotations

from maos_runtime.dag_workflow import (
    _is_local_operation_node,
    _run_local_operation_payload,
)


def test_emit_node_is_local_operation() -> None:
    node = {
        "id": "intake",
        "operation": "emit",
        "params": {"user_message": "$input.user_message"},
    }
    assert _is_local_operation_node(node) is True


def test_agent_task_node_is_not_local_operation() -> None:
    node = {
        "id": "run_task",
        "operation": "agent_task",
        "agent": {"backend": "multica_job", "agent_key": "bot-1"},
    }
    assert _is_local_operation_node(node) is False


def test_emit_resolves_input_params() -> None:
    payload = _run_local_operation_payload(
        {
            "id": "intake",
            "operation": "emit",
            "params": {
                "user_message": "$input.user_message",
                "channel_id": "$input.channel_id",
            },
        },
        dependency_executions={},
        graph_input={
            "user_message": "STM32 LED blink",
            "channel_id": "ch-1",
        },
        results={},
    )
    assert payload == {
        "user_message": "STM32 LED blink",
        "channel_id": "ch-1",
    }
