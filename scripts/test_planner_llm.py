"""Diagnose planner LLM task graph output (loads .env)."""

from __future__ import annotations

import asyncio
import json

from sementic.llm import OpenAICompatibleClient, create_llm_client, extract_json_object
from sementic.models import BotProfile, ChatMessage, PlannerRequest
from sementic.prompts import SYSTEM_PROMPT, build_user_prompt
from sementic.task_graph import TaskGraphPlan, normalize_task_graph_payload


def _weather_request() -> PlannerRequest:
    return PlannerRequest(
        channel_id="opiadbqaa7bqmx5xrq8bptydsc",
        sender_user_id="k7hgneowkbybj8zx1q9i6weccw",
        sender_display_name="刘颂",
        recent_messages=[
            ChatMessage(sender="刘颂", text="查询今天南京的天气"),
        ],
        mentioned_bot_ids=[],
        available_bots=[
            BotProfile(
                bot_user_id="bot_liusong_code",
                display_name="代码助手",
                role="帮你写 Python、调试脚本",
                expertise=["general"],
                owner_user_id="k7hgneowkbybj8zx1q9i6weccw",
                share_scope="private",
                is_online=True,
            ),
        ],
        current_message="查询今天南京的天气",
    )


async def main() -> None:
    request = _weather_request()
    user_prompt = build_user_prompt(
        channel_id=request.channel_id,
        sender_user_id=request.sender_user_id,
        sender_display_name=request.sender_display_name,
        recent_messages=request.recent_messages,
        available_bots=request.available_bots,
        mentioned_bot_ids=request.mentioned_bot_ids,
        current_message=request.current_message,
    )

    client = create_llm_client()
    assert isinstance(client, OpenAICompatibleClient)

    raw = await client.complete(system=SYSTEM_PROMPT, user=user_prompt)
    print("--- raw ---")
    print(raw)
    print("--- end raw ---")

    payload = normalize_task_graph_payload(extract_json_object(raw))
    plan = TaskGraphPlan.model_validate(payload)
    print(json.dumps(plan.model_dump(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
