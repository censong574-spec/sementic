from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timedelta

from sementic.llm import ProviderName, create_llm_client
from sementic.models import BotProfile, ChatMessage, PlannerRequest
from sementic.planner import Planner


def build_demo_request() -> PlannerRequest:
    now = datetime.now()
    return PlannerRequest(
        channel_id="room_99",
        sender_user_id="user_hassan",
        sender_display_name="Hassan",
        recent_messages=[
            ChatMessage(
                sender="Hassan",
                text="刚才那个视频增强脚本报错了",
                timestamp=now - timedelta(minutes=12),
            ),
            ChatMessage(
                sender="DevBot",
                text="Exception: NullPointer in sharpen.py line 88",
                timestamp=now - timedelta(minutes=10),
            ),
        ],
        mentioned_bot_ids=["bot_project_assistant"],
        available_bots=[
            BotProfile(
                bot_user_id="bot_project_assistant",
                display_name="项目助手",
                role="DevOps assistant",
                expertise=["deploy", "restart_cli", "log_analysis"],
                owner_user_id="user_hassan",
                share_scope="channel_shared",
                multica_agent_id="multica_agent_001",
                is_online=True,
            )
        ],
        current_message="@项目助手，把刚才那个报错的视频增强 CLI 脚本在我的 Windows 电脑上重启一下。",
    )


async def run(args: argparse.Namespace) -> int:
    llm = create_llm_client(provider=args.provider, mock=args.mock)
    planner = Planner(llm=llm)
    request = build_demo_request()

    if args.verbose:
        print(
            f"Using provider={args.provider}, mock={args.mock}",
            flush=True,
        )

    plan = await planner.plan(request)
    print(json.dumps(plan.model_dump(mode="json"), ensure_ascii=False, indent=2))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the sementic collaboration planner demo")
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use deterministic mock LLM output",
    )
    parser.add_argument(
        "--provider",
        choices=["aliyun", "openai"],
        default="aliyun",
        help="LLM provider preset (default: aliyun / DashScope)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print runtime configuration",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
