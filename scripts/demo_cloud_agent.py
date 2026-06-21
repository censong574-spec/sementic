#!/usr/bin/env python3
"""
简单 AI Agent Demo + 验证 sementic 是否消费「看一下云上机器」。

流程:
  1. 简单对话 Agent（可选，直连 DashScope）
  2. 构造 IM 事件 POST 到 gateway
  3. 本地模拟 worker Kafka 消费链路（不依赖真实 Kafka）

用法:
  python scripts/demo_cloud_agent.py
  python scripts/demo_cloud_agent.py --gateway http://1.95.200.170:8081
  python scripts/demo_cloud_agent.py --local-only
  python scripts/demo_cloud_agent.py --message "看一下云上机器"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tomllib
import urllib.error
import urllib.request
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from fakeredis.aioredis import FakeRedis

from sementic.bot_registry import BotRegistry
from sementic.config import RedisSettings
from sementic.handler import MessageHandler
from sementic.im_models import IMMessageEvent
from sementic.intent_classifier import TaskIntentClassifier
from sementic.llm import MockIntentLLMClient, MockLLMClient
from sementic.planner import Planner
from sementic.redis_history import RedisHistoryStore

DEFAULT_MESSAGE = "看一下云上机器"


def load_gateway_url() -> str:
    remote_toml = REPO_ROOT / "scripts" / "deploy" / "remote.toml"
    if remote_toml.is_file():
        cfg = tomllib.loads(remote_toml.read_bytes())
        host = cfg["server"]["host"]
        port = cfg["gateway"]["port"]
        return f"http://{host}:{port}"
    return "http://127.0.0.1:8080"


def simple_agent_reply(user_message: str) -> str:
    """最小 Agent：把用户话发给 DashScope，没有 key 则 mock。"""
    api_key = os.getenv("DASHSCOPE_API_KEY") or os.getenv("SEMENTIC_LLM_API_KEY")
    if not api_key:
        return f"[mock-agent] 收到：{user_message}。若要查云上机器状态，需要 @你的运维机器人并说明具体动作。"

    payload = {
        "model": os.getenv("SEMENTIC_LLM_MODEL", "qwen-plus"),
        "messages": [
            {"role": "system", "content": "你是简洁的云运维助手。"},
            {"role": "user", "content": user_message},
        ],
    }
    request = urllib.request.Request(
        "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        body = json.loads(response.read().decode("utf-8"))
    return body["choices"][0]["message"]["content"]


def build_im_event(message: str) -> dict:
    return {
        "event_id": f"evt_demo_{uuid.uuid4().hex[:12]}",
        "group_session_id": "room_cloud_ops_demo",
        "user_context": {
            "user_id": "usr_hassan_95",
            "username": "Hassan",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": f"post_{uuid.uuid4().hex[:8]}",
            "parent_msg_id": None,
            "content": message,
            "mentions_registry": [
                {"entity_id": "bot_project_assistant", "ownership": "MY_SYSTEM"},
            ],
        },
    }


def post_to_gateway(gateway_url: str, event: dict) -> dict:
    url = gateway_url.rstrip("/") + "/api/v1/im/messages"
    data = json.dumps(event, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


async def simulate_worker_consume(event_payload: dict) -> dict:
    """模拟 Kafka 消费后 worker 的 MessageHandler 处理。"""
    redis = FakeRedis(decode_responses=True)
    handler = MessageHandler(
        history_store=RedisHistoryStore(redis, RedisSettings()),
        intent_classifier=TaskIntentClassifier(llm=MockIntentLLMClient()),
        planner=Planner(llm=MockLLMClient()),
        bot_registry=BotRegistry(),
    )
    event = IMMessageEvent.model_validate(event_payload)
    result = await handler.handle(event)
    return result.model_dump(mode="json")


def explain_gateway_response(body: dict) -> str:
    if body.get("filtered"):
        return "网关 L1 过滤，未入 Kafka，worker 不会消费。"
    if body.get("rejected"):
        return f"网关熔断拒绝（{body.get('reject_reason')}），未入 Kafka。"
    if body.get("queued"):
        return "网关已入 Kafka，worker 应异步消费（需 worker 进程在跑）。"
    if body.get("accepted") and not body.get("queued"):
        return "网关已接受但未入队（可能是 bot 消息）。"
    return "网关响应需人工查看。"


def explain_worker_result(result: dict) -> str:
    if result.get("skipped_planning"):
        reason = result.get("skip_reason")
        intent = result.get("task_intent") or {}
        if reason == "no_task_intent":
            return (
                f"worker 已消费，但意图判定为不需要任务 (needs_task={intent.get('needs_task')}, "
                f"reason={intent.get('reason')})，未进入 LLM 编排。"
            )
        if reason == "no_owned_bots":
            return "worker 已消费，但发言人名下没有可用 bot，跳过编排。"
        return f"worker 已消费，跳过编排：{reason}"
    if result.get("plan"):
        return "worker 已消费并完成 LLM 编排，生成了执行计划。"
    return "worker 处理完成，详见 JSON。"


async def run_demo(*, message: str, gateway_url: str | None, local_only: bool) -> int:
    print("=" * 60)
    print("1) 简单 AI Agent 对话")
    print("=" * 60)
    agent_reply = simple_agent_reply(message)
    print(f"用户: {message}")
    print(f"Agent: {agent_reply}\n")

    event = build_im_event(message)
    kafka_payload = {
        "event_id": event["event_id"],
        "group_session_id": event["group_session_id"],
        "user_context": event["user_context"],
        "message_context": event["message_context"],
        "ingested_at": "demo",
    }

    print("=" * 60)
    print("2) 本地模拟 worker 消费（Mock 意图 + Mock 编排）")
    print("=" * 60)
    worker_result = await simulate_worker_consume(kafka_payload)
    print(json.dumps(worker_result, ensure_ascii=False, indent=2))
    print(f"\n结论: {explain_worker_result(worker_result)}\n")

    if local_only:
        print("(--local-only，未请求远程 gateway)")
        return 0

    url = gateway_url or load_gateway_url()
    print("=" * 60)
    print(f"3) 请求远程 gateway: {url}")
    print("=" * 60)
    try:
        gateway_body = post_to_gateway(url, event)
        print(json.dumps(gateway_body, ensure_ascii=False, indent=2))
        print(f"\n结论: {explain_gateway_response(gateway_body)}")
        if gateway_body.get("queued"):
            print(
                "\n提示: 若需确认云上 worker 实消费，在服务器执行:\n"
                f"  python scripts/deploy/deploy.py diagnose --grep '{message}'"
            )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"gateway HTTP {exc.code}: {detail}")
        return 1
    except urllib.error.URLError as exc:
        print(f"无法连接 gateway: {exc}")
        print("可先用 --local-only 看本地模拟，或检查 gateway 是否启动。")
        return 1

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple agent demo + sementic consume check")
    parser.add_argument("--message", default=DEFAULT_MESSAGE)
    parser.add_argument("--gateway", default=None, help="e.g. http://1.95.200.170:8081")
    parser.add_argument("--local-only", action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run_demo(
        message=args.message,
        gateway_url=args.gateway,
        local_only=args.local_only,
    )))


if __name__ == "__main__":
    main()
