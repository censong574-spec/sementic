"""Inspect planner output for latest LED message on remote."""
from __future__ import annotations

import json
import os
import sys
import textwrap
import tomllib
from pathlib import Path

import paramiko

EVENT_ID = "z9a6ejxftirodmpkmewi6zr46r:1781956809826"
REMOTE_SCRIPT = textwrap.dedent(
    f"""
    import asyncio
    import json
    import os
    import sys

    sys.path.insert(0, "/opt/sementic/sementic/src")
    os.chdir("/opt/sementic/sementic")

    from redis.asyncio import Redis
    from sementic.bot_registry import BotRegistry
    from sementic.config import RedisSettings
    from sementic.handler import MessageHandler
    from sementic.im_models import IMMessageEvent
    from sementic.intent_classifier import TaskIntentClassifier
    from sementic.llm import create_intent_llm_client, create_llm_client
    from sementic.planner import Planner
    from sementic.redis_history import RedisHistoryStore

    payload = {{
        "event_id": "{EVENT_ID}",
        "group_session_id": "ge84c14y5jnt8xd38fp33or5yo",
        "workspace_id": "d1cd810c-8c2b-4d58-a448-be52b675380e",
        "multica_token": "mdt_d1e33180210b8ba0e63c605237b8a764343a64e6",
        "user_context": {{
            "user_id": "z9a6ejxftirodmpkmewi6zr46r",
            "username": "liusong",
            "is_bot": False,
            "ownership": "OTHERS",
            "workspace_id": "d1cd810c-8c2b-4d58-a448-be52b675380e",
            "multica_token": "mdt_d1e33180210b8ba0e63c605237b8a764343a64e6",
        }},
        "message_context": {{
            "msg_id": "{EVENT_ID}",
            "content": "用C语言写一个51单片机点亮LED灯的简单代码",
            "mentions_registry": [],
        }},
    }}

    async def main() -> None:
        event = IMMessageEvent.model_validate(payload)
        redis = Redis.from_url(RedisSettings().url, decode_responses=True)
        handler = MessageHandler(
            history_store=RedisHistoryStore(redis, RedisSettings()),
            intent_classifier=TaskIntentClassifier(llm=create_intent_llm_client(provider="aliyun")),
            planner=Planner(llm=create_llm_client(provider="aliyun")),
            bot_registry=BotRegistry(),
        )
        response = await handler.handle(event)
        result = {{
            "skipped_planning": response.skipped_planning,
            "skip_reason": response.skip_reason,
            "task_intent": response.task_intent.model_dump() if response.task_intent else None,
        }}
        if response.plan is not None:
            plan = response.plan.model_dump(mode="json")
            token = plan.get("graph", {{}}).get("input", {{}}).get("multica_token", "")
            if token:
                plan["graph"]["input"]["multica_token"] = token[:8] + "..." + token[-6:]
            result["reply_to_user"] = plan.get("reply_to_user")
            result["graph_input"] = plan.get("graph", {{}}).get("input")
            result["graph_id"] = plan.get("graph", {{}}).get("id")
            result["graph_name"] = plan.get("graph", {{}}).get("name")
            result["node_ids"] = [n.get("id") for n in plan.get("graph", {{}}).get("nodes", [])]
            result["agent_keys"] = [
                n.get("agent", {{}}).get("agent_key")
                for n in plan.get("graph", {{}}).get("nodes", [])
                if n.get("agent")
            ]
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
    """
)

cfg = tomllib.loads((Path(__file__).resolve().parents[2] / "scripts/deploy/remote.toml").read_text(encoding="utf-8"))
password = os.environ.get("DEPLOY_SSH_PASSWORD", "")

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(cfg["server"]["host"], username=cfg["server"]["user"], password=password, timeout=30)

sftp = c.open_sftp()
remote_path = "/tmp/inspect_plan.py"
with sftp.file(remote_path, "w") as f:
    f.write(REMOTE_SCRIPT)
sftp.close()

cmd = (
    "cd /opt/sementic/sementic && "
    "set -a && source .env 2>/dev/null; set +a && "
    "/opt/sementic/venv/bin/python /tmp/inspect_plan.py"
)
print("$", cmd)
_, stdout, stderr = c.exec_command(cmd, get_pty=True, timeout=180)
out = stdout.read().decode("utf-8", errors="replace")
err = stderr.read().decode("utf-8", errors="replace")
sys.stdout.buffer.write(out.encode("utf-8", errors="replace"))
if err.strip():
    sys.stdout.buffer.write(("\nSTDERR:\n" + err).encode("utf-8", errors="replace"))
c.close()
