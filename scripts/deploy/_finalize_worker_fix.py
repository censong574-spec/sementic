"""Finalize worker fix: disable bot service, deploy handler fallback, replay user message."""
from __future__ import annotations

import json
import os
import sys
import time
import tomllib
from pathlib import Path

import paramiko

REPO = Path(__file__).resolve().parents[2]
REMOTE_TOML = REPO / "scripts" / "deploy" / "remote.toml"
HANDLER = REPO / "src" / "sementic" / "handler.py"


def run(client: paramiko.SSHClient, cmd: str) -> str:
    print(f"\n$ {cmd}")
    _, stdout, _ = client.exec_command(cmd, get_pty=True, timeout=120)
    out = stdout.read().decode("utf-8", errors="replace")
    if out.strip():
        print(out.rstrip())
    return out


def main() -> int:
    password = os.environ.get("DEPLOY_SSH_PASSWORD", "")
    if not password:
        print("Set DEPLOY_SSH_PASSWORD", file=sys.stderr)
        return 1

    cfg = tomllib.loads(REMOTE_TOML.read_text(encoding="utf-8"))
    host, user = cfg["server"]["host"], cfg["server"]["user"]
    topic = cfg["gateway"]["kafka_topic"]
    kafka_home = "/opt/kafka"

    payload = {
        "event_id": "evt_replay_agent_demo_v2",
        "group_session_id": "ge84c14y5jnt8xd38fp33or5yo",
        "user_context": {
            "user_id": "z9a6ejxftirodmpkmewi6zr46r",
            "username": "Hassan",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": "post_replay_demo_v2",
            "content": "用python语言写一个简单的AI Agent demo",
            "mentions_registry": [
                {
                    "entity_id": "managed-hermes-desktop-qbg4054",
                    "ownership": "MY_SYSTEM",
                }
            ],
        },
        "ingested_at": "replay",
    }
    local_json = REPO / "scripts" / "deploy" / "_replay_payload.json"
    local_json.write_text(
        json.dumps(payload, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=password, timeout=30)

    sftp = client.open_sftp()
    sftp.put(str(HANDLER), "/opt/sementic/sementic/src/sementic/handler.py")
    sftp.put(str(local_json), "/tmp/replay_payload.json")
    sftp.close()
    print("uploaded handler.py and replay payload")

    run(
        client,
        "grep SEMENTIC_BOT /opt/sementic/sementic/.env",
    )

    kb = (
        "JAVA_BIN=$(readlink -f /proc/$(pgrep -f 'kafka.Kafka' | head -1)/exe); "
        "export JAVA_HOME=$(dirname $(dirname \"$JAVA_BIN\")); "
        f"export PATH=$JAVA_HOME/bin:$PATH:{kafka_home}/bin"
    )
    run(
        client,
        f"{kb}; cat /tmp/replay_payload.json | {kafka_home}/bin/kafka-console-producer.sh "
        f"--bootstrap-server 127.0.0.1:9092 --topic {topic}",
    )
    time.sleep(20)
    run(
        client,
        "journalctl -u sementic-worker --no-pager --since '1 min ago' | "
        "grep -E 'evt_replay_agent_demo_v2|kafka message processed|kafka message failed|"
        "task intent|no_owned_bots|bot service|planner|ERROR|Traceback|skip_reason|needs_task' | tail -30",
    )

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
