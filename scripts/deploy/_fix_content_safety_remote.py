"""Deploy gateway static_filter fix and verify blocked message now passes."""
from __future__ import annotations

import json
import os
import sys
import time
import tomllib
from pathlib import Path

import paramiko

GATEWAY_REPO = Path(__file__).resolve().parents[3] / "gateway" / "sementic-gateway"
REPO = Path(__file__).resolve().parents[2]
STATIC_FILTER = (
    GATEWAY_REPO / "src" / "gateway" / "filters" / "content_safety" / "static_filter.py"
)
REMOTE_TOML = REPO / "scripts" / "deploy" / "remote.toml"


def run(client: paramiko.SSHClient, cmd: str) -> str:
    print(f"\n$ {cmd}")
    _, stdout, _ = client.exec_command(cmd, get_pty=True, timeout=120)
    out = stdout.read().decode("utf-8", errors="replace")
    if out.strip():
        sys.stdout.buffer.write((out.rstrip() + "\n").encode("utf-8", errors="replace"))
        sys.stdout.buffer.flush()
    return out


def main() -> int:
    password = os.environ.get("DEPLOY_SSH_PASSWORD", "")
    if not password:
        print("Set DEPLOY_SSH_PASSWORD", file=sys.stderr)
        return 1

    cfg = tomllib.loads(REMOTE_TOML.read_text(encoding="utf-8"))
    host, user = cfg["server"]["host"], cfg["server"]["user"]
    gw_port = cfg["gateway"]["port"]

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=password, timeout=30)

    remote_path = "/opt/sementic/gateway/src/gateway/filters/content_safety/static_filter.py"
    sftp = client.open_sftp()
    sftp.put(str(STATIC_FILTER), remote_path)
    sftp.close()
    print(f"uploaded {STATIC_FILTER.name}")

    run(client, "systemctl restart sementic-gateway; sleep 4; systemctl is-active sementic-gateway")

    payload = {
        "event_id": "evt_fix_paomadeng_test",
        "group_session_id": "ge84c14y5jnt8xd38fp33or5yo",
        "user_context": {
            "user_id": "z9a6ejxftirodmpkmewi6zr46r",
            "username": "Hassan",
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": "post_fix_test",
            "content": "用C语言写一个51单片机跑马灯程序",
            "mentions_registry": [],
        },
    }
    body = json.dumps(payload, ensure_ascii=False)
    run(
        client,
        f"curl -s -X POST http://127.0.0.1:{gw_port}/api/v1/im/messages "
        f"-H 'Content-Type: application/json' -d '{body}'",
    )
    time.sleep(2)
    run(
        client,
        "journalctl -u sementic-gateway --no-pager --since '30 sec ago' | "
        "grep -E 'evt_fix_paomadeng_test|BLOCKED|ASYNC_PROCESSING|content safety' | tail -10",
    )
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
