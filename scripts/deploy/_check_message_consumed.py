"""Check if a specific user message was consumed on remote."""
from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

import paramiko

REPO = Path(__file__).resolve().parents[2]
cfg = tomllib.loads((REPO / "scripts/deploy/remote.toml").read_text(encoding="utf-8"))
host, user = cfg["server"]["host"], cfg["server"]["user"]
password = os.environ.get("DEPLOY_SSH_PASSWORD", "")
needle = "51单片机点亮LED"

cmds = [
    f"journalctl -u sementic-gateway --no-pager --since '2 hours ago' | grep -F '{needle}' | tail -20",
    f"journalctl -u sementic-gateway --no-pager --since '2 hours ago' | grep -E '点亮LED|单片机点亮|ingress request' | tail -30",
    f"journalctl -u sementic-worker --no-pager --since '2 hours ago' | grep -E '点亮LED|单片机|kafka message processed|kafka message failed|kafka message skipped|task intent|needs_task|BLOCKED|content safety' | tail -30",
    f"journalctl -u sementic-worker --no-pager --since '2026-06-20 17:44:50' --until '2026-06-20 17:46:00'",
    "journalctl -u sementic-gateway --no-pager -n 15",
]

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(host, username=user, password=password, timeout=30)
for cmd in cmds:
    print("\n===", cmd[:100], "===")
    _, o, _ = c.exec_command(cmd, get_pty=True, timeout=90)
    text = o.read().decode("utf-8", errors="replace").rstrip()
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()
c.close()
