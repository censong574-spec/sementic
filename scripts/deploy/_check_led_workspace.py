"""Check latest LED message: kafka consumption + workspace fields."""
from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

import paramiko

cfg = tomllib.loads((Path(__file__).resolve().parents[2] / "scripts/deploy/remote.toml").read_text(encoding="utf-8"))
password = os.environ.get("DEPLOY_SSH_PASSWORD", "")

cmds = [
    "date",
    # Gateway: message + workspace fields
    "journalctl -u sementic-gateway --no-pager --since '3 hours ago' | grep -F '点亮LED' | tail -10",
    "journalctl -u sementic-gateway --no-pager --since '3 hours ago' | grep -E 'workspace_id|has_multica_token|点亮LED|ingress request' | tail -25",
    # Worker: consumption + workspace
    "journalctl -u sementic-worker --no-pager --since '3 hours ago' | grep -E '点亮LED|kafka message processed|kafka message failed|workspace context|workspace_id|has_multica_token|task intent|no_owned_bots' | tail -25",
    "journalctl -u sementic-worker --no-pager -n 30",
]

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(cfg["server"]["host"], username=cfg["server"]["user"], password=password, timeout=30)
for cmd in cmds:
    print("\n===", cmd[:110], "===")
    _, o, _ = c.exec_command(cmd, get_pty=True, timeout=90)
    sys.stdout.buffer.write(o.read())
    sys.stdout.buffer.flush()
c.close()
