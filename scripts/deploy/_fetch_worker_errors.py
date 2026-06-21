"""Fetch worker error logs for failed kafka messages."""
from __future__ import annotations

import os
import tomllib
from pathlib import Path

import paramiko

cfg = tomllib.loads((Path(__file__).resolve().parents[2] / "scripts/deploy/remote.toml").read_text(encoding="utf-8"))
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(cfg["server"]["host"], username=cfg["server"]["user"], password=os.environ["DEPLOY_SSH_PASSWORD"], timeout=30)
cmds = [
    "journalctl -u sementic-worker --no-pager --since '2026-06-20 17:18:00' --until '2026-06-20 17:19:30'",
    "cat /opt/sementic/sementic/.env",
]
for cmd in cmds:
    print("\n===", cmd[:70], "===")
    _, o, _ = c.exec_command(cmd, get_pty=True, timeout=60)
    text = o.read().decode("utf-8", errors="replace")
    if "journalctl" in cmd:
        for line in text.splitlines():
            if any(k in line for k in ("1781945693854", "evt_smoke", "Traceback", "Error", "failed", "bot service", "Exception")):
                print(line)
    else:
        print(text)
c.close()
