"""Fetch sementic_gateway.go from remote."""
from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

import paramiko

cfg = tomllib.loads((Path(__file__).resolve().parents[2] / "scripts/deploy/remote.toml").read_text(encoding="utf-8"))
password = os.environ.get("DEPLOY_SSH_PASSWORD", "")

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(cfg["server"]["host"], username=cfg["server"]["user"], password=password, timeout=30)
for path in [
    "/opt/mattermost-app/repo/server/channels/app/sementic_gateway.go",
    "/opt/mattermost-app/repo/server/channels/app/sementic_gateway_workspace.go",
]:
    print(f"\n===== {path} =====")
    _, o, _ = c.exec_command(f"cat {path}", get_pty=True, timeout=60)
    sys.stdout.buffer.write(o.read())
c.close()
