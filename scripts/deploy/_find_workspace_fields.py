"""Find workspace_id / multica_token in remote gateway logs and code."""
from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

import paramiko

cfg = tomllib.loads((Path(__file__).resolve().parents[2] / "scripts/deploy/remote.toml").read_text(encoding="utf-8"))
password = os.environ.get("DEPLOY_SSH_PASSWORD", "")

cmds = [
    "journalctl -u sementic-gateway --no-pager --since '6 hours ago' | grep -iE 'workspace|multica_token|multica' | tail -25",
    "grep -r 'workspace_id\\|multica_token\\|managed_workspace\\|managed_multica' /opt/mattermost-app /opt/sementic 2>/dev/null | grep -v '.pyc' | head -30",
    "find /opt /root -name 'sync_user_workspace_props.py' 2>/dev/null | head -5",
]

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(cfg["server"]["host"], username=cfg["server"]["user"], password=password, timeout=30)
for cmd in cmds:
    print("\n===", cmd[:110], "===")
    _, o, _ = c.exec_command(cmd, get_pty=True, timeout=120)
    sys.stdout.buffer.write(o.read())
c.close()
