"""Diagnose Mattermost / gateway / bridge failures on remote."""
from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

import paramiko

REPO = Path(__file__).resolve().parents[2]
cfg = tomllib.loads((REPO / "scripts/deploy/remote.toml").read_text(encoding="utf-8"))
host, user = cfg["server"]["host"], cfg["server"]["user"]
gw_port = cfg["gateway"]["port"]
password = os.environ.get("DEPLOY_SSH_PASSWORD", "")

cmds = [
    "date; uptime",
    "systemctl is-active mattermost mattermost-bridge sementic-gateway sementic-worker kafka redis 2>/dev/null || true",
    "systemctl status mattermost --no-pager -n 8 2>/dev/null || echo no_mattermost_unit",
    "systemctl status mattermost-bridge --no-pager -n 15 2>/dev/null || echo no_bridge_unit",
    "systemctl status sementic-gateway --no-pager -n 12 2>/dev/null",
    f"curl -s -o /dev/null -w 'mm_ping=%{{http_code}}\\n' --connect-timeout 3 http://127.0.0.1:8065/api/v4/system/ping || echo mm_fail",
    f"curl -s -o /dev/null -w 'gw_health=%{{http_code}}\\n' --connect-timeout 3 http://127.0.0.1:{gw_port}/health || echo gw_fail",
    "curl -s -o /dev/null -w 'bridge_health=%{http_code}\\n' --connect-timeout 3 http://127.0.0.1:8090/health || echo bridge_fail",
    "ss -tlnp | grep -E '8065|8081|8090|8080' || echo no_listeners",
    "pgrep -af mattermost_bridge || echo no_bridge_process",
    "journalctl -u mattermost --no-pager --since '2 hours ago' | tail -30",
    f"journalctl -u sementic-gateway --no-pager --since '2 hours ago' | tail -50",
    "ls -la /opt/sementic/gateway/scripts/mattermost-bridge.env 2>/dev/null || echo no_bridge_env",
    "grep -E '^(GATEWAY_URL|MATTERMOST_URL|BRIDGE_PORT|MATTERMOST_TOKEN)=' /opt/sementic/gateway/scripts/mattermost-bridge.env 2>/dev/null | sed 's/TOKEN=.*/TOKEN=***redacted***/' || true",
    "grep -r '8090\\|mattermost/webhook\\|sementic-bridge' /opt/mattermost-app 2>/dev/null | head -5 || "
    "find /opt/mattermost-app -name 'config.json' 2>/dev/null | head -3",
]

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(host, username=user, password=password, timeout=30)
for cmd in cmds:
    print("\n===", cmd[:120], "===")
    _, o, _ = c.exec_command(cmd, get_pty=True, timeout=90)
    text = o.read().decode("utf-8", errors="replace").rstrip()
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()
c.close()
