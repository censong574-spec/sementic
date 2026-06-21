"""Fetch recent worker logs from remote."""
from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

import paramiko

REPO = Path(__file__).resolve().parents[2]
REMOTE_TOML = REPO / "scripts" / "deploy" / "remote.toml"


def main() -> int:
    password = os.environ.get("DEPLOY_SSH_PASSWORD", "")
    if not password:
        print("Set DEPLOY_SSH_PASSWORD", file=sys.stderr)
        return 1

    cfg = tomllib.loads(REMOTE_TOML.read_text(encoding="utf-8"))
    host, user = cfg["server"]["host"], cfg["server"]["user"]

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=password, timeout=30)

    cmd = "journalctl -u sementic-worker --no-pager --since '3 min ago' | tail -60"
    print(f"$ {cmd}")
    _, stdout, _ = client.exec_command(cmd, get_pty=True, timeout=60)
    print(stdout.read().decode("utf-8", errors="replace"))
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
