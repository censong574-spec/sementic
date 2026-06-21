"""Trace latest message; reads SSH password from DEPLOY_SSH_PASSWORD env."""
from __future__ import annotations

import base64
import os
import paramiko
import sys

HOST = os.environ.get("DEPLOY_HOST", "1.95.200.170")
USER = os.environ.get("DEPLOY_USER", "root")
PASSWORD = os.environ.get("DEPLOY_SSH_PASSWORD", "")


def run(client: paramiko.SSHClient, label: str, cmd: str) -> None:
    print(f"\n=== {label} ===")
    _, stdout, stderr = client.exec_command(cmd, timeout=60)
    stdout.channel.settimeout(65)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    if out.strip():
        sys.stdout.buffer.write(out.rstrip().encode("utf-8", errors="replace") + b"\n")
    if err.strip():
        print("ERR:", err.rstrip())


def main() -> None:
    if not PASSWORD:
        raise SystemExit("set DEPLOY_SSH_PASSWORD")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=USER, password=PASSWORD, timeout=15)

    run(
        client,
        "gateway (3h)",
        "journalctl -u sementic-gateway --since '3 hours ago' --no-pager | grep -E 'ingress request|ingress completed' | tail -20",
    )
    run(
        client,
        "worker (3h)",
        "journalctl -u sementic-worker --since '3 hours ago' --no-pager | grep -E 'kafka message|bot service|no_owned_bots|task intent|workspace context|failed|skipped' | tail -40",
    )

    script = r"""
import json, redis
r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
print("PING", r.ping(), "DBSIZE", r.dbsize())
best = ("", "", {})
for key in sorted(r.scan_iter("channel:history:*")):
    item = (r.lrange(key, 0, 0) or [""])[0]
    try:
        obj = json.loads(item)
    except Exception:
        continue
    ts = obj.get("timestamp") or ""
    if ts >= best[0]:
        best = (ts, key, obj)
    content = obj.get("content") or ""
    if "一闪" in content or ("LED" in content and "51" in content):
        print("MATCH", key, ts, content[:150])
print("LATEST_TS", best[0])
print("LATEST_KEY", best[1])
print("LATEST_CONTENT", (best[2].get("content") or "")[:200])
"""
    b64 = base64.b64encode(script.encode()).decode()
    run(client, "redis", f"echo {b64} | base64 -d | /opt/sementic/venv/bin/python")
    client.close()


if __name__ == "__main__":
    main()
