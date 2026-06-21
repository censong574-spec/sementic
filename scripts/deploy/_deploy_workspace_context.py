"""Deploy workspace_id / multica_token support to remote gateway + worker."""
from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

import paramiko

REPO = Path(__file__).resolve().parents[2]
GATEWAY = (REPO.parent / "gateway" / "sementic-gateway").resolve()
REMOTE_TOML = REPO / "scripts" / "deploy" / "remote.toml"

WORKER_FILES = [
    "src/sementic/im_models.py",
    "src/sementic/kafka_consumer.py",
    "src/sementic/models.py",
    "src/sementic/handler.py",
    "src/sementic/planner.py",
    "src/sementic/prompts.py",
    "src/sementic/config.py",
    "src/sementic/multica_client.py",
]
GATEWAY_FILES = [
    "src/gateway/models/im_event.py",
    "src/gateway/infra/kafka_producer.py",
    "src/gateway/app.py",
]


def run(client: paramiko.SSHClient, cmd: str) -> None:
    print(f"\n$ {cmd}")
    _, stdout, _ = client.exec_command(cmd, get_pty=True, timeout=120)
    out = stdout.read().decode("utf-8", errors="replace").rstrip()
    if out:
        sys.stdout.buffer.write((out + "\n").encode("utf-8", errors="replace"))


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
    sftp = client.open_sftp()

    for rel in WORKER_FILES:
        local = REPO / rel
        remote = f"/opt/sementic/sementic/{rel.replace('src/sementic/', 'src/sementic/')}"
        sftp.put(str(local), remote)
        print(f"uploaded worker {rel}")

    for rel in GATEWAY_FILES:
        local = GATEWAY / rel
        remote = f"/opt/sementic/gateway/{rel}"
        sftp.put(str(local), remote)
        print(f"uploaded gateway {rel}")

    sftp.close()

    run(
        client,
        "grep -q '^SEMENTIC_MULTICA_SERVICE_BASE=' /opt/sementic/sementic/.env || "
        "echo 'SEMENTIC_MULTICA_SERVICE_BASE=http://127.0.0.1:8080/api/multica' >> /opt/sementic/sementic/.env",
    )
    run(client, "systemctl restart sementic-gateway sementic-worker")
    run(client, "sleep 5; systemctl is-active sementic-gateway sementic-worker")
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
