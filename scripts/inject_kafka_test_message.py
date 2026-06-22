#!/usr/bin/env python3
"""Inject a test IM message into Kafka and wait for worker processing signals.

Simulates the Gateway Kafka envelope (see gateway.infra.kafka_producer.build_kafka_payload).

Examples:
  # On cloud host (Kafka/Redis on localhost):
  python scripts/inject_kafka_test_message.py \\
    --token "$MULTICA_TOKEN" \\
    --content "用C语言写一个80C51单片机点亮LED灯，一闪一闪的简单代码"

  # From laptop: SSH to remote and run inject there:
  python scripts/inject_kafka_test_message.py --remote \\
    --token "mdt_..." \\
    --content "用C语言写一个80C51单片机点亮LED灯，一闪一闪的简单代码"

Required for full MAOS/multica path:
  --workspace-id and --token (or env KAFKA_TEST_MULTICA_TOKEN)

Optional env (see load_settings()):
  KAFKA_TEST_BOOTSTRAP_SERVERS, KAFKA_TEST_TOPIC, KAFKA_TEST_REDIS_URL,
  KAFKA_TEST_CHANNEL_ID, KAFKA_TEST_USER_ID, KAFKA_TEST_WORKSPACE_ID
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_DIR = REPO_ROOT / "scripts" / "deploy"
REMOTE_TOML = DEPLOY_DIR / "remote.toml"
DEPLOY_ENV = DEPLOY_DIR / "deploy.env"
LOCAL_ENV = REPO_ROOT / ".env"


@dataclass(frozen=True)
class TestSettings:
    bootstrap_servers: str
    topic: str
    redis_url: str
    channel_id: str
    user_id: str
    username: str
    workspace_id: str
    multica_token: str
    redis_key_prefix: str
    remote_host: str
    remote_user: str
    remote_password: str
    remote_venv_python: str
    worker_service: str


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def load_settings() -> TestSettings:
    file_env: dict[str, str] = {}
    for path in (LOCAL_ENV, DEPLOY_ENV):
        file_env.update(_read_env_file(path))

    def env(name: str, default: str = "") -> str:
        return os.environ.get(name) or file_env.get(name) or default

    remote_cfg: dict = {}
    if REMOTE_TOML.is_file():
        remote_cfg = tomllib.loads(REMOTE_TOML.read_text(encoding="utf-8"))

    server = remote_cfg.get("server", {})
    worker = remote_cfg.get("worker", {})
    services = remote_cfg.get("services", {})

    return TestSettings(
        bootstrap_servers=env("KAFKA_TEST_BOOTSTRAP_SERVERS", worker.get("kafka_bootstrap_servers", "127.0.0.1:9092")),
        topic=env("KAFKA_TEST_TOPIC", worker.get("kafka_topic", "im.messages")),
        redis_url=env("KAFKA_TEST_REDIS_URL", worker.get("redis_url", "redis://127.0.0.1:6379/0")),
        channel_id=env("KAFKA_TEST_CHANNEL_ID", "ge84c14y5jnt8xd38fp33or5yo"),
        user_id=env("KAFKA_TEST_USER_ID", "z9a6ejxftirodmpkmewi6zr46r"),
        username=env("KAFKA_TEST_USERNAME", "liusong"),
        workspace_id=env("KAFKA_TEST_WORKSPACE_ID", "d1cd810c-8c2b-4d58-a448-be52b675380e"),
        multica_token=env("KAFKA_TEST_MULTICA_TOKEN", ""),
        redis_key_prefix=env("KAFKA_TEST_REDIS_KEY_PREFIX", worker.get("redis_key_prefix", "channel:history")),
        remote_host=env("DEPLOY_HOST", server.get("host", "")),
        remote_user=env("DEPLOY_USER", server.get("user", "root")),
        remote_password=env("DEPLOY_SSH_PASSWORD", ""),
        remote_venv_python=env("REMOTE_VENV_PYTHON", f"{server.get('remote_root', '/opt/sementic')}/venv/bin/python"),
        worker_service=services.get("worker", "sementic-worker"),
    )


def build_kafka_payload(
    *,
    content: str,
    channel_id: str,
    user_id: str,
    username: str,
    workspace_id: str,
    multica_token: str,
    event_id: str | None = None,
) -> dict:
    ts = int(time.time() * 1000)
    event_id = event_id or f"{user_id}:kafka-test-{ts}"
    payload: dict = {
        "event_id": event_id,
        "group_session_id": channel_id,
        "user_context": {
            "user_id": user_id,
            "username": username,
            "is_bot": False,
            "ownership": "OTHERS",
        },
        "message_context": {
            "msg_id": event_id,
            "content": content,
            "mentions_registry": [],
        },
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    if workspace_id:
        payload["workspace_id"] = workspace_id
        payload["user_context"]["workspace_id"] = workspace_id
    if multica_token:
        payload["multica_token"] = multica_token
        payload["user_context"]["multica_token"] = multica_token
    return payload


async def produce_to_kafka(settings: TestSettings, payload: dict) -> None:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        client_id="sementic-kafka-test-inject",
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )
    await producer.start()
    try:
        metadata = await producer.send_and_wait(
            settings.topic,
            value=payload,
            key=payload["group_session_id"],
        )
        print(
            f"Kafka published topic={metadata.topic} "
            f"partition={metadata.partition} offset={metadata.offset}"
        )
    finally:
        await producer.stop()


async def wait_for_redis_message(
    settings: TestSettings,
    *,
    event_id: str,
    channel_id: str,
    timeout_seconds: float,
) -> bool:
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    key = f"{settings.redis_key_prefix}:{channel_id}"
    deadline = time.time() + timeout_seconds
    try:
        while time.time() < deadline:
            raw = await client.lindex(key, 0)
            if raw:
                try:
                    item = json.loads(raw)
                except json.JSONDecodeError:
                    item = {}
                if item.get("msg_id") == event_id or event_id in str(item.get("content", "")):
                    print("Redis history updated (latest channel message):")
                    print(json.dumps(item, ensure_ascii=False, indent=2))
                    return True
            await asyncio.sleep(2.0)
    finally:
        await client.aclose()
    return False


def _worker_log_patterns(event_id: str) -> list[str]:
    return [
        event_id,
        "kafka message processed",
        "kafka message failed",
        "submitted plan",
        "task graph plan",
        "bot service query",
        "no_owned_bots",
        "MULTICA_JOB_TOKEN",
        "multica_job",
        "Connection refused",
        "task intent",
        "workspace context",
    ]


def grep_worker_logs_local(settings: TestSettings, event_id: str, *, since: str = "5 minutes ago") -> None:
    grep = "|".join(_worker_log_patterns(event_id))
    cmd = (
        f"journalctl -u {settings.worker_service} --since '{since}' --no-pager "
        f"| grep -E '{grep}' | tail -40"
    )
    print(f"\n=== worker logs ({since}) ===")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding="utf-8", errors="replace")
    out = (result.stdout or "").strip()
    if out:
        sys.stdout.buffer.write((out + "\n").encode("utf-8", errors="replace"))
    else:
        print("(no matching worker log lines)")


def grep_worker_logs_remote(settings: TestSettings, event_id: str, *, since: str = "5 minutes ago") -> None:
    import paramiko

    if not settings.remote_host or not settings.remote_password:
        print("Skip remote worker log check: DEPLOY_HOST / DEPLOY_SSH_PASSWORD not set")
        return

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        settings.remote_host,
        username=settings.remote_user,
        password=settings.remote_password,
        timeout=30,
    )
    grep = "|".join(_worker_log_patterns(event_id))
    cmd = (
        f"journalctl -u {settings.worker_service} --since '{since}' --no-pager "
        f"| grep -E '{grep}' | tail -40"
    )
    print(f"\n=== worker logs ({since}) ===")
    _, stdout, stderr = client.exec_command(cmd, timeout=60)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    if out.strip():
        sys.stdout.buffer.write(out.encode("utf-8", errors="replace"))
    else:
        print("(no matching worker log lines)")
    if err.strip():
        print(err.rstrip())
    client.close()


def grep_worker_logs(settings: TestSettings, event_id: str, *, since: str = "5 minutes ago", via_ssh: bool | None = None) -> None:
    use_ssh = via_ssh if via_ssh is not None else bool(settings.remote_host and settings.remote_password)
    if use_ssh:
        grep_worker_logs_remote(settings, event_id, since=since)
    else:
        grep_worker_logs_local(settings, event_id, since=since)


def run_on_remote(settings: TestSettings, args: argparse.Namespace) -> int:
    import paramiko

    if not settings.remote_password:
        print("Set DEPLOY_SSH_PASSWORD in scripts/deploy/deploy.env for --remote", file=sys.stderr)
        return 2

    script_path = Path(__file__).resolve()
    remote_script = "/tmp/inject_kafka_test_message.py"
    remote_config = "/tmp/inject_kafka_test_config.json"
    config = {
        "content": args.content,
        "channel_id": settings.channel_id,
        "user_id": settings.user_id,
        "username": settings.username,
        "workspace_id": settings.workspace_id,
        "multica_token": settings.multica_token,
        "wait_seconds": args.wait,
        "event_id": args.event_id,
        "bootstrap_servers": settings.bootstrap_servers,
        "topic": settings.topic,
        "redis_url": settings.redis_url,
    }

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        settings.remote_host,
        username=settings.remote_user,
        password=settings.remote_password,
        timeout=30,
    )
    sftp = client.open_sftp()
    try:
        sftp.put(str(script_path), remote_script)
        with sftp.file(remote_config, "w") as handle:
            handle.write(json.dumps(config, ensure_ascii=False))
    finally:
        sftp.close()

    cmd = f"{settings.remote_venv_python} {remote_script} --config-file {remote_config}"
    print(f"$ ssh {settings.remote_user}@{settings.remote_host} {cmd}")
    _, stdout, stderr = client.exec_command(cmd, timeout=180)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    exit_status = stdout.channel.recv_exit_status()
    if out.strip():
        sys.stdout.buffer.write(out.encode("utf-8", errors="replace"))
    if err.strip():
        sys.stderr.buffer.write(err.encode("utf-8", errors="replace"))
    client.close()

    event_id = args.event_id
    for line in out.splitlines():
        if '"event_id"' in line:
            try:
                fragment = line.split('"event_id"', 1)[1]
                event_id = fragment.split('"', 2)[1]
            except IndexError:
                pass

    if event_id:
        grep_worker_logs(settings, event_id, via_ssh=True)
    return exit_status


async def run_local(
    settings: TestSettings,
    *,
    content: str,
    event_id: str | None,
    wait_seconds: float,
    check_logs: bool,
) -> int:
    if not settings.multica_token:
        print(
            "Warning: no --token / KAFKA_TEST_MULTICA_TOKEN; "
            "worker may skip bots or fail at multica_job.",
            file=sys.stderr,
        )

    payload = build_kafka_payload(
        content=content,
        channel_id=settings.channel_id,
        user_id=settings.user_id,
        username=settings.username,
        workspace_id=settings.workspace_id,
        multica_token=settings.multica_token,
        event_id=event_id,
    )
    event_id = payload["event_id"]
    print("Kafka payload:")
    print(json.dumps(_redact_payload(payload), ensure_ascii=False, indent=2))

    await produce_to_kafka(settings, payload)
    print(f"Waiting up to {wait_seconds:.0f}s for Redis channel history...")
    found = await wait_for_redis_message(
        settings,
        event_id=event_id,
        channel_id=settings.channel_id,
        timeout_seconds=wait_seconds,
    )
    if not found:
        print("Redis: message not seen in channel history (timeout)", file=sys.stderr)
        if check_logs:
            grep_worker_logs(settings, event_id, via_ssh=False)
        return 1

    print("Redis: worker consumed message (history updated)")
    if check_logs:
        grep_worker_logs(settings, event_id, via_ssh=False)
    return 0


def _redact_payload(payload: dict) -> dict:
    redacted = json.loads(json.dumps(payload))
    token = redacted.get("multica_token")
    if isinstance(token, str) and len(token) > 12:
        redacted["multica_token"] = token[:8] + "..." + token[-4:]
    user_ctx = redacted.get("user_context")
    if isinstance(user_ctx, dict):
        t = user_ctx.get("multica_token")
        if isinstance(t, str) and len(t) > 12:
            user_ctx["multica_token"] = t[:8] + "..." + t[-4:]
    return redacted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inject a Kafka test IM message and observe processing")
    parser.add_argument(
        "--content",
        default="用C语言写一个80C51单片机点亮LED灯，一闪一闪的简单代码",
        help="User message text",
    )
    parser.add_argument("--channel", dest="channel_id", default=None)
    parser.add_argument("--user-id", default=None)
    parser.add_argument("--username", default=None)
    parser.add_argument("--workspace-id", default=None)
    parser.add_argument("--token", dest="multica_token", default=None, help="multica_token (mdt_...)")
    parser.add_argument("--bootstrap", dest="bootstrap_servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--redis-url", default=None)
    parser.add_argument("--event-id", default=None)
    parser.add_argument("--wait", type=float, default=90.0, help="Seconds to wait for Redis")
    parser.add_argument(
        "--remote",
        action="store_true",
        help="SSH to cloud host and run inject against local Kafka/Redis there",
    )
    parser.add_argument(
        "--check-logs",
        action="store_true",
        help="After wait, grep worker journal for this event_id (local journalctl on host)",
    )
    parser.add_argument(
        "--config-file",
        default=None,
        help=argparse.SUPPRESS,
    )
    return parser.parse_args()


def apply_config_file(args: argparse.Namespace, settings: TestSettings) -> tuple[argparse.Namespace, TestSettings]:
    if not args.config_file:
        return args, settings
    data = json.loads(Path(args.config_file).read_text(encoding="utf-8"))
    args.content = data.get("content", args.content)
    args.event_id = data.get("event_id", args.event_id)
    args.wait = float(data.get("wait_seconds", args.wait))
    settings = TestSettings(
        bootstrap_servers=data.get("bootstrap_servers", settings.bootstrap_servers),
        topic=data.get("topic", settings.topic),
        redis_url=data.get("redis_url", settings.redis_url),
        channel_id=data.get("channel_id", settings.channel_id),
        user_id=data.get("user_id", settings.user_id),
        username=data.get("username", settings.username),
        workspace_id=data.get("workspace_id", settings.workspace_id),
        multica_token=data.get("multica_token", settings.multica_token),
        redis_key_prefix=settings.redis_key_prefix,
        remote_host=settings.remote_host,
        remote_user=settings.remote_user,
        remote_password=settings.remote_password,
        remote_venv_python=settings.remote_venv_python,
        worker_service=settings.worker_service,
    )
    return args, settings


def merge_settings(base: TestSettings, args: argparse.Namespace) -> TestSettings:
    return TestSettings(
        bootstrap_servers=args.bootstrap_servers or base.bootstrap_servers,
        topic=args.topic or base.topic,
        redis_url=args.redis_url or base.redis_url,
        channel_id=args.channel_id or base.channel_id,
        user_id=args.user_id or base.user_id,
        username=args.username or base.username,
        workspace_id=args.workspace_id or base.workspace_id,
        multica_token=args.multica_token or base.multica_token,
        redis_key_prefix=base.redis_key_prefix,
        remote_host=base.remote_host,
        remote_user=base.remote_user,
        remote_password=base.remote_password,
        remote_venv_python=base.remote_venv_python,
        worker_service=base.worker_service,
    )


def main() -> int:
    args = parse_args()
    settings = merge_settings(load_settings(), args)
    args, settings = apply_config_file(args, settings)

    if args.remote:
        return run_on_remote(settings, args)

    return asyncio.run(
        run_local(
            settings,
            content=args.content,
            event_id=args.event_id,
            wait_seconds=args.wait,
            check_logs=args.check_logs,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
