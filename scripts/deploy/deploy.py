"""Remote deploy CLI for sementic gateway + worker.

Deploy (from worker repo root):
  python scripts/deploy/deploy.py full
  python scripts/deploy/deploy.py diagnose

Config:
  scripts/deploy/remote.toml
  scripts/deploy/deploy.env
"""
from __future__ import annotations

import argparse
import base64
import io
import os
import sys
import tarfile
import textwrap
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import paramiko

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPTS / "remote.toml"
DEPLOY_ENV_PATH = SCRIPTS / "deploy.env"
WORKER_LOCAL_ENV = REPO_ROOT / ".env"


def resolve_packages(cfg: dict[str, Any]) -> list[tuple[Path, str]]:
    local = cfg.get("local", {})
    gateway_rel = local.get("gateway_path", "../gateway/sementic-gateway")
    gateway_base = Path(gateway_rel)
    if not gateway_base.is_absolute():
        gateway_base = (REPO_ROOT / gateway_base).resolve()
    return [
        (gateway_base, "gateway"),
        (REPO_ROOT, "sementic"),
    ]


@dataclass(frozen=True)
class DeploySettings:
    host: str
    user: str
    password: str
    key_path: str
    llm_key: str
    remote_root: str
    remote_python: str
    gateway_service: str
    worker_service: str
    gateway: dict[str, Any]
    worker: dict[str, Any]
    server: dict[str, Any]

    @property
    def remote_venv(self) -> str:
        return f"{self.remote_root}/venv"

    @property
    def remote_venv_python(self) -> str:
        return f"{self.remote_venv}/bin/python"

    @property
    def gateway_port(self) -> int:
        return int(self.gateway["port"])

    @property
    def redis_key_prefix(self) -> str:
        return str(self.worker.get("redis_key_prefix", "channel:history"))

    @property
    def kafka_bin(self) -> str:
        return str(self.server.get("kafka_bin", "/opt/kafka/bin"))

    @property
    def kafka_topic(self) -> str:
        return str(self.gateway.get("kafka_topic", "im.messages"))


def _load_toml_config() -> dict[str, Any]:
    if not CONFIG_PATH.is_file():
        raise SystemExit(f"missing config: {CONFIG_PATH}")
    with CONFIG_PATH.open("rb") as f:
        return tomllib.load(f)


def _resolve_ssh(deploy_env: dict[str, str], server: dict[str, Any], *, host: str | None, user: str | None) -> tuple[str, str, str, str]:
    password = os.environ.get("DEPLOY_SSH_PASSWORD") or deploy_env.get("DEPLOY_SSH_PASSWORD", "")
    key_path = os.environ.get("DEPLOY_SSH_KEY_PATH") or deploy_env.get("DEPLOY_SSH_KEY_PATH", "")
    resolved_host = host or os.environ.get("DEPLOY_HOST") or deploy_env.get("DEPLOY_HOST") or server["host"]
    resolved_user = user or os.environ.get("DEPLOY_USER") or deploy_env.get("DEPLOY_USER") or server["user"]
    if not password and not key_path:
        raise SystemExit(
            "missing SSH credentials: set DEPLOY_SSH_PASSWORD or DEPLOY_SSH_KEY_PATH in scripts/deploy/deploy.env"
        )
    return resolved_host, resolved_user, password, key_path


def load_settings(*, host: str | None = None, user: str | None = None, require_llm: bool = True) -> DeploySettings:
    cfg = _load_toml_config()
    deploy_env = _load_dotenv(DEPLOY_ENV_PATH)
    server = cfg["server"]
    services = cfg["services"]

    resolved_host, resolved_user, password, key_path = _resolve_ssh(deploy_env, server, host=host, user=user)
    llm_key = _read_llm_key(deploy_env)
    if require_llm and not llm_key:
        raise SystemExit(
            "missing LLM key: set SEMENTIC_LLM_API_KEY in scripts/deploy/deploy.env or .env"
        )

    return DeploySettings(
        host=resolved_host,
        user=resolved_user,
        password=password,
        key_path=key_path,
        llm_key=llm_key,
        remote_root=server["remote_root"],
        remote_python=server["remote_python"],
        gateway_service=services["gateway"],
        worker_service=services["worker"],
        gateway=cfg["gateway"],
        worker=cfg["worker"],
        server=server,
    )


def _load_dotenv(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def _read_llm_key(deploy_env: dict[str, str]) -> str:
    if deploy_env.get("SEMENTIC_LLM_API_KEY"):
        return deploy_env["SEMENTIC_LLM_API_KEY"]
    local = _load_dotenv(WORKER_LOCAL_ENV)
    if local.get("SEMENTIC_LLM_API_KEY"):
        return local["SEMENTIC_LLM_API_KEY"]
    return ""


def connect(settings: DeploySettings) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    kwargs: dict[str, Any] = {
        "hostname": settings.host,
        "username": settings.user,
        "timeout": 30,
        "banner_timeout": 30,
    }
    if settings.key_path:
        kwargs["key_filename"] = settings.key_path
    else:
        kwargs["password"] = settings.password
    client.connect(**kwargs)
    return client


def _safe_print(text: str) -> None:
    if not text.strip():
        return
    sys.stdout.buffer.write(text.rstrip().encode("utf-8", errors="replace") + b"\n")


def run(client: paramiko.SSHClient, command: str, *, check: bool = True) -> tuple[int, str, str]:
    print(f"$ {command}")
    _, stdout, stderr = client.exec_command(command, get_pty=True)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    exit_code = stdout.channel.recv_exit_status()
    _safe_print(out)
    if err.strip():
        _safe_print(err)
    if check and exit_code != 0:
        raise RuntimeError(f"command failed ({exit_code}): {command}")
    return exit_code, out, err


def build_tarball() -> bytes:
    cfg = _load_toml_config()
    packages = resolve_packages(cfg)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for base, remote_name in packages:
            if not base.is_dir():
                raise FileNotFoundError(f"package source not found: {base}")
            for path in base.rglob("*"):
                if any(
                    part in {".git", "__pycache__", ".pytest_cache", ".venv", "venv", "node_modules"}
                    for part in path.parts
                ):
                    continue
                if path.is_file() and path.suffix == ".pyc":
                    continue
                arcname = str(Path(remote_name) / path.relative_to(base)).replace("\\", "/")
                tar.add(path, arcname=arcname)
    return buffer.getvalue()


def upload_bytes(sftp: paramiko.SFTPClient, data: bytes, remote_path: str) -> None:
    with sftp.file(remote_path, "wb") as remote:
        remote.write(data)


def render_gateway_env(settings: DeploySettings) -> str:
    g = settings.gateway
    return textwrap.dedent(
        f"""
        GATEWAY_HOST={g["host"]}
        GATEWAY_PORT={g["port"]}
        REDIS_URL={g["redis_url"]}
        REDIS_BOT_STATUS_KEY_PREFIX={g["redis_bot_status_key_prefix"]}
        KAFKA_BOOTSTRAP_SERVERS={g["kafka_bootstrap_servers"]}
        KAFKA_TOPIC={g["kafka_topic"]}
        KAFKA_CLIENT_ID={g["kafka_client_id"]}
        GATEWAY_CONTENT_SAFETY_ENABLED={str(g["content_safety_enabled"]).lower()}
        GATEWAY_CONTENT_SAFETY_DISABLE={str(g["content_safety_disable"]).lower()}
        GATEWAY_CONTENT_SAFETY_STATIC_ENABLED={str(g["content_safety_static_enabled"]).lower()}
        GATEWAY_CONTENT_SAFETY_LLM_ENABLED={str(g["content_safety_llm_enabled"]).lower()}
        GATEWAY_CONTENT_SAFETY_BLOCK_LEVEL={g["content_safety_block_level"]}
        GATEWAY_CONTENT_SAFETY_FAIL_CLOSED={str(g["content_safety_fail_closed"]).lower()}
        GATEWAY_CONTENT_SAFETY_LLM_URL={g["content_safety_llm_url"]}
        GATEWAY_CONTENT_SAFETY_LLM_API_KEY={settings.llm_key}
        GATEWAY_CONTENT_SAFETY_LLM_MODEL={g["content_safety_llm_model"]}
        GATEWAY_CONTENT_SAFETY_LLM_TIMEOUT_SECONDS={g["content_safety_llm_timeout_seconds"]}
        GATEWAY_CONTENT_SAFETY_LLM_MAX_TOKENS={g["content_safety_llm_max_tokens"]}
        """
    ).strip() + "\n"


def render_worker_env(settings: DeploySettings) -> str:
    w = settings.worker
    return textwrap.dedent(
        f"""
        SEMENTIC_LLM_API_BASE={w["llm_api_base"]}
        SEMENTIC_LLM_API_KEY={settings.llm_key}
        SEMENTIC_LLM_MODEL={w["llm_model"]}
        SEMENTIC_LLM_TIMEOUT_SECONDS={w["llm_timeout_seconds"]}
        SEMENTIC_REDIS_URL={w["redis_url"]}
        SEMENTIC_KAFKA_BOOTSTRAP_SERVERS={w["kafka_bootstrap_servers"]}
        SEMENTIC_KAFKA_TOPIC={w["kafka_topic"]}
        SEMENTIC_BOT_SERVICE_BASE={w["bot_service_base"]}
        """
    ).strip() + "\n"


def render_systemd_unit(name: str, workdir: str, exec_cmd: str) -> str:
    return textwrap.dedent(
        f"""
        [Unit]
        Description={name}
        After=network.target redis.service
        Wants=redis.service

        [Service]
        Type=simple
        WorkingDirectory={workdir}
        ExecStart={exec_cmd}
        Restart=always
        RestartSec=3
        Environment=PYTHONUNBUFFERED=1
        Environment=PATH=/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin

        [Install]
        WantedBy=multi-user.target
        """
    ).strip() + "\n"


def _service_names(settings: DeploySettings, only: str | None) -> list[str]:
    if only == "gateway":
        return [settings.gateway_service]
    if only == "worker":
        return [settings.worker_service]
    return [settings.gateway_service, settings.worker_service]


def upload_code(client: paramiko.SSHClient, settings: DeploySettings) -> None:
    run(client, f"mkdir -p {settings.remote_root}")
    tarball = build_tarball()
    print(f"uploading tarball ({len(tarball)} bytes)")
    sftp = client.open_sftp()
    try:
        remote_tar = f"{settings.remote_root}/sementic-deploy.tgz"
        upload_bytes(sftp, tarball, remote_tar)
    finally:
        sftp.close()

    run(client, f"rm -rf {settings.remote_root}/gateway {settings.remote_root}/sementic")
    run(client, f"tar -xzf {settings.remote_root}/sementic-deploy.tgz -C {settings.remote_root}")
    run(client, f"rm -f {settings.remote_root}/sementic-deploy.tgz")

    run(client, f"{settings.remote_python} -m venv {settings.remote_venv}")
    run(client, f"{settings.remote_venv_python} -m pip install --upgrade pip setuptools wheel")
    run(
        client,
        f"{settings.remote_venv_python} -m pip install -e {settings.remote_root}/gateway -e {settings.remote_root}/sementic",
    )


def push_config(client: paramiko.SSHClient, settings: DeploySettings) -> None:
    gateway_env = render_gateway_env(settings)
    worker_env = render_worker_env(settings)
    run(client, f"cat > {settings.remote_root}/gateway/.env <<'EOF'\n{gateway_env}EOF")
    run(client, f"cat > {settings.remote_root}/sementic/.env <<'EOF'\n{worker_env}EOF")

    gateway_cmd = f"{settings.remote_venv_python} -m gateway.main"
    worker_cmd = f"{settings.remote_venv_python} -m sementic.worker_main"

    run(
        client,
        f"cat > /etc/systemd/system/{settings.gateway_service}.service <<'EOF'\n"
        f"{render_systemd_unit(settings.gateway_service, f'{settings.remote_root}/gateway', gateway_cmd)}EOF",
    )
    run(
        client,
        f"cat > /etc/systemd/system/{settings.worker_service}.service <<'EOF'\n"
        f"{render_systemd_unit(settings.worker_service, f'{settings.remote_root}/sementic', worker_cmd)}EOF",
    )
    run(client, "systemctl daemon-reload")
    run(client, f"systemctl enable {settings.gateway_service} {settings.worker_service}")


def restart_services(client: paramiko.SSHClient, settings: DeploySettings, *, only: str | None = None) -> None:
    names = " ".join(_service_names(settings, only))
    run(client, f"systemctl restart {names}", check=False)
    time.sleep(2)


def show_status(client: paramiko.SSHClient, settings: DeploySettings) -> None:
    names = f"{settings.gateway_service} {settings.worker_service}"
    run(client, f"systemctl --no-pager --full status {names}", check=False)
    run(client, f"curl -s http://127.0.0.1:{settings.gateway_port}/health || true", check=False)


def show_logs(client: paramiko.SSHClient, settings: DeploySettings, *, only: str | None, lines: int) -> None:
    for name in _service_names(settings, only):
        run(client, f"journalctl -u {name} -n {lines} --no-pager", check=False)


def cmd_full(settings: DeploySettings, *, only: str | None = None) -> None:
    client = connect(settings)
    try:
        run(client, f"{settings.remote_python} --version")
        upload_code(client, settings)
        push_config(client, settings)
        restart_services(client, settings, only=only)
        show_status(client, settings)
    finally:
        client.close()


def cmd_code(settings: DeploySettings, *, only: str | None = None) -> None:
    client = connect(settings)
    try:
        upload_code(client, settings)
        restart_services(client, settings, only=only)
        show_status(client, settings)
    finally:
        client.close()


def cmd_config(settings: DeploySettings, *, only: str | None = None) -> None:
    client = connect(settings)
    try:
        push_config(client, settings)
        restart_services(client, settings, only=only)
        show_status(client, settings)
    finally:
        client.close()


def cmd_restart(settings: DeploySettings, *, only: str | None = None) -> None:
    client = connect(settings)
    try:
        restart_services(client, settings, only=only)
        show_status(client, settings)
    finally:
        client.close()


def cmd_status(settings: DeploySettings) -> None:
    client = connect(settings)
    try:
        show_status(client, settings)
    finally:
        client.close()


def cmd_logs(settings: DeploySettings, *, only: str | None, lines: int) -> None:
    client = connect(settings)
    try:
        show_logs(client, settings, only=only, lines=lines)
    finally:
        client.close()


def run_remote_python(client: paramiko.SSHClient, settings: DeploySettings, script: str) -> str:
    encoded = base64.b64encode(script.encode()).decode()
    cmd = f"echo {encoded} | base64 -d | {settings.remote_venv_python}"
    _, out, _ = run(client, cmd, check=False)
    return out


def show_messages(
    client: paramiko.SSHClient,
    settings: DeploySettings,
    *,
    since: str,
    lines: int,
) -> None:
    pattern = "ingress request|ingress completed|ingress response|L1 filter"
    cmd = (
        f"journalctl -u {settings.gateway_service} --since '{since}' --no-pager "
        f"| grep -E '{pattern}' | tail -{lines}"
    )
    run(client, cmd, check=False)


def show_redis(
    client: paramiko.SSHClient,
    settings: DeploySettings,
    *,
    grep: str | None,
    history_limit: int,
) -> None:
    prefix = settings.redis_key_prefix
    needle = grep or ""
    script = textwrap.dedent(
        f"""
        import json
        import redis

        needle = {needle!r}
        prefix = {prefix!r}
        history_limit = {history_limit}
        r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
        print("PING:", r.ping())
        print("DBSIZE:", r.dbsize())
        pattern = f"{{prefix}}:*"
        keys = sorted(r.scan_iter(pattern))
        print(f"history keys ({{pattern}}):", len(keys))
        found = False
        for key in keys:
            items = r.lrange(key, 0, history_limit - 1)
            print(f"--- {{key}} (total {{r.llen(key)}}, showing {{len(items)}})")
            for i, item in enumerate(items):
                if needle and needle not in item:
                    continue
                found = True
                try:
                    obj = json.loads(item)
                    print(f"  [{{i}}]", json.dumps(obj, ensure_ascii=False))
                except Exception:
                    print(f"  [{{i}}]", item[:500])
        if needle:
            for key in r.scan_iter("*"):
                t = r.type(key)
                if t == "list" and not str(key).startswith(prefix + ":"):
                    for item in r.lrange(key, 0, -1):
                        if needle in item:
                            found = True
                            print(f"OTHER LIST {{key}}:", item[:500])
                elif t == "string":
                    val = r.get(key) or ""
                    if needle in val:
                        found = True
                        print(f"STRING {{key}}:", val[:500])
            if not found:
                print(f"NOT FOUND: no value contains {{needle!r}}")
        elif not keys:
            print("(no channel history yet — message may be FILTERED/BLOCKED or worker not consuming Kafka)")
        """
    ).strip()
    run_remote_python(client, settings, script)


def show_kafka_summary(client: paramiko.SSHClient, settings: DeploySettings) -> None:
    kafka_bin = settings.kafka_bin
    topic = settings.kafka_topic
    run(client, "systemctl is-active kafka 2>/dev/null || systemctl is-active kafka.service 2>/dev/null || echo inactive", check=False)
    run(client, "ss -tlnp | grep 9092 || echo 'port 9092 not listening'", check=False)
    run(
        client,
        f"PATH=/usr/bin:/usr/local/bin:$PATH {kafka_bin}/kafka-get-offsets.sh "
        f"--bootstrap-server 127.0.0.1:9092 --topic {topic} 2>&1 || true",
        check=False,
    )
    run(
        client,
        f"journalctl -u {settings.worker_service} --since '10 min ago' --no-pager "
        "| grep -E 'GroupCoordinatorNotAvailableError|kafka consumer ready|im_handler|ERROR' | tail -8",
        check=False,
    )


def cmd_messages(settings: DeploySettings, *, since: str, lines: int) -> None:
    client = connect(settings)
    try:
        print(f"=== gateway ingress logs (since {since}, last {lines}) ===")
        show_messages(client, settings, since=since, lines=lines)
    finally:
        client.close()


def cmd_redis(settings: DeploySettings, *, grep: str | None, history_limit: int) -> None:
    client = connect(settings)
    try:
        print("=== redis ===")
        show_redis(client, settings, grep=grep, history_limit=history_limit)
    finally:
        client.close()


def cmd_diagnose(settings: DeploySettings, *, since: str, lines: int, grep: str | None) -> None:
    client = connect(settings)
    try:
        print("=== service status ===")
        run(client, f"systemctl is-active {settings.gateway_service} {settings.worker_service}", check=False)
        run(client, f"curl -s http://127.0.0.1:{settings.gateway_port}/health || true", check=False)
        print(f"\n=== latest gateway messages (since {since}) ===")
        show_messages(client, settings, since=since, lines=lines)
        print("\n=== redis ===")
        show_redis(client, settings, grep=grep, history_limit=5)
        print("\n=== kafka / worker ===")
        show_kafka_summary(client, settings)
        print(
            textwrap.dedent(
                """
                --- how to read ---
                FILTERED   = L1 noise filter, not sent to Kafka
                BLOCKED    = content safety, not sent to Kafka
                ASYNC_PROCESSING = passed gateway, sent to Kafka (check worker/redis above)
                """
            ).strip()
        )
    finally:
        client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deploy sementic gateway + worker to remote server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            commands:
              full       upload code, push config, restart (default)
              code       upload code + pip install + restart
              config     push .env + systemd units + restart
              restart    restart systemd services
              status     show service status + health check
              logs       tail journalctl
              messages   recent gateway ingress logs (what users sent)
              redis      Redis channel history (+ --grep to search)
              diagnose   messages + redis + kafka/worker summary
            """
        ).strip(),
    )
    deploy_cmds = ("full", "code", "config")
    ops_cmds = ("restart", "status", "logs", "messages", "redis", "diagnose")
    parser.add_argument(
        "command",
        nargs="?",
        default="full",
        choices=deploy_cmds + ops_cmds,
    )
    parser.add_argument("--host", help="override server host from remote.toml / deploy.env")
    parser.add_argument("--user", help="override SSH user")
    parser.add_argument(
        "--only",
        choices=("gateway", "worker"),
        help="limit restart/code/config/logs to one service",
    )
    parser.add_argument("--lines", type=int, default=30, help="lines for logs/messages")
    parser.add_argument("--since", default="2 hours ago", help="journalctl --since for messages/diagnose")
    parser.add_argument("--grep", default="", help="search Redis values (redis/diagnose)")
    parser.add_argument("--history-limit", type=int, default=10, help="messages per channel key in redis")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    require_llm = args.command in ("full", "code", "config")
    settings = load_settings(host=args.host, user=args.user, require_llm=require_llm)

    print(f"target: {settings.user}@{settings.host} ({settings.remote_root})")
    print(f"gateway port: {settings.gateway_port}")

    grep = args.grep.strip() or None
    dispatch = {
        "full": lambda: cmd_full(settings, only=args.only),
        "code": lambda: cmd_code(settings, only=args.only),
        "config": lambda: cmd_config(settings, only=args.only),
        "restart": lambda: cmd_restart(settings, only=args.only),
        "status": lambda: cmd_status(settings),
        "logs": lambda: cmd_logs(settings, only=args.only, lines=args.lines),
        "messages": lambda: cmd_messages(settings, since=args.since, lines=args.lines),
        "redis": lambda: cmd_redis(settings, grep=grep, history_limit=args.history_limit),
        "diagnose": lambda: cmd_diagnose(settings, since=args.since, lines=args.lines, grep=grep),
    }
    dispatch[args.command]()


if __name__ == "__main__":
    main()
