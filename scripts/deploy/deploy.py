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
TEMPORAL_DIR = SCRIPTS / "temporal"
TEMPORAL_ARTIFACTS_DIR = TEMPORAL_DIR / "artifacts"
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
    temporal_service: str
    gateway: dict[str, Any]
    worker: dict[str, Any]
    temporal: dict[str, Any]
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

    @property
    def temporal_root(self) -> str:
        return str(self.temporal.get("remote_root", "/opt/temporal"))


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
        temporal_service=services.get("temporal", "temporal-server"),
        gateway=cfg["gateway"],
        worker=cfg["worker"],
        temporal=cfg.get("temporal", {}),
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


def _read_mm_bot_tokens_json(deploy_env: dict[str, str]) -> str:
    if deploy_env.get("SEMENTIC_MM_BOT_TOKENS_JSON"):
        return deploy_env["SEMENTIC_MM_BOT_TOKENS_JSON"]
    local = _load_dotenv(WORKER_LOCAL_ENV)
    return local.get("SEMENTIC_MM_BOT_TOKENS_JSON", "")


def _read_remote_env_vars(client: paramiko.SSHClient, path: str, prefix: str) -> dict[str, str]:
    code, stdout, _ = run(
        client,
        f"grep '^{prefix}' {path} 2>/dev/null || true",
        check=False,
    )
    if code != 0 or not stdout.strip():
        return {}
    out: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def _resolve_mm_egress_config(
    client: paramiko.SSHClient,
    settings: DeploySettings,
    deploy_env: dict[str, str],
) -> dict[str, str]:
    w = settings.worker
    remote_env_path = f"{settings.remote_root}/sementic/.env"
    preserved = _read_remote_env_vars(client, remote_env_path, "SEMENTIC_MM_")
    tokens_json = _read_mm_bot_tokens_json(deploy_env) or preserved.get("SEMENTIC_MM_BOT_TOKENS_JSON", "")
    default_token = deploy_env.get("SEMENTIC_MM_DEFAULT_BOT_TOKEN") or preserved.get(
        "SEMENTIC_MM_DEFAULT_BOT_TOKEN", ""
    )
    if not tokens_json and not default_token:
        bridge_paths = [
            f"{settings.remote_root}/gateway/scripts/mattermost-bridge.env",
            "/opt/sementic/gateway/scripts/mattermost-bridge.env",
            "/opt/mattermost-bridge/mattermost-bridge.env",
        ]
        for bridge_env in bridge_paths:
            bridge = _read_remote_env_vars(client, bridge_env, "MATTERMOST_")
            bridge_token = bridge.get("MATTERMOST_TOKEN", "")
            if bridge_token:
                default_token = bridge_token
                break
    return {
        "url": str(w.get("mm_url") or preserved.get("SEMENTIC_MM_URL") or "http://127.0.0.1:8065"),
        "enabled": str(w.get("mm_enabled", True)).lower(),
        "bot_tokens_json": tokens_json,
        "default_bot_token": default_token,
        "poll_interval_seconds": preserved.get("SEMENTIC_MM_POLL_INTERVAL_SECONDS", "5"),
        "completion_timeout_seconds": preserved.get("SEMENTIC_MM_COMPLETION_TIMEOUT_SECONDS", "7200"),
    }


def _remote_mm_external_ingress_ready(client: paramiko.SSHClient) -> bool:
    code, stdout, _ = run(
        client,
        "redis-cli GET shared:service_token 2>/dev/null || true",
        check=False,
    )
    if code != 0:
        return False
    token = stdout.strip()
    return bool(token) and token not in {"(nil)", "nil"}


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


def run(
    client: paramiko.SSHClient,
    command: str,
    *,
    check: bool = True,
    timeout: float = 600.0,
) -> tuple[int, str, str]:
    print(f"$ {command}", flush=True)
    _, stdout, stderr = client.exec_command(command, get_pty=False, timeout=timeout)
    stdout.channel.settimeout(timeout + 30.0)
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
    exclude_dirs = {
        ".git",
        "__pycache__",
        ".pytest_cache",
        ".venv",
        "venv",
        "node_modules",
        ".mypy_cache",
        ".cache",
        "artifacts",
    }
    buffer = io.BytesIO()
    seen: set[str] = set()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for base, remote_name in packages:
            if not base.is_dir():
                raise FileNotFoundError(f"package source not found: {base}")
            for path in base.rglob("*"):
                if any(part in exclude_dirs for part in path.parts):
                    continue
                if path.name == ".env" or path.name.startswith(".env."):
                    continue
                if path.is_file() and path.suffix == ".pyc":
                    continue
                arcname = str(Path(remote_name) / path.relative_to(base)).replace("\\", "/")
                if arcname in seen:
                    continue
                seen.add(arcname)
                tar.add(path, arcname=arcname, recursive=False)
    return buffer.getvalue()


def upload_bytes(sftp: paramiko.SFTPClient, data: bytes, remote_path: str) -> None:
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(data)
        tmp.flush()
        local_path = tmp.name
    try:
        sftp.put(local_path, remote_path)
        remote_size = sftp.stat(remote_path).st_size
        if remote_size != len(data):
            raise RuntimeError(
                f"upload size mismatch for {remote_path}: "
                f"local={len(data)} remote={remote_size}"
            )
    finally:
        Path(local_path).unlink(missing_ok=True)


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


def render_worker_env(settings: DeploySettings, *, mm: dict[str, str] | None = None) -> str:
    w = settings.worker
    mm = mm or {}
    lines = [
        f"SEMENTIC_LLM_API_BASE={w['llm_api_base']}",
        f"SEMENTIC_LLM_API_KEY={settings.llm_key}",
        f"SEMENTIC_LLM_MODEL={w['llm_model']}",
        f"SEMENTIC_LLM_TIMEOUT_SECONDS={w['llm_timeout_seconds']}",
        f"SEMENTIC_REDIS_URL={w['redis_url']}",
        f"SEMENTIC_KAFKA_BOOTSTRAP_SERVERS={w['kafka_bootstrap_servers']}",
        f"SEMENTIC_KAFKA_TOPIC={w['kafka_topic']}",
        f"SEMENTIC_BOT_AGENTS_URL={w['bot_agents_url']}",
        f"SEMENTIC_MAOS_TEMPORAL_ADDRESS={w.get('maos_temporal_address', '127.0.0.1:7233')}",
        f"SEMENTIC_MAOS_MULTICA_JOB_API_BASE={w.get('maos_multica_job_api_base', 'http://127.0.0.1:8080')}",
    ]
    if mm:
        lines.extend(
            [
                f"SEMENTIC_MM_URL={mm.get('url', 'http://127.0.0.1:8065')}",
                f"SEMENTIC_MM_ENABLED={mm.get('enabled', 'true')}",
                f"SEMENTIC_MM_DEFAULT_BOT_TOKEN={mm.get('default_bot_token', '')}",
                f"SEMENTIC_MM_BOT_TOKENS_JSON={mm.get('bot_tokens_json', '')}",
                f"SEMENTIC_MM_POLL_INTERVAL_SECONDS={mm.get('poll_interval_seconds', '5')}",
                f"SEMENTIC_MM_COMPLETION_TIMEOUT_SECONDS={mm.get('completion_timeout_seconds', '7200')}",
            ]
        )
    return "\n".join(lines) + "\n"


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
    remote_tar = f"{settings.remote_root}/sementic-deploy.tgz"
    staging = f"{settings.remote_root}/.deploy-staging"
    run(client, f"rm -f {remote_tar}")
    sftp = client.open_sftp()
    try:
        upload_bytes(sftp, tarball, remote_tar)
    finally:
        sftp.close()

    run(client, f"rm -rf {settings.remote_root}/gateway {settings.remote_root}/sementic {staging}")
    run(client, f"tar -xzf {remote_tar} -C {settings.remote_root}")
    run(client, f"rm -f {remote_tar}")

    run(client, f"{settings.remote_python} -m venv {settings.remote_venv}")
    run(client, f"{settings.remote_venv_python} -m pip install --upgrade pip setuptools wheel")
    run(
        client,
        f"{settings.remote_venv_python} -m pip install -e {settings.remote_root}/gateway -e {settings.remote_root}/sementic",
    )


def push_config(client: paramiko.SSHClient, settings: DeploySettings) -> None:
    deploy_env = _load_dotenv(DEPLOY_ENV_PATH)
    mm = _resolve_mm_egress_config(client, settings, deploy_env)
    gateway_env = render_gateway_env(settings)
    worker_env = render_worker_env(settings, mm=mm)
    run(client, f"cat > {settings.remote_root}/gateway/.env <<'EOF'\n{gateway_env}EOF")
    run(client, f"cat > {settings.remote_root}/sementic/.env <<'EOF'\n{worker_env}EOF")
    if _remote_mm_external_ingress_ready(client):
        print("worker mm egress: external_ingress via redis shared:service_token")
    elif mm.get("bot_tokens_json") or mm.get("default_bot_token"):
        print("worker mm egress: configured (bot token present)")
    else:
        print("worker mm egress: WARNING no auth — posts will be skipped")

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
        push_config(client, settings)
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


def _read_temporal_postgres_password(deploy_env: dict[str, str]) -> str:
    return (
        os.environ.get("TEMPORAL_POSTGRES_PASSWORD")
        or deploy_env.get("TEMPORAL_POSTGRES_PASSWORD", "")
    )


def _temporal_tarball_name(settings: DeploySettings) -> str:
    version = str(settings.temporal.get("server_version", "1.27.2"))
    return f"temporal_{version}_linux_amd64.tar.gz"


def _temporal_bundle_fallback_dirs(settings: DeploySettings) -> list[str]:
    raw = settings.temporal.get("bundle_fallback_dirs", "/home/liusong")
    if isinstance(raw, list):
        return [str(d).strip() for d in raw if str(d).strip()]
    return [part.strip() for part in str(raw).split(":") if part.strip()]


def temporal_shell_prefix(settings: DeploySettings) -> str:
    root = settings.temporal_root
    return f"INSTALL_ROOT={root} ENV_FILE={root}/temporal.env"


def render_temporal_env(settings: DeploySettings, *, postgres_password: str = "") -> str:
    t = settings.temporal
    root = t.get("remote_root", "/opt/temporal")
    fallbacks = ":".join(_temporal_bundle_fallback_dirs(settings))
    lines = [
        f"INSTALL_ROOT={root}",
        f"TEMPORAL_PERSISTENCE={t.get('persistence', 'postgres')}",
        f"TEMPORAL_BIN={root}/bin/temporal",
        f"TEMPORAL_GRPC_HOST={t.get('grpc_host', '127.0.0.1')}",
        f"TEMPORAL_GRPC_PORT={t.get('grpc_port', 7233)}",
        f"TEMPORAL_UI_PORT={t.get('ui_port', 8233)}",
        f"TEMPORAL_DB_FILE={t.get('db_file', '/opt/temporal/data/temporal.db')}",
        f"TEMPORAL_SERVICE_NAME={settings.temporal_service}",
        f"TEMPORAL_NAMESPACE={t.get('namespace', 'default')}",
        f"TEMPORAL_CLI_VERSION={t.get('cli_version', '1.3.0')}",
        f"TEMPORAL_SERVER_VERSION={t.get('server_version', '1.27.2')}",
        f"TEMPORAL_POSTGRES_HOST={t.get('postgres_host', '127.0.0.1')}",
        f"TEMPORAL_POSTGRES_PORT={t.get('postgres_port', 5432)}",
        f"TEMPORAL_POSTGRES_USER={t.get('postgres_user', 'temporal')}",
        f"TEMPORAL_POSTGRES_DB={t.get('postgres_db', 'temporal')}",
        f"TEMPORAL_POSTGRES_VISIBILITY_DB={t.get('postgres_visibility_db', 'temporal_visibility')}",
        f"TEMPORAL_POSTGRES_CUSTOM_ROOT={t.get('postgres_custom_root', '/opt/postgresql-14')}",
        f"TEMPORAL_POSTGRES_ADMIN_SOCKET={t.get('postgres_admin_socket', '/tmp')}",
        f"TEMPORAL_BUNDLE_FALLBACK_DIRS={fallbacks}",
        f"SECRETS_FILE={root}/temporal.secrets.env",
    ]
    if postgres_password.strip():
        lines.append(f"TEMPORAL_POSTGRES_PASSWORD={postgres_password.strip()}")
    return "\n".join(lines) + "\n"


def stage_remote_temporal_bundle(client: paramiko.SSHClient, settings: DeploySettings) -> None:
    """Copy server tarball from remote fallback dirs if artifacts/ is empty."""
    root = settings.temporal_root
    name = _temporal_tarball_name(settings)
    fallbacks = _temporal_bundle_fallback_dirs(settings)
    dirs_quoted = " ".join(f'"{d}"' for d in fallbacks) or '"/home/liusong"'
    run(
        client,
        f"""
set -e
name="{name}"
dest="{root}/artifacts/$name"
if [[ -s "$dest" ]]; then
  echo "artifact ready: $dest ($(stat -c%s "$dest") bytes)"
  exit 0
fi
mkdir -p {root}/artifacts
for dir in {dirs_quoted}; do
  if [[ -f "$dir/$name" ]]; then
    cp -f "$dir/$name" "$dest"
    echo "staged $dir/$name -> $dest ($(stat -c%s "$dest") bytes)"
    exit 0
  fi
done
echo "note: no remote bundle in fallback dirs; deploy.sh may download from GitHub"
""",
        check=False,
        timeout=60.0,
    )


def upload_temporal_bundle(
    client: paramiko.SSHClient,
    settings: DeploySettings,
    *,
    postgres_password: str = "",
) -> None:
    deploy_sh = TEMPORAL_DIR / "deploy.sh"
    server_template = TEMPORAL_DIR / "server.yaml.template"
    if not deploy_sh.is_file():
        raise FileNotFoundError(f"missing {deploy_sh}")
    if not server_template.is_file():
        raise FileNotFoundError(f"missing {server_template}")

    remote_root = settings.temporal_root
    remote_deploy = f"{remote_root}/deploy"
    run(
        client,
        f"mkdir -p {remote_deploy} {remote_root}/data {remote_root}/logs "
        f"{remote_root}/bin {remote_root}/config {remote_root}/server {remote_root}/artifacts",
    )

    sftp = client.open_sftp()
    try:
        upload_bytes(sftp, deploy_sh.read_bytes(), f"{remote_deploy}/deploy.sh")
        upload_bytes(sftp, server_template.read_bytes(), f"{remote_deploy}/server.yaml.template")
        upload_bytes(
            sftp,
            render_temporal_env(settings, postgres_password=postgres_password).encode("utf-8"),
            f"{remote_root}/temporal.env",
        )
        if postgres_password.strip():
            secrets = f"TEMPORAL_POSTGRES_PASSWORD={postgres_password.strip()}\n"
            upload_bytes(sftp, secrets.encode("utf-8"), f"{remote_root}/temporal.secrets.env")
        tarball = TEMPORAL_ARTIFACTS_DIR / _temporal_tarball_name(settings)
        if tarball.is_file():
            remote_tar = f"{remote_root}/artifacts/{tarball.name}"
            print(f"uploading {tarball.name} ({tarball.stat().st_size} bytes)", flush=True)
            upload_bytes(sftp, tarball.read_bytes(), remote_tar)
        else:
            print(
                f"local artifact missing: {tarball}\n"
                f"  download: .\\scripts\\deploy\\temporal\\fetch_temporal_server.ps1\n"
                f"  or place tarball on remote under {settings.temporal.get('bundle_fallback_dirs', '/home/liusong')}",
                flush=True,
            )
    finally:
        sftp.close()

    run(client, f"chmod +x {remote_deploy}/deploy.sh")
    if postgres_password.strip():
        run(client, f"chmod 600 {remote_root}/temporal.secrets.env")
    stage_remote_temporal_bundle(client, settings)


def cmd_temporal_bundle(settings: DeploySettings) -> None:
    """Upload deploy scripts + stage server tarball (no install/start)."""
    deploy_env = _load_dotenv(DEPLOY_ENV_PATH)
    postgres_password = _read_temporal_postgres_password(deploy_env)
    client = connect(settings)
    try:
        upload_temporal_bundle(client, settings, postgres_password=postgres_password)
    finally:
        client.close()


def cmd_temporal(settings: DeploySettings) -> None:
    deploy_env = _load_dotenv(DEPLOY_ENV_PATH)
    postgres_password = _read_temporal_postgres_password(deploy_env)
    client = connect(settings)
    root = settings.temporal_root
    prefix = temporal_shell_prefix(settings)
    try:
        upload_temporal_bundle(client, settings, postgres_password=postgres_password)
        run(
            client,
            f"{prefix} bash {root}/deploy/deploy.sh start",
            timeout=900.0,
        )
    finally:
        client.close()


def _print_temporal_health(client: paramiko.SSHClient, settings: DeploySettings) -> None:
    t = settings.temporal
    root = t.get("remote_root", "/opt/temporal")
    grpc = f"{t.get('grpc_host', '127.0.0.1')}:{t.get('grpc_port', 7233)}"
    unit = settings.temporal_service
    socket = t.get("postgres_admin_socket", "/tmp")
    port = t.get("postgres_port", 5432)
    cmds = [
        f"systemctl is-active {unit} postgresql-14-custom 2>/dev/null || true",
        f"grep -E '^(Description|ExecStart)=' /etc/systemd/system/{unit}.service 2>/dev/null || true",
        f"ss -tlnp | grep -E ':7233|:5432' || true",
        f"timeout 10 {root}/bin/temporal operator cluster health --address {grpc} 2>&1 || echo 'health: unavailable'",
    ]
    if t.get("persistence", "postgres") == "postgres":
        cmds.append(
            f"cd {socket} && runuser -u postgres -- psql -h {socket} -p {port} -Atc "
            f"\"SELECT datname FROM pg_database WHERE datname LIKE 'temporal%' ORDER BY 1;\" "
            f"2>/dev/null || echo 'postgres temporal DBs: unavailable'"
        )
    for cmd in cmds:
        print(f"$ {cmd}", flush=True)
        try:
            run(client, cmd, check=False, timeout=30.0)
        except (TimeoutError, OSError) as exc:
            print(f"(timeout: {exc})", flush=True)


def cmd_temporal_status(settings: DeploySettings) -> None:
    client = connect(settings)
    try:
        _print_temporal_health(client, settings)
    finally:
        client.close()


def cmd_temporal_logs(settings: DeploySettings, *, lines: int) -> None:
    client = connect(settings)
    try:
        prefix = temporal_shell_prefix(settings)
        run(
            client,
            f"{prefix} bash {settings.temporal_root}/deploy/deploy.sh logs {lines}",
            check=False,
        )
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
        if settings.temporal:
            print("\n=== temporal server ===")
            _print_temporal_health(client, settings)
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
              full              upload code, push config, restart (default)
              code              upload code + pip install + restart
              config            push .env + systemd units + restart
              restart           restart systemd services
              status            show service status + health check
              logs              tail journalctl
              messages          recent gateway ingress logs (what users sent)
              redis             Redis channel history (+ --grep to search)
              diagnose          messages + redis + kafka/worker summary
              temporal              upload scripts + install/start Temporal Server (PostgreSQL)
              temporal-bundle       upload scripts + stage server tarball only
              temporal-status       systemd, gRPC health, postgres DBs
              temporal-logs         journalctl for temporal-server
            """
        ).strip(),
    )
    deploy_cmds = ("full", "code", "config")
    ops_cmds = (
        "restart",
        "status",
        "logs",
        "messages",
        "redis",
        "diagnose",
        "temporal",
        "temporal-bundle",
        "temporal-status",
        "temporal-logs",
    )
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
    settings = load_settings(
        host=args.host,
        user=args.user,
        require_llm=require_llm,
    )

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
        "temporal": lambda: cmd_temporal(settings),
        "temporal-bundle": lambda: cmd_temporal_bundle(settings),
        "temporal-status": lambda: cmd_temporal_status(settings),
        "temporal-logs": lambda: cmd_temporal_logs(settings, lines=args.lines),
    }
    dispatch[args.command]()


if __name__ == "__main__":
    main()
