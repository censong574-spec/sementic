"""Install Go 1.26.3 on remote server via download + SCP."""
from __future__ import annotations

import hashlib
import os
import sys
import tomllib
from pathlib import Path

import paramiko

GO_VERSION = "1.26.3"
GO_ARCH = "linux-amd64"
GO_SHA256 = "2b2cfc7148493da5e73981bffbf3353af381d5f93e789c82c79aff64962eb556"
GO_URL = f"https://dl.google.com/go/go{GO_VERSION}.{GO_ARCH}.tar.gz"
GO_FILENAME = f"go{GO_VERSION}.{GO_ARCH}.tar.gz"

REPO_ROOT = Path(__file__).resolve().parents[2]
REMOTE_TOML = REPO_ROOT / "scripts" / "deploy" / "remote.toml"
DEPLOY_ENV = REPO_ROOT / "scripts" / "deploy" / "deploy.env"
CACHE_DIR = REPO_ROOT / ".cache" / "go-install"
LOCAL_TAR = CACHE_DIR / GO_FILENAME
REMOTE_TAR = f"/root/{GO_FILENAME}"


def load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def load_ssh() -> tuple[str, str, str]:
    with REMOTE_TOML.open("rb") as f:
        cfg = tomllib.load(f)
    env = load_dotenv(DEPLOY_ENV)
    host = os.environ.get("DEPLOY_HOST") or env.get("DEPLOY_HOST") or cfg["server"]["host"]
    user = os.environ.get("DEPLOY_USER") or env.get("DEPLOY_USER") or cfg["server"]["user"]
    password = os.environ.get("DEPLOY_SSH_PASSWORD") or env.get("DEPLOY_SSH_PASSWORD", "")
    if not password:
        raise SystemExit("missing DEPLOY_SSH_PASSWORD in scripts/deploy/deploy.env")
    return host, user, password


def download_go() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if LOCAL_TAR.is_file():
        digest = hashlib.sha256(LOCAL_TAR.read_bytes()).hexdigest()
        if digest == GO_SHA256:
            print(f"reuse cached {LOCAL_TAR} ({LOCAL_TAR.stat().st_size} bytes)")
            return
        print("cached file checksum mismatch, re-downloading")
    print(f"downloading {GO_URL}")
    import urllib.request

    urllib.request.urlretrieve(GO_URL, LOCAL_TAR)
    digest = hashlib.sha256(LOCAL_TAR.read_bytes()).hexdigest()
    if digest != GO_SHA256:
        raise SystemExit(f"checksum mismatch: got {digest}, want {GO_SHA256}")
    print(f"downloaded {LOCAL_TAR} ({LOCAL_TAR.stat().st_size} bytes), sha256 ok")


def run_remote(host: str, user: str, password: str) -> None:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=password, timeout=30)

    print("=== before ===")
    for cmd in ("uname -m", "go version 2>/dev/null || echo 'go not found'", "which go 2>/dev/null || true"):
        _, out, _ = client.exec_command(cmd)
        print(out.read().decode().rstrip())

    print(f"uploading {LOCAL_TAR.name} -> {REMOTE_TAR}")
    sftp = client.open_sftp()
    try:
        sftp.put(str(LOCAL_TAR), REMOTE_TAR)
    finally:
        sftp.close()

    install_script = f"""
set -e
echo "extracting to /usr/local ..."
rm -rf /usr/local/go
tar -C /usr/local -xzf {REMOTE_TAR}
cat > /etc/profile.d/golang.sh <<'EOF'
export GOROOT=/usr/local/go
export PATH=$GOROOT/bin:$PATH
EOF
chmod 644 /etc/profile.d/golang.sh
export GOROOT=/usr/local/go
export PATH=$GOROOT/bin:$PATH
rm -f {REMOTE_TAR}
echo "=== after ==="
go version
which go
"""
    _, out, err = client.exec_command(install_script, get_pty=True)
    sys.stdout.buffer.write(out.read())
    e = err.read().decode()
    if e.strip():
        sys.stdout.buffer.write(e.encode())
    code = out.channel.recv_exit_status()
    client.close()
    if code != 0:
        raise SystemExit(f"remote install failed ({code})")


def main() -> None:
    download_go()
    host, user, password = load_ssh()
    print(f"target: {user}@{host}")
    run_remote(host, user, password)
    print("done")


if __name__ == "__main__":
    main()
