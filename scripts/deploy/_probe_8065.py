"""Probe remote 8065 / Mattermost / multica."""
import os
import tomllib
from pathlib import Path
import paramiko

root = Path(__file__).resolve().parents[2]
toml = root / "scripts" / "deploy" / "remote.toml"
env_path = root / "scripts" / "deploy" / "deploy.env"
cfg = tomllib.loads(toml.read_bytes())
password = os.environ.get("DEPLOY_SSH_PASSWORD", "")
if env_path.is_file():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("DEPLOY_SSH_PASSWORD="):
            password = password or line.split("=", 1)[1].strip()
host = cfg["server"]["host"]
user = cfg["server"]["user"]
cmds = [
    "systemctl is-active mattermost 2>/dev/null; systemctl is-active docker 2>/dev/null; docker ps --format '{{.Names}} {{.Ports}}' 2>/dev/null | head -10",
    "ss -tlnp | grep -E '8065|8080|8067' || netstat -tlnp 2>/dev/null | grep -E '8065|8080|8067' || echo 'no listeners'",
    "curl -s -o /dev/null -w 'localhost8065=%{http_code}\\n' --connect-timeout 3 http://127.0.0.1:8065/ || echo curl8065_fail",
    "curl -s -o /dev/null -w 'localhost8065_api=%{http_code}\\n' --connect-timeout 3 http://127.0.0.1:8065/api/v4/system/ping || true",
    "curl -s -o /dev/null -w 'multica_register=%{http_code}\\n' --connect-timeout 3 -X POST http://127.0.0.1:8065/api/multica/api/daemon/register -H 'Content-Type: application/json' -d '{}' || true",
    "firewall-cmd --list-ports 2>/dev/null || iptables -L INPUT -n 2>/dev/null | head -15 || echo no_firewall_cmd",
]
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(host, username=user, password=password, timeout=20)
for cmd in cmds:
    print("===", cmd[:100])
    _, o, e = c.exec_command(cmd, get_pty=True)
    print(o.read().decode("utf-8", errors="replace").rstrip())
c.close()
