"""Fix Kafka __consumer_offsets replication + verify worker consumption."""
from __future__ import annotations

import os
import sys
import time
import tomllib
from pathlib import Path

import paramiko

REPO = Path(__file__).resolve().parents[2]
REMOTE_TOML = REPO / "scripts" / "deploy" / "remote.toml"


def run(client: paramiko.SSHClient, cmd: str, timeout: int = 120) -> str:
    print(f"\n$ {cmd[:200]}")
    _, stdout, stderr = client.exec_command(cmd, get_pty=True, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    if out.strip():
        print(out.rstrip())
    if err.strip():
        print(err.rstrip(), file=sys.stderr)
    return out


def main() -> int:
    password = os.environ.get("DEPLOY_SSH_PASSWORD", "")
    if not password:
        print("Set DEPLOY_SSH_PASSWORD", file=sys.stderr)
        return 1

    cfg = tomllib.loads(REMOTE_TOML.read_text(encoding="utf-8"))
    host = cfg["server"]["host"]
    user = cfg["server"]["user"]
    kafka_home = "/opt/kafka"
    topic = cfg["gateway"]["kafka_topic"]

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=password, timeout=30)

    # Discover JAVA_HOME from running kafka JVM
    run(client, "readlink -f /proc/$(pgrep -f 'kafka.Kafka' | head -1)/exe 2>/dev/null || true")
    java_setup = (
        "JAVA_BIN=$(readlink -f /proc/$(pgrep -f 'kafka.Kafka' | head -1)/exe 2>/dev/null); "
        "if [ -n \"$JAVA_BIN\" ] && [ -x \"$JAVA_BIN\" ]; then "
        "export JAVA_HOME=$(dirname $(dirname \"$JAVA_BIN\")); "
        "else export JAVA_HOME=/usr/lib/jvm/java-17-openjdk 2>/dev/null; fi; "
        f"export PATH=$JAVA_HOME/bin:$PATH:{kafka_home}/bin; "
        "echo JAVA_HOME=$JAVA_HOME; java -version 2>&1 | head -2"
    )
    run(client, java_setup)

    kraft_props = f"{kafka_home}/config/kraft/server.properties"
    print("\n=== PATCH KAFKA REPLICATION FACTORS (single broker) ===")
    patch = f"""
PROP={kraft_props}
cp "$PROP" "$PROP.bak.$(date +%s)"
for kv in \\
  'offsets.topic.replication.factor=1' \\
  'transaction.state.log.replication.factor=1' \\
  'transaction.state.log.min.isr=1' \\
  'default.replication.factor=1' \\
  'min.insync.replicas=1'
do
  key=$(echo "$kv" | cut -d= -f1)
  if grep -q "^$key=" "$PROP"; then
    sed -i "s|^$key=.*|$kv|" "$PROP"
  else
    echo "$kv" >> "$PROP"
  fi
done
grep -E 'offsets.topic.replication.factor|transaction.state.log.replication.factor|default.replication.factor|min.insync.replicas|advertised.listeners' "$PROP"
"""
    run(client, patch)

    print("\n=== RESTART KAFKA ===")
    run(client, "systemctl stop kafka; sleep 3; systemctl start kafka; sleep 10; systemctl is-active kafka")

    kb = (
        "JAVA_BIN=$(readlink -f /proc/$(pgrep -f 'kafka.Kafka' | head -1)/exe 2>/dev/null); "
        "export JAVA_HOME=$(dirname $(dirname \"$JAVA_BIN\")); "
        f"export PATH=$JAVA_HOME/bin:$PATH:{kafka_home}/bin"
    )

    print("\n=== ENSURE TOPICS ===")
    run(
        client,
        f"{kb}; {kafka_home}/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list",
    )
    run(
        client,
        f"{kb}; {kafka_home}/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 "
        f"--create --if-not-exists --topic {topic} --partitions 3 --replication-factor 1",
    )
    # Ensure internal consumer offsets topic can be created
    run(
        client,
        f"{kb}; {kafka_home}/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list | grep consumer || true",
    )

    print("\n=== RESTART WORKER ===")
    run(client, "systemctl restart sementic-worker; sleep 8; systemctl is-active sementic-worker")
    run(
        client,
        "journalctl -u sementic-worker --no-pager --since '1 min ago' | "
        "grep -E 'kafka consumer ready|kafka consumer starting|kafka message processed|GroupCoordinator|ERROR' | tail -20",
    )

    print("\n=== SMOKE: produce + wait for consumer log ===")
    smoke_payload = (
        '{"event_id":"evt_smoke_fix","group_session_id":"room_smoke_test",'
        '"user_context":{"user_id":"usr_hassan_95","username":"Hassan","is_bot":false,"ownership":"OTHERS"},'
        '"message_context":{"msg_id":"post_smoke","content":"smoke test after kafka fix",'
        '"mentions_registry":[{"entity_id":"bot_project_assistant","ownership":"MY_SYSTEM"}]},'
        '"ingested_at":"smoke"}'
    )
    run(
        client,
        f"{kb}; echo '{smoke_payload}' | {kafka_home}/bin/kafka-console-producer.sh "
        f"--bootstrap-server 127.0.0.1:9092 --topic {topic} --property parse.key=true "
        f"--property key.separator=: 2>&1 <<< 'room_smoke_test:{smoke_payload}' || "
        f"echo '{smoke_payload}' | {kafka_home}/bin/kafka-console-producer.sh "
        f"--bootstrap-server 127.0.0.1:9092 --topic {topic}",
    )
    time.sleep(5)
    run(
        client,
        "journalctl -u sementic-worker --no-pager --since '30 sec ago' | "
        "grep -E 'evt_smoke_fix|smoke test|kafka message processed|task intent|ERROR' | tail -15",
    )

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
