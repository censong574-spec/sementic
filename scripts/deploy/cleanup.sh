#!/usr/bin/env bash
# Remove all sementic deploy artifacts from remote host.
# Invoked by: python scripts/deploy/deploy.py cleanup
# Config via env (set by deploy.py from remote.toml):
#   SEMENTIC_REMOTE_ROOT, TEMPORAL_ROOT, TEMPORAL_SERVICE,
#   GATEWAY_SERVICE, WORKER_SERVICE,
#   POSTGRES_CUSTOM_ROOT, POSTGRES_SERVICE, POSTGRES_PORT
set -euo pipefail

REMOTE_ROOT="${SEMENTIC_REMOTE_ROOT:-/opt/sementic}"
TEMPORAL_ROOT="${TEMPORAL_ROOT:-/opt/temporal}"
GATEWAY_SERVICE="${GATEWAY_SERVICE:-sementic-gateway}"
WORKER_SERVICE="${WORKER_SERVICE:-sementic-worker}"
TEMPORAL_SERVICE="${TEMPORAL_SERVICE:-temporal-server}"
POSTGRES_CUSTOM_ROOT="${POSTGRES_CUSTOM_ROOT:-/opt/postgresql-14}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgresql-14-custom}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
GATEWAY_PORT="${GATEWAY_PORT:-8081}"
OBSERVER_PORT="${OBSERVER_PORT:-8766}"
TEMPORAL_GRPC_PORT="${TEMPORAL_GRPC_PORT:-7233}"

require_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "run as root" >&2
    exit 1
  fi
}

step() {
  printf '==> %s\n' "$1"
}

stop_unit() {
  local unit="$1"
  systemctl stop "${unit}.service" 2>/dev/null || true
  systemctl disable "${unit}.service" 2>/dev/null || true
}

kill_port() {
  local port="$1"
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${port}/tcp" 2>/dev/null || true
  fi
}

require_root

SERVICES=(
  "$WORKER_SERVICE"
  "$GATEWAY_SERVICE"
  "$TEMPORAL_SERVICE"
  "$POSTGRES_SERVICE"
)

step "Stopping systemd services"
for svc in "${SERVICES[@]}"; do
  stop_unit "$svc"
done
sleep 1

step "Stopping stray listeners on deploy ports"
for port in "$GATEWAY_PORT" "$OBSERVER_PORT" "$TEMPORAL_GRPC_PORT" "$POSTGRES_PORT"; do
  kill_port "$port"
done

step "Removing systemd unit files"
for svc in "${SERVICES[@]}"; do
  rm -f "/etc/systemd/system/${svc}.service"
done
systemctl daemon-reload
systemctl reset-failed 2>/dev/null || true

step "Removing deploy directories"
rm -rf "$REMOTE_ROOT" "$TEMPORAL_ROOT"
rm -rf "${POSTGRES_CUSTOM_ROOT}/data" "${POSTGRES_CUSTOM_ROOT}/logs"

step "Clearing journal entries for deploy units"
for svc in "${SERVICES[@]}"; do
  journalctl --rotate 2>/dev/null || true
  journalctl --unit="${svc}.service" --vacuum-time=1s 2>/dev/null || true
done

step "Removing postgres role if unused"
if id postgres >/dev/null 2>&1; then
  if ! pgrep -u postgres >/dev/null 2>&1; then
    userdel postgres 2>/dev/null || true
    groupdel postgres 2>/dev/null || true
  else
    echo "postgres user still has running processes; kept"
  fi
fi

step "Cleanup complete"
echo "removed: $REMOTE_ROOT, $TEMPORAL_ROOT, ${POSTGRES_CUSTOM_ROOT}/{data,logs}"
echo "removed units: ${SERVICES[*]}"
