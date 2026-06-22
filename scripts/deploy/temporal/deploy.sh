#!/usr/bin/env bash
# Standalone Temporal Server for MAOS / sementic.
#
# Invoked by: python scripts/deploy/deploy.py temporal
# Config:     /opt/temporal/temporal.env (uploaded from remote.toml)
# Server:     temporal-server --root $INSTALL_ROOT --config config start
#
# persistence=postgres uses host PostgreSQL (postgresql-14-custom on :5432).
# persistence=sqlite uses embedded dev server (local only).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/temporal}"
ENV_FILE="${ENV_FILE:-$INSTALL_ROOT/temporal.env}"
SECRETS_FILE="${SECRETS_FILE:-$INSTALL_ROOT/temporal.secrets.env}"
DATA_DIR="${DATA_DIR:-$INSTALL_ROOT/data}"
LOG_DIR="${LOG_DIR:-$INSTALL_ROOT/logs}"
CONFIG_DIR="${CONFIG_DIR:-$INSTALL_ROOT/config}"
SERVER_DIR="${SERVER_DIR:-$INSTALL_ROOT/server}"

TEMPORAL_BIN="${TEMPORAL_BIN:-$INSTALL_ROOT/bin/temporal}"
TEMPORAL_SERVER_BIN="${TEMPORAL_SERVER_BIN:-$SERVER_DIR/temporal-server}"
TEMPORAL_SQL_TOOL="${TEMPORAL_SQL_TOOL:-$SERVER_DIR/temporal-sql-tool}"

TEMPORAL_PERSISTENCE="${TEMPORAL_PERSISTENCE:-postgres}"
TEMPORAL_CLI_VERSION="${TEMPORAL_CLI_VERSION:-1.3.0}"
TEMPORAL_SERVER_VERSION="${TEMPORAL_SERVER_VERSION:-1.27.2}"
GRPC_HOST="${TEMPORAL_GRPC_HOST:-127.0.0.1}"
GRPC_PORT="${TEMPORAL_GRPC_PORT:-7233}"
UI_PORT="${TEMPORAL_UI_PORT:-8233}"
DB_FILE="${TEMPORAL_DB_FILE:-$DATA_DIR/temporal.db}"
SERVICE_NAME="${TEMPORAL_SERVICE_NAME:-temporal-server}"
NAMESPACE="${TEMPORAL_NAMESPACE:-default}"

POSTGRES_HOST="${TEMPORAL_POSTGRES_HOST:-127.0.0.1}"
POSTGRES_PORT="${TEMPORAL_POSTGRES_PORT:-5432}"
POSTGRES_USER="${TEMPORAL_POSTGRES_USER:-temporal}"
POSTGRES_DB="${TEMPORAL_POSTGRES_DB:-temporal}"
POSTGRES_VISIBILITY_DB="${TEMPORAL_POSTGRES_VISIBILITY_DB:-temporal_visibility}"
POSTGRES_PASSWORD="${TEMPORAL_POSTGRES_PASSWORD:-}"

TEMPORAL_BUNDLE_FALLBACK_DIRS="${TEMPORAL_BUNDLE_FALLBACK_DIRS:-/home/liusong}"
POSTGRES_CUSTOM_ROOT="${TEMPORAL_POSTGRES_CUSTOM_ROOT:-/opt/postgresql-14}"
POSTGRES_ADMIN_SOCKET="${TEMPORAL_POSTGRES_ADMIN_SOCKET:-/tmp}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi
if [[ -f "$SECRETS_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$SECRETS_FILE"
fi
POSTGRES_PASSWORD="${TEMPORAL_POSTGRES_PASSWORD:-$POSTGRES_PASSWORD}"
TEMPORAL_BUNDLE_FALLBACK_DIRS="${TEMPORAL_BUNDLE_FALLBACK_DIRS:-/home/liusong}"
POSTGRES_CUSTOM_ROOT="${TEMPORAL_POSTGRES_CUSTOM_ROOT:-$POSTGRES_CUSTOM_ROOT}"
POSTGRES_ADMIN_SOCKET="${TEMPORAL_POSTGRES_ADMIN_SOCKET:-$POSTGRES_ADMIN_SOCKET}"

TEMPORAL_BIN="${TEMPORAL_BIN:-$INSTALL_ROOT/bin/temporal}"
TEMPORAL_SERVER_BIN="${TEMPORAL_SERVER_BIN:-$SERVER_DIR/temporal-server}"
TEMPORAL_SQL_TOOL="${TEMPORAL_SQL_TOOL:-$SERVER_DIR/temporal-sql-tool}"

step() {
  printf '\n==> %s\n' "$1"
}

require_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "run as root: sudo bash $0 $*" >&2
    exit 1
  fi
}

port_free() {
  local host="$1"
  local port="$2"
  python3 - "$host" "$port" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
sock = socket.socket()
try:
    sock.bind((host, port))
except OSError:
    sys.exit(1)
finally:
    sock.close()
PY
}

wait_grpc() {
  local attempts="${1:-45}"
  for _ in $(seq 1 "$attempts"); do
    if [[ -x "$TEMPORAL_BIN" ]] && \
      "$TEMPORAL_BIN" operator cluster health --address "${GRPC_HOST}:${GRPC_PORT}" >/dev/null 2>&1; then
      printf 'Temporal gRPC healthy at %s:%s\n' "$GRPC_HOST" "$GRPC_PORT"
      return 0
    fi
    sleep 2
  done
  echo "Temporal gRPC did not become healthy at ${GRPC_HOST}:${GRPC_PORT}" >&2
  return 1
}

ensure_postgres_password() {
  if [[ -n "$POSTGRES_PASSWORD" ]]; then
    return 0
  fi
  if [[ -f "$SECRETS_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$SECRETS_FILE"
    POSTGRES_PASSWORD="${TEMPORAL_POSTGRES_PASSWORD:-$POSTGRES_PASSWORD}"
  fi
  if [[ -n "$POSTGRES_PASSWORD" ]]; then
    return 0
  fi
  POSTGRES_PASSWORD="$(openssl rand -hex 16)"
  umask 077
  cat >"$SECRETS_FILE" <<EOF
TEMPORAL_POSTGRES_PASSWORD=$POSTGRES_PASSWORD
EOF
  chmod 0600 "$SECRETS_FILE"
  echo "Generated postgres password -> $SECRETS_FILE"
}

POSTGRES_ADMIN_SOCKET="${POSTGRES_ADMIN_SOCKET:-/tmp}"

copy_bundle_from_fallback() {
  local name="$1"
  local dest="$2"
  local dir
  IFS=':' read -ra dirs <<< "$TEMPORAL_BUNDLE_FALLBACK_DIRS"
  for dir in "${dirs[@]}"; do
    [[ -n "$dir" ]] || continue
    if [[ -f "${dir}/${name}" ]]; then
      echo "Using bundle from ${dir}/${name}"
      cp -f "${dir}/${name}" "$dest"
      return 0
    fi
  done
  return 1
}

psql_admin() {
  # Socket lives under /tmp on this host (see /tmp/.s.PGSQL.5432), not /var/run/postgresql.
  (cd /tmp && runuser -u postgres -- psql -h "$POSTGRES_ADMIN_SOCKET" -p "$POSTGRES_PORT" -v ON_ERROR_STOP=1 "$@")
}

setup_postgres() {
  step "Preparing PostgreSQL role/databases on ${POSTGRES_HOST}:${POSTGRES_PORT}"
  ensure_postgres_password
  if ! systemctl is-active --quiet postgresql-14-custom 2>/dev/null && \
     ! systemctl is-active --quiet postgresql 2>/dev/null; then
    echo "warning: postgresql systemd unit not active; continuing if port is open" >&2
  fi
  if ! psql_admin -Atc "SELECT 1" >/dev/null 2>&1; then
    echo "cannot connect to PostgreSQL via socket ${POSTGRES_ADMIN_SOCKET}:${POSTGRES_PORT}" >&2
    exit 1
  fi
  if ! psql_admin -Atc "SELECT 1 FROM pg_roles WHERE rolname='${POSTGRES_USER}'" | grep -q 1; then
    psql_admin -c "CREATE USER ${POSTGRES_USER} WITH PASSWORD '${POSTGRES_PASSWORD}';"
  else
    psql_admin -c "ALTER USER ${POSTGRES_USER} WITH PASSWORD '${POSTGRES_PASSWORD}';"
  fi
  if ! psql_admin -Atc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1; then
    psql_admin -c "CREATE DATABASE ${POSTGRES_DB} OWNER ${POSTGRES_USER};"
  fi
  if ! psql_admin -Atc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_VISIBILITY_DB}'" | grep -q 1; then
    psql_admin -c "CREATE DATABASE ${POSTGRES_VISIBILITY_DB} OWNER ${POSTGRES_USER};"
  fi
  psql_admin -c "GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DB} TO ${POSTGRES_USER};"
  psql_admin -c "GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_VISIBILITY_DB} TO ${POSTGRES_USER};"
  echo "PostgreSQL ready: ${POSTGRES_DB}, ${POSTGRES_VISIBILITY_DB} (user ${POSTGRES_USER})"
}

ensure_postgres_btree_gin() {
  local pg_root="$POSTGRES_CUSTOM_ROOT"
  local ext_dir="$pg_root/share/extension"
  if [[ -f "$ext_dir/btree_gin.control" ]]; then
    return 0
  fi
  step "Building btree_gin extension for $pg_root"
  if [[ ! -x "$pg_root/bin/pg_config" ]]; then
    echo "missing pg_config at $pg_root/bin/pg_config" >&2
    exit 1
  fi
  local pg_ver
  pg_ver="$("$pg_root/bin/pg_config" --version | awk '{print $2}')"
  local tmpdir
  tmpdir="$(mktemp -d)"
  local archive="$tmpdir/postgresql-${pg_ver}.tar.gz"
  if ! curl -sSfL --retry 3 --retry-delay 2 \
    "https://ftp.postgresql.org/pub/source/v${pg_ver}/postgresql-${pg_ver}.tar.gz" \
    -o "$archive"; then
    rm -rf "$tmpdir"
    echo "failed to download PostgreSQL ${pg_ver} source for btree_gin" >&2
    exit 1
  fi
  tar -xzf "$archive" -C "$tmpdir"
  (
    cd "$tmpdir/postgresql-${pg_ver}/contrib/btree_gin"
    make USE_PGXS=1 PG_CONFIG="$pg_root/bin/pg_config"
    make USE_PGXS=1 PG_CONFIG="$pg_root/bin/pg_config" install
  )
  rm -rf "$tmpdir"
  if [[ ! -f "$ext_dir/btree_gin.control" ]]; then
    echo "btree_gin install failed" >&2
    exit 1
  fi
  echo "btree_gin extension installed"
}

install_server_bundle() {
  step "Installing temporal-server ${TEMPORAL_SERVER_VERSION} -> $SERVER_DIR"
  mkdir -p "$SERVER_DIR" "${INSTALL_ROOT}/artifacts"
  local name="temporal_${TEMPORAL_SERVER_VERSION}_linux_amd64.tar.gz"
  local preupload="${INSTALL_ROOT}/artifacts/${name}"
  if [[ -x "$TEMPORAL_SERVER_BIN" && -x "$TEMPORAL_SQL_TOOL" ]]; then
    echo "temporal-server bundle already present"
    return 0
  fi
  rm -f "${SERVER_DIR}/temporal-server" "${SERVER_DIR}/temporal-sql-tool"
  local archive="/tmp/${name}"
  if [[ -f "$preupload" ]]; then
    echo "Using pre-uploaded bundle: $preupload"
    cp "$preupload" "$archive"
  elif copy_bundle_from_fallback "$name" "$archive"; then
    :
  else
    echo "Downloading ${name} from GitHub (may fail if host cannot reach github.com)..."
    curl -sSfL --http1.1 --retry 3 --retry-delay 2 \
      "https://github.com/temporalio/temporal/releases/download/v${TEMPORAL_SERVER_VERSION}/${name}" \
      -o "$archive"
  fi
  if [[ ! -s "$archive" ]]; then
    echo "missing temporal-server bundle: upload ${name} to ${preupload} or a dir in TEMPORAL_BUNDLE_FALLBACK_DIRS" >&2
    exit 1
  fi
  # Release tarball ships binaries only; schema is embedded in temporal-sql-tool.
  tar -xzf "$archive" -C "$SERVER_DIR" temporal-server temporal-sql-tool
  chmod +x "$TEMPORAL_SERVER_BIN" "$TEMPORAL_SQL_TOOL"
  rm -f "$archive"
}

setup_postgres_schema() {
  step "Applying Temporal SQL schema (postgres12, embedded)"
  install_server_bundle
  ensure_postgres_btree_gin
  local sql_args=(
    --ep "$POSTGRES_HOST"
    -p "$POSTGRES_PORT"
    -u "$POSTGRES_USER"
    -pw "$POSTGRES_PASSWORD"
    --plugin postgres12
  )
  "$TEMPORAL_SQL_TOOL" "${sql_args[@]}" --db "$POSTGRES_DB" create 2>/dev/null || true
  "$TEMPORAL_SQL_TOOL" "${sql_args[@]}" --db "$POSTGRES_VISIBILITY_DB" create 2>/dev/null || true
  if ! "$TEMPORAL_SQL_TOOL" "${sql_args[@]}" --db "$POSTGRES_DB" setup-schema -v 0.0 2>/dev/null; then
    echo "temporal schema already initialized for ${POSTGRES_DB}"
  fi
  "$TEMPORAL_SQL_TOOL" "${sql_args[@]}" --db "$POSTGRES_DB" update-schema \
    --schema-name postgresql/v12/temporal
  if ! "$TEMPORAL_SQL_TOOL" "${sql_args[@]}" --db "$POSTGRES_VISIBILITY_DB" setup-schema -v 0.0 2>/dev/null; then
    echo "temporal visibility schema already initialized"
  fi
  "$TEMPORAL_SQL_TOOL" "${sql_args[@]}" --db "$POSTGRES_VISIBILITY_DB" update-schema \
    --schema-name postgresql/v12/visibility
}

write_server_config() {
  step "Writing $CONFIG_DIR/development.yaml"
  mkdir -p "$CONFIG_DIR"
  local template="$SCRIPT_DIR/server.yaml.template"
  if [[ ! -f "$template" ]]; then
    echo "missing template: $template" >&2
    exit 1
  fi
  sed \
    -e "s|{{POSTGRES_HOST}}|${POSTGRES_HOST}|g" \
    -e "s|{{POSTGRES_PORT}}|${POSTGRES_PORT}|g" \
    -e "s|{{POSTGRES_USER}}|${POSTGRES_USER}|g" \
    -e "s|{{POSTGRES_PASSWORD}}|${POSTGRES_PASSWORD}|g" \
    -e "s|{{POSTGRES_DB}}|${POSTGRES_DB}|g" \
    -e "s|{{POSTGRES_VISIBILITY_DB}}|${POSTGRES_VISIBILITY_DB}|g" \
    -e "s|{{GRPC_HOST}}|${GRPC_HOST}|g" \
    -e "s|{{GRPC_PORT}}|${GRPC_PORT}|g" \
    <"$template" >"$CONFIG_DIR/development.yaml"
  chmod 0640 "$CONFIG_DIR/development.yaml"
}

write_env_file() {
  step "Writing $ENV_FILE"
  mkdir -p "$INSTALL_ROOT" "$DATA_DIR" "$LOG_DIR" "$CONFIG_DIR"
  cat >"$ENV_FILE" <<EOF
# Generated by deploy.sh
INSTALL_ROOT=$INSTALL_ROOT
TEMPORAL_PERSISTENCE=$TEMPORAL_PERSISTENCE
TEMPORAL_BIN=$TEMPORAL_BIN
TEMPORAL_SERVER_BIN=$TEMPORAL_SERVER_BIN
TEMPORAL_GRPC_HOST=$GRPC_HOST
TEMPORAL_GRPC_PORT=$GRPC_PORT
TEMPORAL_UI_PORT=$UI_PORT
TEMPORAL_DB_FILE=$DB_FILE
TEMPORAL_SERVICE_NAME=$SERVICE_NAME
TEMPORAL_NAMESPACE=$NAMESPACE
TEMPORAL_CLI_VERSION=$TEMPORAL_CLI_VERSION
TEMPORAL_SERVER_VERSION=$TEMPORAL_SERVER_VERSION
TEMPORAL_POSTGRES_HOST=$POSTGRES_HOST
TEMPORAL_POSTGRES_PORT=$POSTGRES_PORT
TEMPORAL_POSTGRES_USER=$POSTGRES_USER
TEMPORAL_POSTGRES_DB=$POSTGRES_DB
TEMPORAL_POSTGRES_VISIBILITY_DB=$POSTGRES_VISIBILITY_DB
TEMPORAL_POSTGRES_CUSTOM_ROOT=$POSTGRES_CUSTOM_ROOT
TEMPORAL_POSTGRES_ADMIN_SOCKET=$POSTGRES_ADMIN_SOCKET
TEMPORAL_BUNDLE_FALLBACK_DIRS=$TEMPORAL_BUNDLE_FALLBACK_DIRS
SECRETS_FILE=$SECRETS_FILE
EOF
  chmod 0644 "$ENV_FILE"
}

install_cli() {
  step "Installing Temporal CLI ${TEMPORAL_CLI_VERSION}"
  mkdir -p "$INSTALL_ROOT"
  if [[ -x "$TEMPORAL_BIN" ]] && "$TEMPORAL_BIN" --version 2>/dev/null | grep -q "$TEMPORAL_CLI_VERSION"; then
    echo "Temporal CLI already present: $($TEMPORAL_BIN --version)"
    return 0
  fi
  curl -sSfL "https://temporal.download/cli.sh" | sh -s -- \
    --version "v${TEMPORAL_CLI_VERSION}" \
    --dir "$INSTALL_ROOT"
  if [[ ! -x "$TEMPORAL_BIN" && -x "$INSTALL_ROOT/bin/bin/temporal" ]]; then
    TEMPORAL_BIN="$INSTALL_ROOT/bin/bin/temporal"
  fi
  if [[ ! -x "$TEMPORAL_BIN" ]]; then
    echo "Temporal CLI install failed" >&2
    exit 1
  fi
  "$TEMPORAL_BIN" --version
}

install_systemd_sqlite() {
  local unit_path="/etc/systemd/system/${SERVICE_NAME}.service"
  cat >"$unit_path" <<EOF
[Unit]
Description=Temporal Server (dev/SQLite)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=-$ENV_FILE
WorkingDirectory=$INSTALL_ROOT
ExecStart=$TEMPORAL_BIN server start-dev \\
  --db-filename $DB_FILE \\
  --ip $GRPC_HOST \\
  --port $GRPC_PORT \\
  --ui-port $UI_PORT \\
  --namespace $NAMESPACE \\
  --log-format pretty \\
  --log-level warn
Restart=always
RestartSec=5
LimitNOFILE=65536
StandardOutput=append:$LOG_DIR/temporal-server.out.log
StandardError=append:$LOG_DIR/temporal-server.err.log

[Install]
WantedBy=multi-user.target
EOF
}

install_systemd_postgres() {
  local unit_path="/etc/systemd/system/${SERVICE_NAME}.service"
  cat >"$unit_path" <<EOF
[Unit]
Description=Temporal Server (PostgreSQL persistence)
Documentation=https://docs.temporal.io
After=network-online.target postgresql-14-custom.service
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=-$ENV_FILE
EnvironmentFile=-$SECRETS_FILE
WorkingDirectory=$INSTALL_ROOT
ExecStart=$TEMPORAL_SERVER_BIN --root $INSTALL_ROOT --config config --allow-no-auth start
Restart=always
RestartSec=5
LimitNOFILE=65536
StandardOutput=append:$LOG_DIR/temporal-server.out.log
StandardError=append:$LOG_DIR/temporal-server.err.log

[Install]
WantedBy=multi-user.target
EOF
}

install_systemd() {
  step "Installing systemd unit $SERVICE_NAME (persistence=$TEMPORAL_PERSISTENCE)"
  if [[ "$TEMPORAL_PERSISTENCE" == "postgres" ]]; then
    install_systemd_postgres
  else
    install_systemd_sqlite
  fi
  systemctl daemon-reload
  systemctl enable "$SERVICE_NAME"
}

check_ports() {
  step "Checking ports"
  port_free "$GRPC_HOST" "$GRPC_PORT" || {
    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
      echo "Port $GRPC_PORT in use by $SERVICE_NAME (ok for restart)"
    else
      echo "Port $GRPC_PORT already in use" >&2
      ss -tlnp | grep ":$GRPC_PORT" || true
      exit 1
    fi
  }
}

cmd_install() {
  require_root
  install_cli
  if [[ "$TEMPORAL_PERSISTENCE" == "postgres" ]]; then
    setup_postgres
    install_server_bundle
    setup_postgres_schema
    write_server_config
  fi
  write_env_file
  check_ports
  install_systemd
  echo "Install complete. Run: $0 start"
}

cmd_start() {
  require_root
  install_cli
  if [[ "$TEMPORAL_PERSISTENCE" == "postgres" ]]; then
    setup_postgres
    install_server_bundle
    setup_postgres_schema
    write_server_config
  fi
  [[ -f "$ENV_FILE" ]] || write_env_file
  install_systemd
  step "Starting $SERVICE_NAME"
  systemctl restart "$SERVICE_NAME"
  sleep 4
  wait_grpc 45
  systemctl --no-pager --full status "$SERVICE_NAME" || true
  echo ""
  echo "Temporal gRPC:  ${GRPC_HOST}:${GRPC_PORT}"
  if [[ "$TEMPORAL_PERSISTENCE" == "postgres" ]]; then
    echo "Persistence:    PostgreSQL ${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
  else
    echo "Persistence:    SQLite ${DB_FILE}"
    echo "Temporal UI:    http://${GRPC_HOST}:${UI_PORT}"
  fi
  echo "MAOS connect:   --temporal-address ${GRPC_HOST}:${GRPC_PORT}"
}

cmd_stop() {
  require_root
  systemctl stop "$SERVICE_NAME" || true
}

cmd_restart() {
  cmd_start
}

cmd_status() {
  systemctl is-active "$SERVICE_NAME" 2>/dev/null || echo inactive
  systemctl --no-pager --full status "$SERVICE_NAME" 2>/dev/null | head -20 || true
  if [[ -x "$TEMPORAL_BIN" ]]; then
    timeout 10 "$TEMPORAL_BIN" operator cluster health --address "${GRPC_HOST}:${GRPC_PORT}" 2>/dev/null || \
      echo "gRPC health: unavailable at ${GRPC_HOST}:${GRPC_PORT}"
  fi
  ss -tlnp 2>/dev/null | grep ":${GRPC_PORT}" || true
  if [[ "$TEMPORAL_PERSISTENCE" == "postgres" ]]; then
    timeout 10 psql_admin -Atc \
      "SELECT datname FROM pg_database WHERE datname IN ('${POSTGRES_DB}','${POSTGRES_VISIBILITY_DB}') ORDER BY 1;" \
      2>/dev/null || echo "postgres temporal databases: unavailable"
  fi
}

cmd_health() {
  wait_grpc 30
}

cmd_logs() {
  journalctl -u "$SERVICE_NAME" -n "${1:-50}" --no-pager
}

usage() {
  cat <<EOF
Usage: $0 {install|start|stop|restart|status|health|logs [N]}

Persistence: $TEMPORAL_PERSISTENCE
Config: $ENV_FILE
Secrets: $SECRETS_FILE
EOF
}

main() {
  local cmd="${1:-start}"
  case "$cmd" in
    install) cmd_install ;;
    start) cmd_start ;;
    stop) cmd_stop ;;
    restart) cmd_restart ;;
    status) cmd_status ;;
    health) cmd_health ;;
    logs) cmd_logs "${2:-50}" ;;
    -h|--help|help) usage ;;
    *)
      echo "unknown command: $cmd" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
