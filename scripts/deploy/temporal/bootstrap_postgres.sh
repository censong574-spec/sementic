#!/usr/bin/env bash
# Bootstrap standalone PostgreSQL for Temporal (idempotent).
# Config via env (usually from /opt/temporal/temporal.env):
#   POSTGRES_CUSTOM_ROOT  default /opt/postgresql-14
#   POSTGRES_PORT         default 5432
#   POSTGRES_ADMIN_SOCKET default /tmp
#   POSTGRES_SERVICE_NAME default postgresql-14-custom
set -euo pipefail

PG="${POSTGRES_CUSTOM_ROOT:-/opt/postgresql-14}"
PORT="${POSTGRES_PORT:-5432}"
SOCKET_DIR="${POSTGRES_ADMIN_SOCKET:-/tmp}"
UNIT="${POSTGRES_SERVICE_NAME:-postgresql-14-custom}"
SERVICE_UNIT="${UNIT%.service}.service"

if [[ ! -x "${PG}/bin/initdb" || ! -x "${PG}/bin/postgres" ]]; then
  echo "missing PostgreSQL binaries under ${PG}/bin" >&2
  echo "install PostgreSQL to ${PG} before running temporal deploy" >&2
  exit 1
fi

id postgres >/dev/null 2>&1 || useradd -r -s /sbin/nologin postgres
mkdir -p "${PG}/data" "${PG}/logs"
chown -R postgres:postgres "${PG}/data" "${PG}/logs"

if [[ ! -f "${PG}/data/PG_VERSION" ]]; then
  runuser -u postgres -- "${PG}/bin/initdb" -D "${PG}/data" -E UTF8 --locale=C
fi

grep -q "^port = ${PORT}" "${PG}/data/postgresql.conf" 2>/dev/null || echo "port = ${PORT}" >> "${PG}/data/postgresql.conf"
grep -q "^listen_addresses" "${PG}/data/postgresql.conf" || echo "listen_addresses = '127.0.0.1'" >> "${PG}/data/postgresql.conf"
grep -q "^unix_socket_directories" "${PG}/data/postgresql.conf" || echo "unix_socket_directories = '${SOCKET_DIR}'" >> "${PG}/data/postgresql.conf"

cat >"/etc/systemd/system/${SERVICE_UNIT}" <<EOF
[Unit]
Description=PostgreSQL (custom ${PG})
After=network.target

[Service]
Type=simple
User=postgres
Group=postgres
Environment=PGDATA=${PG}/data
ExecStart=${PG}/bin/postgres -D ${PG}/data
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_UNIT}"
systemctl restart "${SERVICE_UNIT}"
sleep 2
if ! ss -ltnp | grep -q ":${PORT} "; then
  echo "postgres not listening on 127.0.0.1:${PORT}" >&2
  exit 1
fi
echo "${SERVICE_UNIT} ready on 127.0.0.1:${PORT} (socket ${SOCKET_DIR})"
