#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for make dev-postgres" >&2
  exit 1
fi

CONTAINER_NAME="${ORCA_DEV_POSTGRES_CONTAINER:-orca-dev-postgres}"
IMAGE="${ORCA_DEV_POSTGRES_IMAGE:-postgres:16-alpine}"
HOST="${ORCA_DEV_POSTGRES_HOST:-127.0.0.1}"
PORT="${ORCA_DEV_POSTGRES_PORT:-55432}"
DB_USER="${ORCA_DEV_POSTGRES_USER:-orca}"
DB_PASSWORD="${ORCA_DEV_POSTGRES_PASSWORD:-orca}"
DB_NAME="${ORCA_DEV_POSTGRES_DB:-orca}"
CONFIG_DIR="${ORCA_CONFIG_DIR:-$HOME/.config/orca}"
CONFIG_PATH="${CONFIG_DIR}/config.toml"
DSN="postgres://${DB_USER}:${DB_PASSWORD}@${HOST}:${PORT}/${DB_NAME}?sslmode=disable"

if docker container inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
  if [ "$(docker container inspect -f '{{.State.Running}}' "${CONTAINER_NAME}")" != "true" ]; then
    docker start "${CONTAINER_NAME}" >/dev/null
  fi
else
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -e "POSTGRES_USER=${DB_USER}" \
    -e "POSTGRES_PASSWORD=${DB_PASSWORD}" \
    -e "POSTGRES_DB=${DB_NAME}" \
    -p "${PORT}:5432" \
    "${IMAGE}" >/dev/null
fi

for _ in $(seq 1 60); do
  if docker exec "${CONTAINER_NAME}" pg_isready -U "${DB_USER}" -d "${DB_NAME}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! docker exec "${CONTAINER_NAME}" pg_isready -U "${DB_USER}" -d "${DB_NAME}" >/dev/null 2>&1; then
  echo "postgres container ${CONTAINER_NAME} did not become ready" >&2
  exit 1
fi

mkdir -p "${CONFIG_DIR}"
cat >"${CONFIG_PATH}" <<EOF
[state]
dsn = "${DSN}"
EOF

echo "Postgres ready in container ${CONTAINER_NAME}."
echo "Wrote Orca state backend config to ${CONFIG_PATH}."
echo "Legacy SQLite state at ${CONFIG_DIR}/state.db will auto-migrate on the next 'orca start'."
