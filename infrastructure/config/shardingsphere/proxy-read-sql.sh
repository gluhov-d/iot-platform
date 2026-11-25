#!/usr/bin/env bash
# SQL через ShardingSphere-Proxy в логической базе devices_read (реплики шардов) от имени учётной записи чтения
# device-service - так же, как его видит приложение.
#
#   infrastructure/config/shardingsphere/proxy-read-sql.sh -tA -c "SELECT count(*) FROM devices WHERE device_id = 'dev-1'"
#
# Клиент psql берётся из контейнера postgres-shard-0 (подключается к Proxy по сети compose), пароль - из .env в том
# виде, как его разобрал Compose; он передаётся только через окружение процесса, не в командной строке.
# Нужна, чтобы проверять права роли чтения: запись через proxy-sql.sh (база devices) их не затрагивает.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${COMPOSE_FILE:-${ROOT_DIR}/infrastructure/docker-compose.yaml}")
[[ -f "${ROOT_DIR}/.env" ]] || { echo "[FAIL] ${ROOT_DIR}/.env not found: cp .env.example .env" >&2; exit 1; }

env_dump="$("${COMPOSE[@]}" config --environment)"
env_value() { sed -n "s/^$1=//p" <<<"${env_dump}"; }
PGUSER="$(env_value DEVICES_READ_DB_USER)"
PGPASSWORD="$(env_value DEVICES_READ_DB_PASSWORD)"
export PGUSER PGPASSWORD

psql_args=(-h shardingsphere-proxy -p 3307 -d devices_read -v ON_ERROR_STOP=1)
if [[ $# -eq 0 ]]; then
  exec "${COMPOSE[@]}" exec -e PGUSER -e PGPASSWORD postgres-shard-0 psql "${psql_args[@]}"
fi
"${COMPOSE[@]}" exec -T -e PGUSER -e PGPASSWORD postgres-shard-0 psql "${psql_args[@]}" "$@" < /dev/null
