#!/usr/bin/env bash
# SQL через ShardingSphere-Proxy (логическая база devices) - так же, как его видит приложение.
#
#   infrastructure/config/shardingsphere/proxy-sql.sh -c "PREVIEW SELECT * FROM devices WHERE device_id = 'dev-1'"
#   infrastructure/config/shardingsphere/proxy-sql.sh -tA -c "SELECT seen_count FROM devices WHERE device_id = 'dev-1'"
#   infrastructure/config/shardingsphere/proxy-sql.sh            # интерактивный psql
#
# Клиент psql берётся из контейнера postgres-shard-0 (подключается к Proxy по сети compose), пароль Proxy -
# из .env в том виде, как его разобрал Compose; он передаётся только через окружение процесса, не в командной строке.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${COMPOSE_FILE:-${ROOT_DIR}/infrastructure/docker-compose.yaml}")
[[ -f "${ROOT_DIR}/.env" ]] || { echo "[FAIL] ${ROOT_DIR}/.env not found: cp .env.example .env" >&2; exit 1; }

env_dump="$("${COMPOSE[@]}" config --environment)"
env_value() { sed -n "s/^$1=//p" <<<"${env_dump}"; }
PGUSER="$(env_value SHARDING_PROXY_USER)"
PGPASSWORD="$(env_value SHARDING_PROXY_PASSWORD)"
export PGUSER PGPASSWORD

psql_args=(-h shardingsphere-proxy -p 3307 -d devices -v ON_ERROR_STOP=1)
if [[ $# -eq 0 ]]; then
  exec "${COMPOSE[@]}" exec -e PGUSER -e PGPASSWORD postgres-shard-0 psql "${psql_args[@]}"
fi
"${COMPOSE[@]}" exec -T -e PGUSER -e PGPASSWORD postgres-shard-0 psql "${psql_args[@]}" "$@" < /dev/null