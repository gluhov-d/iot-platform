#!/usr/bin/env bash
# Состояние потоковой репликации шардов (make replication-status).
# Шард: кто подключён (pg_stat_replication), слот, отставание в байтах WAL. Реплика: режим standby и состояние приёмника WAL.
# Код выхода 1, если какая-то реплика не в режиме streaming или не в режиме восстановления.
# Команду compose передаёт Makefile в COMPOSE_CMD.
set -uo pipefail

: "${COMPOSE_CMD:?запускайте через make replication-status}"
rc=0

psql_in() { # контейнер, база, запрос
  $COMPOSE_CMD exec -T "$1" sh -c 'psql -U "$POSTGRES_USER" -d "$1" -At -F " | " -c "$2"' sh "$2" "$3"
}

for n in 0 1; do
  echo "== shard_${n}: шард (postgres-shard-${n})"
  psql_in "postgres-shard-${n}" "shard_${n}" \
    "SELECT 'клиент ' || application_name || ', ' || state || ', ' || sync_state || ', отставание ' || pg_wal_lsn_diff(sent_lsn, replay_lsn) || ' байт' FROM pg_stat_replication"
  psql_in "postgres-shard-${n}" "shard_${n}" \
    "SELECT 'слот ' || slot_name || ', активен: ' || active || ', wal_status: ' || coalesce(wal_status, '-') FROM pg_replication_slots"
  echo "== shard_${n}: реплика (postgres-shard-${n}-replica)"
  recovery=$(psql_in "postgres-shard-${n}-replica" "shard_${n}" "SELECT pg_is_in_recovery()") || recovery=недоступна
  receiver=$(psql_in "postgres-shard-${n}-replica" "shard_${n}" "SELECT status FROM pg_stat_wal_receiver") || receiver=недоступна
  echo "режим восстановления (только чтение): ${recovery}, приёмник WAL: ${receiver:-нет}"
  if [ "${recovery}" != "t" ] || [ "${receiver}" != "streaming" ]; then
    rc=1
  fi
done
[ "${rc}" -eq 0 ] && echo "OK: обе реплики получают WAL" || echo "!!! репликация нарушена"
exit "${rc}"