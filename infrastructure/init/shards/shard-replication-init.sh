#!/bin/sh
# Одноразовая задача shard-replication-init (образ postgres, sh без bash).
# На каждом шарде готовит приём реплики; идемпотентна, на уже работающих шардах с данными ничего не ломает:
#   1. роль replicator (LOGIN REPLICATION) с паролем SHARD_REPLICATION_PASSWORD; пароль обновляется при каждом запуске;
#   2. физический слот репликации replica_N: шард хранит WAL, пока реплика его не получила
#      (объём ограничен max_slot_wal_keep_size в compose); слот, потерянный из-за лимита, пересоздаётся.
set -eu

: "${SHARD_DB_USER:?SHARD_DB_USER не задан}"
: "${SHARD_DB_PASSWORD:?SHARD_DB_PASSWORD не задан}"
: "${SHARD_REPLICATION_PASSWORD:?SHARD_REPLICATION_PASSWORD не задан}"

export PGPASSWORD="${SHARD_DB_PASSWORD}"

for n in 0 1; do
  host="postgres-shard-${n}"
  echo "==> ${host}: роль replicator и слот replica_${n}"
  # Пароль идёт в psql-переменную и попадает в SQL через format(%L): кавычки и спецсимволы экранируются сами
  psql -h "${host}" -U "${SHARD_DB_USER}" -d "shard_${n}" -v ON_ERROR_STOP=1 -q \
    -v repl_password="${SHARD_REPLICATION_PASSWORD}" -v slot="replica_${n}" <<'SQL'
SELECT format('CREATE ROLE replicator WITH LOGIN REPLICATION PASSWORD %L', :'repl_password')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') \gexec
SELECT format('ALTER ROLE replicator WITH LOGIN REPLICATION PASSWORD %L', :'repl_password') \gexec
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
 WHERE slot_name = :'slot' AND wal_status = 'lost' AND NOT active;
SELECT pg_create_physical_replication_slot(:'slot')
 WHERE NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name = :'slot');
SQL
done
echo "==> готово"