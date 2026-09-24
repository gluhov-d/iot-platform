#!/bin/bash
# Схема телеметрии ClickHouse. База берётся из CLICKHOUSE_DB (см. .env), а не
# задаётся в SQL: при другом значении таблицы создаются в выбранной базе.
# Выполняется entrypoint-ом образа один раз - на пустом томе clickhouse-data.
set -euo pipefail
: "${CLICKHOUSE_DB:?CLICKHOUSE_DB не задан}"
ch() {
  clickhouse-client --host 127.0.0.1 \
    --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" "$@"
}

echo "===> ClickHouse: схема телеметрии в базе '${CLICKHOUSE_DB}'"
ch --query "CREATE DATABASE IF NOT EXISTS \`${CLICKHOUSE_DB}\`"

# Имена таблиц без префикса базы - её задаёт --database
ch --database "${CLICKHOUSE_DB}" --multiquery <<'SQL'

CREATE TABLE IF NOT EXISTS events
(
    event_id    UUID,
    device_id   String,
    event_type  LowCardinality(String),
    metric      LowCardinality(String),
    value       Float64,
    payload     String,
    event_time  DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (device_id, event_time)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS dead_letters
(
    id           UUID,
    topic        String,
    partition_id UInt32,
    offset_id    UInt64,
    error        String,
    raw_payload  String,
    failed_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (failed_at, topic);
SQL
