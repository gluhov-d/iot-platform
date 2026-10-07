#!/bin/bash
# Схема ClickHouse для events-collector-service (модуль 2): база и таблицы
# device_events и device_outbox. DDL — из задания, имя базы подставляется.
# Выполняется entrypoint-ом образа один раз - на пустом томе clickhouse-data.
#
# База — из CLICKHOUSE_DB (compose передаёт её из CLICKHOUSE_DATABASE в .env).
# Значение по умолчанию iot_platform нужно для Testcontainers: там CLICKHOUSE_DB
# не задаётся, а этот же скрипт создаёт схему в тестовом контейнере.
set -euo pipefail

DB="${CLICKHOUSE_DB:-iot_platform}"

ch() {
  clickhouse-client --host 127.0.0.1 \
    --user "${CLICKHOUSE_USER:-default}" --password "${CLICKHOUSE_PASSWORD:-}" "$@"
}

echo "===> ClickHouse: схема events-collector в базе '${DB}'"

ch <<SQL
CREATE DATABASE IF NOT EXISTS \`${DB}\`;

-- Все принятые события: одно сообщение Kafka — одна строка.
-- Повторная доставка того же события схлопывается по ключу сортировки.
CREATE TABLE IF NOT EXISTS \`${DB}\`.device_events
(
    device_id    String,
    event_id     String,
    event_date   Date,
    timestamp_ms Int64,
    event_type   LowCardinality(String),
    ingested_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY event_date
ORDER BY (device_id, event_date, timestamp_ms, event_id);

-- Журнал версий outbox: только INSERT с state_version + 1, чтение через FINAL.
CREATE TABLE IF NOT EXISTS \`${DB}\`.device_outbox
(
    event_id            String,
    device_id           String,
    source_timestamp_ms Int64,
    created_at          DateTime64(3, 'UTC'),
    updated_at          DateTime64(3, 'UTC'),
    state_version       UInt64,
    status              Enum8('NEW' = 0, 'SENT' = 1, 'FAILED' = 2),
    attempts            UInt32,
    last_error          String
)
ENGINE = ReplacingMergeTree(state_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (event_id);
SQL
