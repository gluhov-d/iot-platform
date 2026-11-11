#!/bin/bash
# Таблица технического аудита публикаций outbox (дополнительное задание 4).
# Выполняется entrypoint-ом образа после 01-schema.sh на пустом томе clickhouse-data и одноразовым
# clickhouse-init на каждом запуске стенда (IF NOT EXISTS - повторный запуск безопасен).
set -euo pipefail

DB="${CLICKHOUSE_DB:-iot_platform}"

ch() {
  clickhouse-client --host "${CLICKHOUSE_HOST:-127.0.0.1}" \
    --user "${CLICKHOUSE_USER:-default}" --password "${CLICKHOUSE_PASSWORD:-}" "$@"
}

echo "===> ClickHouse: таблица аудита публикаций в базе '${DB}'"

ch <<SQL
-- Каждая попытка публикации outbox-записи - отдельная строка: журнал, а не текущее состояние,
-- поэтому MergeTree без схлопывания. Записи старше 30 дней удаляются по TTL.
CREATE TABLE IF NOT EXISTS \`${DB}\`.device_publication_audit
(
    event_id    String,
    device_id   String,
    outcome     Enum8('PUBLISHED' = 1, 'SKIPPED' = 2, 'FAILED' = 3, 'ERROR' = 4),
    attempt     UInt32,
    error       String,
    instance_id LowCardinality(String),
    occurred_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (device_id, occurred_at, event_id)
TTL toDateTime(occurred_at) + INTERVAL 30 DAY;
SQL