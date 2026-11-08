#!/bin/bash
# Реестр устройств: данные устройства из снимка отдельными колонками device_events (avros 1.2.0:
# DeviceSnapshot.name, osName, osVersion; firmwareVersion есть с 1.0.0).
# Идемпотентный ALTER ... ADD COLUMN IF NOT EXISTS: выполняется entrypoint-ом образа на пустом томе
# и одноразовым clickhouse-init на каждом запуске стенда - так изменение доходит и до уже
# инициализированного тома без ручных команд. Строки, записанные раньше, получают DEFAULT ''.
# Тот же каталог копирует в Testcontainers интеграционный тест.
set -euo pipefail

DB="${CLICKHOUSE_DB:-iot_platform}"

ch() {
  clickhouse-client --host "${CLICKHOUSE_HOST:-127.0.0.1}" \
    --user "${CLICKHOUSE_USER:-default}" --password "${CLICKHOUSE_PASSWORD:-}" "$@"
}

echo "===> ClickHouse: колонки данных устройства в базе '${DB}'"

ch <<SQL
-- Те же значения лежат в device_snapshot (Avro JSON); колонки нужны для запросов и фильтров без разбора JSON.
-- '' - снимка нет или поле не задано. У ОС и прошивки мало различных значений - LowCardinality.
ALTER TABLE \`${DB}\`.device_events
    ADD COLUMN IF NOT EXISTS device_name      String DEFAULT '',
    ADD COLUMN IF NOT EXISTS os_name          LowCardinality(String) DEFAULT '',
    ADD COLUMN IF NOT EXISTS os_version       LowCardinality(String) DEFAULT '',
    ADD COLUMN IF NOT EXISTS firmware_version LowCardinality(String) DEFAULT '';
SQL