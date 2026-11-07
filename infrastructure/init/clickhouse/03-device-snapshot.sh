#!/bin/bash
# Реестр устройств: снимок устройства в событиях и outbox (contracts-avro 1.1.0, DeviceEvent.device).
# Идемпотентные ALTER ... ADD COLUMN IF NOT EXISTS: выполняется entrypoint-ом образа на пустом томе
# и одноразовым clickhouse-init на каждом запуске стенда - так изменение доходит и до уже
# инициализированного тома без ручных команд. Тот же каталог копирует в Testcontainers интеграционный тест.
set -euo pipefail

DB="${CLICKHOUSE_DB:-iot_platform}"

ch() {
  clickhouse-client --host "${CLICKHOUSE_HOST:-127.0.0.1}" \
    --user "${CLICKHOUSE_USER:-default}" --password "${CLICKHOUSE_PASSWORD:-}" "$@"
}

echo "===> ClickHouse: колонки снимка устройства в базе '${DB}'"

ch <<SQL
-- Снимок устройства в формате Avro JSON (DeviceSnapshot); '' - событие без снимка (телеметрия).
-- source_system и payload - поля DeviceEvent 1.1.0 для аудита исходного события.
ALTER TABLE \`${DB}\`.device_events
    ADD COLUMN IF NOT EXISTS device_snapshot String DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_system   String DEFAULT '',
    ADD COLUMN IF NOT EXISTS payload         Nullable(String);
-- Тот же снимок в outbox: публикатору не нужен JOIN с device_events, чтобы собрать DeviceInfo.
-- device_snapshot_schema - схема Avro, которой записан снимок: после эволюции контракта старые записи читаются
-- разрешением схем (Avro JSON без схемы писателя не прочитать). Одинакова у всех строк одной версии - LowCardinality
ALTER TABLE \`${DB}\`.device_outbox
    ADD COLUMN IF NOT EXISTS device_snapshot        String DEFAULT '',
    ADD COLUMN IF NOT EXISTS device_snapshot_schema LowCardinality(String) DEFAULT '';
SQL
