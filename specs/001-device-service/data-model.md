# Data Model: device-service

Владелец всех данных — device-collector-service. device-service их только читает (реплики, Redis) и отправляет команды.
Термины spec.md → здесь: «версия записи» — `devices.version`; «ручные изменения были» — `managed_at IS NOT NULL`;
«удалено администратором» — `deleted_at IS NOT NULL AND deleted_by = 'ADMIN'`; «нет карточки» — `tenant_id IS NULL`.

## DeviceInfo (avros 1.3.0, тема `devices`)

Новое поле — последним в `avros/avro/device-info.avsc`, остальные поля без изменений:

```json
{
  "name": "operation",
  "type": ["null", {
    "type": "enum",
    "name": "OperationType",
    "doc": "Команда владельцу данных. UNKNOWN - символ новее схемы читателя.",
    "symbols": ["CREATE", "UPDATE", "DELETE", "UNKNOWN"],
    "default": "UNKNOWN"
  }],
  "default": null,
  "doc": "null - поле не заполнено: сообщение прежней версии или событие роутера, применяется по eventType."
}
```

Сообщение device-service (ключ — `deviceId`, заголовок `traceparent`):

| Поле | CREATE | UPDATE | DELETE |
|---|---|---|---|
| `deviceId` | из тела запроса | из пути | из пути |
| `operation` | `CREATE` | `UPDATE` | `DELETE` |
| `eventType` (обязательное с 1.1.0) | `CREATED` | `UPDATED` | `DELETED` |
| `eventId` | новый UUID (= `commandId` ответа) | новый UUID | новый UUID |
| `eventTimeMs`, `emittedAtMs` | время приёма запроса | время приёма | время приёма |
| `device` | `DeviceSnapshot`: `deviceId` и поля администратора из запроса; `status` — из запроса; остальное `null` / пусто | то же | `null` |

## Тема `devices`

`cleanup.policy=compact` (как раньше) + `min.compaction.lag.ms=${KAFKA_DEVICES_MIN_COMPACTION_LAG_MS}` (604800000 —
7 суток). Задаётся в `infrastructure/init/kafka/init-topics.sh` при создании и `kafka-configs --alter` для
существующей темы.

## PostgreSQL: `V3__device_management.sql` (db-migrator, все шарды)

```sql
ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS version    BIGINT NOT NULL DEFAULT 0, -- растёт на 1 при каждом APPLIED; по нему работает кэш
    ADD COLUMN IF NOT EXISTS managed_at TIMESTAMPTZ,               -- время последней применённой команды администратора
    ADD COLUMN IF NOT EXISTS deleted_by TEXT;                      -- DEVICE (событие роутера) или ADMIN;
                                                                   -- NULL у удалённых до V3 = DEVICE
ALTER TABLE processed_device_events DROP CONSTRAINT IF EXISTS processed_device_events_outcome_check;
ALTER TABLE processed_device_events ADD CONSTRAINT processed_device_events_outcome_check
    CHECK (outcome IN ('APPLIED', 'STALE', 'IGNORED'));
-- Право чтения для device-service. Роль создаётся вне миграций (кластерный объект), в тестах её нет
DO $$ BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'device_reader') THEN
        GRANT USAGE ON SCHEMA public TO device_reader;
        GRANT SELECT ON devices TO device_reader;
    END IF;
END $$;
```

| Группа | Колонки `devices` | Кто меняет |
|---|---|---|
| Поля администратора | `name`, `tenant_id`, `owner_id`, `device_type`, `serial_number`, `manufacturer`, `model`, `status`, `settings`, `attributes` | роутер — пока `managed_at IS NULL`; после — только команды |
| Данные устройства | `firmware_version`, `os_name`, `os_version`, `sensors`, `last_seen_at`, `registered_at` | только роутер |
| Служебные | `version`, `managed_at`, `deleted_at`, `deleted_by`, `seen_count`, `last_event_*`, `last_state_at` | device-collector-service |

Правила device-collector-service (номера строк — таблица FR-015 в spec.md):

- Команда сравнивается с `managed_at` (строго раньше — `STALE`), событие роутера — с `last_state_at`, как сейчас.
- `APPLIED`: `version = version + 1`; команда ставит `managed_at = eventAt`; `DELETE` — `status = 'DELETED'`,
  `deleted_at = eventAt`, `deleted_by = 'ADMIN'`; `CREATE` — `deleted_at = NULL`, `deleted_by = NULL`.
- Надгробие (строка 5, устройства нет): `INSERT` с `status = 'DELETED'`, `seen_count = 1`, `first_seen_at`,
  `last_event_at`, `last_state_at`, `managed_at`, `deleted_at` = `eventAt`, `last_event_type = 'DELETED'`, `version = 1`.
- `IGNORED` и `STALE`: меняются только `seen_count`, `first_seen_at`, `updated_at` (как `countStale` сейчас).
- В журнал `processed_device_events` команда пишется с `event_key = eventId`, `event_type` = `eventType`.

Чтение device-service (база `devices_read`): одна строка по `device_id`; карточка есть, если
`deleted_at IS NULL AND tenant_id IS NOT NULL`, иначе «не найдено». В ответ идут поля администратора, данные
устройства и `version`; `status` вне перечисления контракта отдаётся как `UNKNOWN`.

Роль `device_reader` на основных узлах (приходит на реплики репликацией): `LOGIN`, пароль `SHARD_READER_PASSWORD`;
создаёт `infrastructure/init/shards/shard-replication-init.sh` (идемпотентно, как роль `exporter`) до запуска
db-migrator. Права — только `USAGE` на схему и `SELECT` на `devices`, их выдаёт миграция V3; членства в
`pg_read_all_data` нет: с ним Proxy не загружает метаданные таблицы (research.md R5).

## ShardingSphere-Proxy

- `devices` — без изменений: основные узлы, пользователь device-collector-service.
- `devices_read` (`infrastructure/config/shardingsphere/database-devices_read.yaml`): источники `replica_0`
  (`postgres-shard-0-replica:5432/shard_0`) и `replica_1` (`postgres-shard-1-replica:5432/shard_1`) под пользователем
  `device_reader`; таблица `devices`, алгоритм `replica_${(device_id.hashCode() % 2).abs()}`.
- `global.yaml`: второй пользователь `${DEVICES_READ_DB_USER}` с доступом только к `devices_read`; пользователю
  device-collector-service доступна только `devices`.

## Redis `device:v1:{deviceId}` (hash, TTL `DEVICE_CACHE_TTL` на ключ)

| Поле | Кто пишет | Значение |
|---|---|---|
| `body` | device-service при промахе | JSON `DeviceResponse` |
| `version` | device-service вместе с `body` | `devices.version` прочитанной строки |
| `floor` | device-collector-service после `COMMIT` | наибольшая применённая версия |

- `put-if-fresh.lua` (device-service; ARGV: body, version, ttl): отказ (0), если `version < floor` или `version` меньше
  уже лежащей; иначе `HSET body version`, `PEXPIRE`, 1.
- `raise-floor.lua` (device-collector-service; ARGV: version, ttl): `floor = max(floor, version)`; если лежащая
  `version < floor` — `HDEL body version`; `PEXPIRE`.
- Префикс `v1` — версия формата значения: новый формат ответа пишет под новым префиксом.
- Отсутствие устройства и ошибки не кэшируются.
- Ключи прежних сервисов (`outbox:lease`, `devices:published`, `devices:fingerprint`) не меняются.
