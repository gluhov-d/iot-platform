# Data Model: device-service

Владелец всех данных — device-collector-service. device-service их только читает (реплики, Redis) и отправляет команды.

## DeviceInfo (avros 1.3.0, тема `devices`)

Новое поле — последним, остальные без изменений:

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
  "doc": "null - событие роутера (events-collector-service), применяется по eventType."
}
```

Сообщение device-service:

| Поле | CREATE | UPDATE | DELETE |
|---|---|---|---|
| `deviceId` | из запроса | из пути | из пути |
| `operation` | `CREATE` | `UPDATE` | `DELETE` |
| `eventType` (обязательное с 1.1.0) | `CREATED` | `UPDATED` | `DELETED` |
| `eventId` | UUID (= `commandId` ответа) | UUID | UUID |
| `eventTimeMs`, `emittedAtMs` | время приёма | время приёма | время приёма |
| `device` | поля администратора; поля устройства `null`/пусто | то же | `null` |

Ключ сообщения — `deviceId`; заголовок `traceparent`.

## PostgreSQL: `V3__device_management.sql` (db-migrator, все шарды)

```sql
ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS version    BIGINT NOT NULL DEFAULT 0, -- растёт при каждом APPLIED; по нему работает кэш
    ADD COLUMN IF NOT EXISTS managed_at TIMESTAMPTZ,               -- время последней команды администратора
    ADD COLUMN IF NOT EXISTS deleted_by TEXT;                      -- DEVICE (событие роутера) или ADMIN;
                                                                   -- NULL у удалённых до V3 = DEVICE
ALTER TABLE processed_device_events DROP CONSTRAINT IF EXISTS processed_device_events_outcome_check;
ALTER TABLE processed_device_events ADD CONSTRAINT processed_device_events_outcome_check
    CHECK (outcome IN ('APPLIED', 'STALE', 'IGNORED'));
```

Поля администратора (меняет только команда после первой ручной правки): `name`, `tenant_id`, `owner_id`, `device_type`,
`serial_number`, `manufacturer`, `model`, `status`, `settings`, `attributes`. Поля устройства (меняет роутер):
`firmware_version`, `os_name`, `os_version`, `sensors`, `last_seen_at`, `registered_at`.

Роль `device_reader` на основных узлах (реплицируется): `LOGIN`, `pg_read_all_data`, пароль `SHARD_READER_PASSWORD`;
создаёт `shard-replication-init`.

## ShardingSphere-Proxy

- `devices` — без изменений: основные узлы, пользователь device-collector-service.
- `devices_read` (`database-devices_read.yaml`) — источники `replica_0`, `replica_1` (`postgres-shard-N-replica`,
  пользователь `device_reader`), то же правило `shard_${(device_id.hashCode() % 2).abs()}`; пользователь Proxy
  `DEVICES_READ_DB_USER` видит только эту базу.

## Redis `device:v1:{deviceId}` (hash, TTL `DEVICE_CACHE_TTL` на ключ)

| Поле | Кто пишет | Значение |
|---|---|---|
| `body` | device-service при промахе | JSON `DeviceResponse` |
| `version` | device-service вместе с `body` | `devices.version` прочитанной строки |
| `floor` | device-collector-service после `COMMIT` | наибольшая применённая версия |

- Запись device-service (Lua): отказ, если `version < floor` или `version < текущей version`; иначе `HSET body version`, `EXPIRE`.
- Сброс device-collector-service (Lua): `floor = max(floor, v)`; если `version < v` — `HDEL body version`; `EXPIRE`.
- Префикс `v1` — версия формата: новый формат ответа читает новый префикс, старые ключи истекают по TTL.
- Отсутствие устройства и ошибки не кэшируются.
