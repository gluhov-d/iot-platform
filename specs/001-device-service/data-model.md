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
| `device` | `DeviceSnapshot`: `deviceId` и поля администратора из запроса; `status` — из запроса; `settings`, `attributes` — из запроса, поле не передано — пустая карта (`null` отклоняется проверкой тела, FR-011); остальное `null` / пусто | то же | `null` |

## Тема `devices`

`cleanup.policy=delete` + `retention.ms=${KAFKA_DEVICES_RETENTION_MS}` (2592000000 — 30 суток, не меньше интервала
копий шардов; [ADR-0011](../../docs/adr/0011-devices-topic-retention.md)). Сжатие выключено: при `compact` команда
администратора, за которой пришло событие роутера, исчезала бы с ключа, и повторное или отставшее чтение темы
оставило бы устройство активным.

Задаётся в `infrastructure/init/kafka/init-topics.sh` при создании и `kafka-configs --alter` для существующей темы;
переход с `compact` — тот же `--alter` (`cleanup.policy=delete`, `retention.ms`) плюс `--delete-config
min.compaction.lag.ms`. Сообщение без ключа тема теперь примет, но ключ `deviceId` остаётся требованием контракта:
по нему идёт маршрутизация в раздел и порядок обработки устройства.

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
- Строка 12: событие роутера `DELETE` без снимка для управляемого устройства (`managed_at` не пуст) — `IGNORED`: удаление
  такого устройства выполняет только администратор командой `DELETE` (решение человека при `converge`).
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

## Redis: кэш устройства (два ключа)

| Ключ | Содержимое | Срок жизни | Кто пишет |
|---|---|---|---|
| `device:cache:v1:{deviceId}` (hash) | `body` — JSON `DeviceResponse`; `version` — `devices.version` прочитанной строки | `DEVICE_CACHE_TTL` (минуты) | device-service при промахе |
| `device:floor:v1:{deviceId}` (string) | `floor` — наибольшая применённая версия, ниже которой кэшировать нельзя | `DEVICE_CACHE_FLOOR_TTL` (часы, больше `DEVICE_CACHE_TTL`) | device-collector-service после `COMMIT`; удаляет device-service, записав версию ≥ `floor` |

Порог вынесен из хеша, потому что в одном ключе он истекал вместе с ответом: при отставании реплики дольше
`DEVICE_CACHE_TTL` старая версия снова попадала в кэш (ADR-0003). `{deviceId}` — подстановка значения `deviceId`,
фигурных скобок (hash tag) в ключе нет: Redis работает через Sentinel, не кластер, и оба ключа устройства просто
передаются в один Lua-скрипт.

Префиксы `device:cache:` и `device:floor:` не являются началом друг друга, поэтому ключ кэша одного устройства не
совпадает с ключом порога другого ни при каком `deviceId`, допустимом по FR-004 (`:` в нём разрешено). При прежних
`device:v1:` и `device:v1:floor:` ключ кэша `floor:dev-42` совпадал с ключом порога `dev-42` (ревью PR #5). Прежние
ключи `device:v1:*` не читаются: ответы истекают через `DEVICE_CACHE_TTL`, пороги — через `DEVICE_CACHE_FLOOR_TTL`.

- `put-if-fresh.lua` (device-service; KEYS: ключ кэша, ключ порога; ARGV: body, version, ttl): отказ (0), если
  `version < floor` или `version` меньше уже лежащей (TTL ключа кэша не продлевается); иначе `HSET body version`,
  `PEXPIRE` ключа кэша, `DEL` ключа порога (реплика догнала — порог больше не нужен), 1.
- `raise-floor.lua` (device-collector-service; KEYS: ключ кэша, ключ порога; ARGV: version, floorTtl):
  `floor = max(floor, version)`, `PEXPIRE` ключа порога на `DEVICE_CACHE_FLOOR_TTL`; если лежащая в кэше
  `version < floor` — `HDEL body version`. Срок ключа кэша не меняется.
- Префикс версии `v1` (после назначения ключа) — версия формата значения: новый формат ответа пишет под новым префиксом.
- Отсутствие устройства и ошибки не кэшируются.
- Ключи прежних сервисов (`outbox:lease`, `devices:published`, `devices:fingerprint`) не меняются.
