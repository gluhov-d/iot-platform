# Data Model: failed-events

Баз данных у новых компонентов нет: данные — сообщения Kafka (`FailedEvent`) и объекты MinIO. Схема Avro —
[contracts/failed-event.avsc](contracts/failed-event.avsc) (JSON в tasks.md T001 переносится дословно).

## FailedEvent (тема `failed-events`, subject `failed-events-value`, avros 1.4.0)

Ключ сообщения — строка `<sourceTopic>-<sourcePartition>-<sourceOffset>`: равномерно по трём разделам даже при шуме одного устройства и при `null`-ключе исходного
сообщения; с ключом объекта не совпадает (он строится из раздела и смещения записи `failed-events`). Порядок между ошибками не нужен — каждый объект независим.

| Поле | Тип | Минимум задания / как используется |
|---|---|---|
| `sourceService` | string | минимум; метка метрик и панелей, метаданные и поле JSON объекта |
| `sourceTopic`, `sourcePartition`, `sourceOffset` | string, int, long | минимум; ключ сообщения; поля JSON объекта; возврат в исходную тему (FR-024) |
| `sourceTimestamp` | timestamp-millis | **сверх**: время исходного сообщения, поле JSON объекта; дата ключа объекта — время записи `failed-events` |
| `failedAt` | timestamp-millis | минимум (время ошибки); поле JSON, панель задержки «ошибка → объект» |
| `errorCode` | enum `ErrorCode`, default `UNKNOWN` | минимум; сегмент ключа объекта, метка метрик; единый справочник кодов |
| `errorType` | enum `ErrorType`, default `PERMANENT` | минимум; метка метрик, разбор: постоянную чинят данные, временную — повтор |
| `description` | string ≤ 2000 | минимум; `<Класс корневой причины>: <сообщение>`, без стека |
| `traceId` | `["null","string"]` | минимум; один `traceId` от события до объекта; `null` — трассы у входящего не было |
| `key`, `value` | `["null","bytes"]` | минимум; байты исходного сообщения до разбора (с заголовком Schema Registry) |
| `headers` | map<bytes> | **сверх**: заголовки исходного сообщения (`traceparent` и прочие) без служебных заголовков Spring Kafka (`spring*`, `kafka_*`) и `x-raw-key`, `x-raw-value`; нужны для байт-в-байт возврата и сквозной трассы |
| `attempts` | int, default 1 | **сверх**: 1 — постоянная ошибка без повторов, N — исчерпаны попытки; разбор «сколько раз пробовали», панель |
| `deviceId` | `["null","string"]` | **сверх**: поиск по устройству через полгода (метаданные объекта и поле JSON); `null`, если сообщение не разобралось |

Коды ошибок (правило: новый код — сначала процессор, потом сборщики; символ добавляется в конец списка):

| Код | Тип | Когда |
|---|---|---|
| `DESERIALIZATION_FAILED` | PERMANENT | мусор, ключ не строгий UTF-8, чужой тип Avro, неизвестная схема (404) |
| `VALIDATION_FAILED` | PERMANENT | сообщение разобрано, но не проходит проверку; `UNKNOWN` операция, `CREATE`/`UPDATE` без данных устройства |
| `STORAGE_UNAVAILABLE` | TRANSIENT | хранилище (ClickHouse, шард PostgreSQL) недоступно, попытки исчерпаны |
| `PROCESSING_FAILED` | TRANSIENT | непредвиденная ошибка, попытки исчерпаны |
| `UNKNOWN` | — | значение новее схемы читателя |

## Соответствие «исключение → код ошибки»

| Сервис | Исключение / условие | Код | Тип | `attempts` |
|---|---|---|---|---|
| оба | `DeserializationException`, не транзиентная (мусор, чужая схема) | `DESERIALIZATION_FAILED` | PERMANENT | 1 |
| оба | ключ не строгий UTF-8 | `DESERIALIZATION_FAILED` | PERMANENT | 1 |
| ECS | `InvalidEventException` | `VALIDATION_FAILED` | PERMANENT | 1 |
| DCS | `InvalidDeviceInfoException`, `UNKNOWN`, `CREATE`/`UPDATE` без данных устройства | `VALIDATION_FAILED` | PERMANENT | 1 |
| ECS | `EventPersistenceException` после `EVENTS_MAX_ATTEMPTS` попыток | `STORAGE_UNAVAILABLE` | TRANSIENT | `EVENTS_MAX_ATTEMPTS` |
| DCS | `DeviceNotStoredException` после `DEVICES_MAX_ATTEMPTS` попыток | `STORAGE_UNAVAILABLE` | TRANSIENT | `DEVICES_MAX_ATTEMPTS` |
| оба | прочее после попыток | `PROCESSING_FAILED` | TRANSIENT | число попыток |
| оба | Schema Registry недоступен (транзиентная десериализация) | `FailedEvent` не создаётся: повтор без предела | — | — |

Метки `reason` прежних метрик (`deserialization`, `invalid`, `storage`, `error`) сохраняются у `devices.messages.failed` и `devices.processing.errors`.

## Объект ошибки (бакет `failed-events`)

Ключ: `<yyyy>/<MM>/<dd>/failed-events-<partition>-<offset>.json`; `partition` и `offset` — раздел и смещение записи в `failed-events`, дата — время
этой записи в UTC. Один ключ для читаемой и нечитаемой записи; при повторной доставке записи он не меняется.
Пример: `2026/11/21/failed-events-1-7.json`.

Метаданные объекта (поиск без чтения тела): `x-amz-meta-error-code`, `-source-service`, `-source-topic`, `-trace-id` (если есть), `-device-id` (если есть).

JSON-документ (все поля `FailedEvent`; время — ISO-8601 UTC; байты — base64 стандартного алфавита; отсутствие — `null`):

```json
{
  "schemaVersion": 1,
  "sourceService": "events-collector-service",
  "sourceTopic": "events",
  "sourcePartition": 2,
  "sourceOffset": 1042,
  "sourceTimestamp": "2026-11-21T10:15:30.123Z",
  "failedAt": "2026-11-21T10:15:31.480Z",
  "errorCode": "VALIDATION_FAILED",
  "errorType": "PERMANENT",
  "description": "InvalidEventException: value must be finite",
  "attempts": 1,
  "traceId": "0af7651916cd43dd8448eb211c80319c",
  "deviceId": "dev-42",
  "key": "ZGV2LTQy",
  "value": "AAAAAAEMZGV2LTQy…",
  "headers": { "traceparent": "MDAtMGFm…" },
  "archive": { "failedEventsTopic": "failed-events", "failedEventsPartition": 1, "failedEventsOffset": 7, "recordTimestamp": "2026-11-21T10:15:31.700Z", "archivedAt": "2026-11-21T10:15:31.902Z" }
}
```

Восстановление байт в байт: `base64 -d` поля `value` (и `key`, `headers`) даёт исходные байты. `archive` — служебный блок процессора: координаты записи
`failed-events` и время записи; из них строится ключ объекта.

Нечитаемая запись (тот же ключ `<yyyy>/<MM>/<dd>/failed-events-<partition>-<offset>.json`, дата — время записи в `failed-events`):

```json
{
  "schemaVersion": 1,
  "kind": "unreadable",
  "reason": "SerializationException: Unknown magic byte!",
  "failedEventsTopic": "failed-events", "failedEventsPartition": 1, "failedEventsOffset": 8,
  "recordTimestamp": "2026-11-21T10:20:00.000Z",
  "key": "…", "value": "…", "headers": { "…": "…" }
}
```

## Бакеты, учётки, сроки

| Бакет | Кто пишет | Учётка и права | Срок в бакете | Срок в системе |
|---|---|---|---|---|
| `failed-events` (lock, версии) | failed-events-processor | `Put`, `Get` на `failed-events/*`, без `ListBucket` | 180 суток | — (данные не удаляет никто, кроме ILM) |
| `mimir-blocks` | Mimir | полный доступ к бакету | 380 | 365 (`MIMIR_RETENTION_PERIOD`) |
| `loki-chunks` | Loki | полный доступ к бакету | 100 | 90 (`LOKI_RETENTION_PERIOD`) |
| `tempo-traces` | Tempo | полный доступ к бакету | 21 | 14 (`TEMPO_BLOCK_RETENTION`) |
| `backups` | backup-runner | `backup-postgres` — `backups/postgres/*`; `backup-clickhouse` — `backups/clickhouse/*` | 30 | — |

Объекты копий: `backups/postgres/<метка>/{shard_0.dump,shard_1.dump,manifest.txt}`, `backups/clickhouse/<метка>/…` (метка — UTC `ГГГГММДДTЧЧММССZ`).

## Метрики (Micrometer → Prometheus, метки без `deviceId`)

| Метрика | Метки | Кто |
|---|---|---|
| `failed.events.published` (`failed_events_published_total`) | `error_code`, `error_type` | оба сборщика |
| `failed.events.archived` (`failed_events_archived_total`) | `source_service`, `error_code`, `outcome` = `created`, `duplicate`, `unreadable` | процессор |
| `failed.events.archive.errors` (`failed_events_archive_errors_total`) | `reason` = `s3_unavailable`, `s3_denied`, `s3_error` | процессор |
| `s3.put-object` (`s3_put_object_seconds_*`) | `bucket` | процессор |
| `kafka_consumergroup_lag{consumergroup="failed-events-processor"}` | — | kafka-exporter (есть) |
