# Implementation Plan: failed-events — обработка ошибок и архив в объектном хранилище

**Feature**: `002-failed-events` | **Date**: 2026-11-21 | **Spec**: [spec.md](spec.md)
**Input**: spec.md, конституция `.specify/memory/constitution.md`, код ветки `main` (приём событий, реестр устройств, управление
устройствами влиты), прототипы — [research.md](research.md).

## Summary

Сборщики (events-collector-service, device-collector-service) вместо собственных DLT публикуют `FailedEvent` в единую тему
`failed-events`: исходные байты до разбора, координаты и время исходного сообщения, код и тип ошибки из общего справочника в
схеме. Публикация синхронная, смещение исходной темы — после неё. Новый сервис `failed-events-processor` читает тему и на каждую
запись пишет один JSON-объект в MinIO условной записью `If-None-Match: *` по ключу из раздела, смещения и времени записи `failed-events`: повторная
доставка записи не даёт второго объекта. MinIO — единое хранилище платформы в базовом стенде: данные Mimir (заменяет Prometheus), Loki, Tempo и
копии баз (`backup-runner`). Сервисы платформы не меняются в части метрик: Alloy передаёт их в Mimir, Grafana читает с тем же `uid`.

## Technical Context

**Language/Version**: Java 25, Spring Boot 4.0.3 (как у device-collector-service)
**Primary Dependencies** (процессор): `spring-boot-starter-kafka`, `-actuator`, `-opentelemetry`; `micrometer-registry-prometheus`;
`kafka-avro-serializer` 8.2.4; avros 1.5.0 (с ревью PR #7; было 1.4.0); AWS SDK for Java v2 `s3` (одна версия через BOM); Jackson 3 (`byte[]` → base64 без настроек)
**Storage**: MinIO `pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, одна нода; Mimir 3.2.6, Loki 3.1.1, Tempo 2.6.0 — на нём же
**Testing**: JUnit 5, Testcontainers (Kafka 4.2, Schema Registry 8.2.4, MinIO `testcontainers-minio` 2.0.3), ArchUnit, JaCoCo ≥ 70 % строк;
`*Test` — unit, `It*Test` — интеграционные
**Target Platform**: Docker Compose платформы, 1–3 экземпляра процессора одной группы
**Project Type**: сервис в монорепозитории Gradle + изменения двух сервисов и инфраструктуры стенда
**Performance Goals**: учебный стенд; скорость процессора при всплеске замеряется один раз (R20)
**Constraints**: не терять сообщение; смещение — после записи; тайм-ауты SDK и клиента Schema Registry короче `max.poll.interval.ms`; у каждого вызова тайм-аут;
предположения research.md проверяются до кода

## Constitution Check

| Принцип | Как выполняется | Статус |
|---|---|---|
| I. Один владелец | процессор — единственный писатель бакета `failed-events`; сборщики — единственные писатели темы | ✅ |
| II. Контракты | `FailedEvent` — новая запись avros 1.4.0, `headerList` — 1.5.0 (только добавлено поле), FULL_TRANSITIVE, `default` у всех необязательных полей; DLT-subjects удалены | ✅ |
| III. Идемпотентность | ключ из координат записи `failed-events` + `If-None-Match: *`; смещение после записи | ✅ |
| IV. Без состояния, окружение | процессор без состояния; все параметры — `.env.example`; тайм-ауты S3 и Kafka заданы | ✅ |
| V. Безопасность | учётка на свой бакет/префикс; секретов в репозитории нет; описание ошибки без стека | ✅ |
| VI. Наблюдаемость | метрики без `deviceId`, JSON-логи с `traceId`, спан `s3.put-object`, дашборд процессора и MinIO | ✅ |
| VII. Тесты | IT на настоящих Kafka, Schema Registry, MinIO; покрытие ≥ 70 % | ✅ |
| Необратимые действия — человек | публикация avros, удаление DLT-тем на стенде владельца — **ЧЕЛОВЕК** | ✅ |

## Project Structure

```text
specs/002-failed-events/   spec · plan · research · data-model · quickstart · tasks · analyze-report · contracts/ · checklists/
docs/adr/0013…0019 · diagrams/{containers,component-failed-events-processor,fep-error-to-minio-sequence}.mmd

avros/avro/failed-event.avsc                    # FailedEvent, ErrorCode, ErrorType, SourceHeader (версия 1.5.0)
services/failed-events-processor/
├── Dockerfile · scripts/quick-check.sh
├── src/main/resources/failed-events-processor.yaml   # конфигурация процессора; application.yaml не создаётся (R23)
└── src/main/java/com/github/gluhov/iotplatform/failedeventsprocessor/
    ├── adapter/in/kafka/   FailedEventListener, UnreadableRecorder, KafkaErrorClassifier
    ├── adapter/out/s3/     S3FailedEventArchive
    ├── config/             KafkaConfig, S3Config, свойства, RawKeepingDeserializer, StrictUtf8StringDeserializer (как у сборщиков, R24)
    ├── domain/model        FailedRecord, ArchiveKey, UnreadableRecord, HeaderEntry · domain/port FailedEventArchive · domain/service ArchiveService
    └── support/            MetricsFacade
services/{events,device}-collector-service/…/adapter/out/kafka/FailedEventRecoverer      # вместо DeadLetterPublishingRecoverer
services/{events,device}-collector-service/…/config/{RawKeepingDeserializer,StrictUtf8StringDeserializer}
infrastructure/init/minio/init.sh · infrastructure/config/minio/check.sh · infrastructure/config/minio/{policies,ilm}/*.json
infrastructure/config/{mimir/mimir.yaml, loki, tempo, alloy, grafana/dashboards/{failed-events-processor,minio}.json}
infrastructure/config/backup-runner/{Dockerfile,entrypoint.sh,backup-postgres.sh,backup-clickhouse.sh,restore-clickhouse.sh,check.sh}
```

**Structure Decision**: пакеты процессора — как у device-collector-service (порты и адаптеры, проверка ArchUnit); у сборщиков
`FailedEventRecoverer` — исходящий адаптер Kafka. Общего модуля Gradle для двух сборщиков нет (ADR-0013): общий только контракт
в `avros`.

## Ключевые решения

| Решение | Почему | ADR |
|---|---|---|
| Единая тема `failed-events`, `FailedEvent` с байтами исходного сообщения и enum кодов | один путь ошибок; `events-dlt` и `devices-dlt` удалены | 0013 |
| Ключ объекта из раздела, смещения и времени записи `failed-events` (один для читаемой и нечитаемой записи), `If-None-Match: *` | повторная доставка записи не даёт второго объекта в обеих ветках; версионирование не плодит версий; повторная публикация сборщиком — второй объект (ограничение) | 0014 |
| `pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, бакеты и учётки по компонентам, сроки заданы | официальный образ недоступен; задание требует свой доступ и срок на бакет | 0015 |
| Mimir вместо Prometheus; Loki и Tempo в MinIO; прежний `uid` источника | дашборды и сервисы не меняются | 0016 |
| `backup-runner` с cron, копии PostgreSQL и ClickHouse в `backups/…`, восстановление из MinIO; один общий скрипт копии шарда (`shard-backup.sh`) для `make shard-backup` и backup-runner | задание; manifest со смещениями группы | 0017 |
| Нечитаемое — объект с `kind = unreadable` по общему ключу записи; MinIO недоступен — повторы и пауза; всплеск — через Kafka | не терять, не выпасть из группы, не остановить основной поток | 0018 |
| Дополнительно: возврат объекта и Object Lock; каскад не берём | Lock нельзя включить позже; каскад меняет сборщики сверх задания | 0019 |

## Ошибки

Соответствие «исключение → код ошибки, тип» — [data-model.md](data-model.md#соответствие-исключение--код-ошибки).

| Ситуация | Поведение |
|---|---|
| Публикация `FailedEvent` не удалась (тема недоступна, тайм-аут) | исключение из recoverer, смещение исходной темы не подтверждается, сообщение обработано заново |
| Запись `failed-events` не разобрана (мусор, 404 схемы, ключ не строгий UTF-8) | объект с `kind = unreadable` по ключу записи, исходные байты из `x-raw-key`, `x-raw-value`, смещение |
| Значение разобрано, но это не `FailedEvent` (корректный Avro другого типа) | слушатель: нечитаемая запись — объект `unreadable` с исходными байтами, смещение |
| В восстановитель пришла ошибка без `DeserializationException` (`ClassCastException` и другие неповторяемые по умолчанию) | объект `unreadable` с исходными байтами из `x-raw-*` |
| Schema Registry недоступен при чтении `failed-events` | повторы без объекта, смещение не подтверждается |
| Schema Registry принимает соединение и не отвечает | тайм-аут клиента Registry (`FAILED_EVENTS_SCHEMA_REGISTRY_*`) → временная ошибка, повторы без объекта, группа `Stable` |
| Первый сбой записи — таймаут Registry, затем 404 схемы | классификация перечитывается на каждом сбое (R28) → объект `unreadable`, повторы прекращаются |
| MinIO недоступен или медленный | тайм-аут вызова, повтор с растущей паузой, дальше не читает, группа `Stable` |
| `412` на запись | объект уже есть — `outcome=duplicate`, смещение |
| Другая ошибка S3 (`403`, `5xx`) | повтор, `failed.events.archive.errors{reason}`, смещение не подтверждается |

## Переменные окружения

В Compose — без значений по умолчанию; значения — в `.env.example`. Удаляются: `KAFKA_EVENTS_DLT_TOPIC`, `KAFKA_DEVICES_DLT_TOPIC`,
`DEVICES_DLT_MAX_BLOCK_MS`, `DEVICES_DLT_REQUEST_TIMEOUT_MS`, `DEVICES_DLT_DELIVERY_TIMEOUT_MS`, `MINIO_BUCKETS`, `PROMETHEUS_VERSION`, `PROMETHEUS_PORT`.

| Переменная | Пример | Кому |
|---|---|---|
| `KAFKA_FAILED_EVENTS_TOPIC` | `failed-events` | `kafka-init`, `avros-publish`, `schema-register`, оба сборщика, процессор |
| `KAFKA_FAILED_EVENTS_RETENTION_MS` | `1209600000` (14 суток) | `kafka-init` |
| `FAILED_EVENTS_PUBLISH_TIMEOUT` | `15s` (не меньше `FAILED_EVENTS_DELIVERY_TIMEOUT_MS`) | оба сборщика |
| `FAILED_EVENTS_MAX_BLOCK_MS`, `FAILED_EVENTS_REQUEST_TIMEOUT_MS`, `FAILED_EVENTS_DELIVERY_TIMEOUT_MS` | `5000`, `5000`, `15000` | оба сборщика (продюсер ошибок) |
| `FAILED_EVENTS_PROCESSOR_IMAGE_TAG`, `FAILED_EVENTS_PROCESSOR_PORTS` | `0.1.0`, `8110-8112` | процессор |
| `FAILED_EVENTS_CONSUMER_GROUP` | `failed-events-processor` | процессор, `backup-runner` не использует |
| `FAILED_EVENTS_MAX_POLL_INTERVAL_MS` | `30000` (больше `FAILED_EVENTS_S3_API_CALL_TIMEOUT`) | процессор |
| `FAILED_EVENTS_RETRY_INITIAL_INTERVAL`, `FAILED_EVENTS_RETRY_MAX_INTERVAL`, `FAILED_EVENTS_RETRY_MULTIPLIER` | `500ms`, `10s`, `2.0` | процессор |
| `FAILED_EVENTS_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS`, `FAILED_EVENTS_SCHEMA_REGISTRY_READ_TIMEOUT_MS`, `FAILED_EVENTS_SCHEMA_REGISTRY_MAX_RETRIES` | `1000`, `5000`, `0` | процессор (клиент Schema Registry: `schema.registry.http.connect.timeout.ms`, `schema.registry.http.read.timeout.ms`, `schema.registry.max.retries`; R25) |
| `FAILED_EVENTS_S3_ENDPOINT`, `FAILED_EVENTS_S3_REGION`, `FAILED_EVENTS_S3_CONNECT_TIMEOUT`, `FAILED_EVENTS_S3_API_CALL_TIMEOUT`, `FAILED_EVENTS_S3_API_CALL_ATTEMPT_TIMEOUT` | `http://minio:9000`, `us-east-1`, `2s`, `8s`, `4s` | процессор |
| `FAILED_EVENTS_BUCKET`, `FAILED_EVENTS_S3_ACCESS_KEY`, `FAILED_EVENTS_S3_SECRET_KEY` | `failed-events`, `failed-events-processor`, — | процессор, `minio-init` |
| `MINIO_IMAGE`, `MINIO_VERSION`, `MC_IMAGE`, `MC_VERSION` | `pgsty/minio`, `RELEASE.2026-08-04T00-00-00Z`, `pgsty/mc`, `RELEASE.2026-11-12T00-00-00Z` | MinIO, `backup-runner` |
| `MINIO_BUCKET_MIMIR`, `MINIO_BUCKET_LOKI`, `MINIO_BUCKET_TEMPO`, `MINIO_BUCKET_BACKUPS` | `mimir-blocks`, `loki-chunks`, `tempo-traces`, `backups` | `minio-init`, компоненты |
| `MIMIR_S3_ACCESS_KEY/SECRET_KEY`, `LOKI_S3_…`, `TEMPO_S3_…`, `BACKUP_POSTGRES_S3_…`, `BACKUP_CLICKHOUSE_S3_…` | имя учётки, `change_me_*` | `minio-init`, свой компонент |
| `MINIO_EXPIRE_FAILED_EVENTS_DAYS`, `MINIO_EXPIRE_BACKUPS_DAYS`, `MINIO_EXPIRE_MIMIR_DAYS`, `MINIO_EXPIRE_LOKI_DAYS`, `MINIO_EXPIRE_TEMPO_DAYS` | `180`, `30`, `380`, `100`, `21` | `minio-init` (ILM) |
| `MINIO_EXPIRE_FAILED_EVENTS_NONCURRENT_DAYS` | `1` (суток после того, как версия стала прежней; R27) | `minio-init` (ILM `failed-events`) |
| `MIMIR_VERSION`, `MIMIR_PORT`, `MIMIR_RETENTION_PERIOD` | `3.2.6`, `8080`, `365d` | Mimir, Alloy, Grafana |
| `LOKI_RETENTION_PERIOD`, `TEMPO_BLOCK_RETENTION` | `2160h`, `336h` | Loki, Tempo |
| `BACKUP_SCHEDULE`, `BACKUP_RUNNER_IMAGE_TAG` | `0 3 * * *`, `0.1.0` | `backup-runner` |
| `FAILED_EVENTS_LOCK_DAYS` | `180` (равен сроку ILM `MINIO_EXPIRE_FAILED_EVENTS_DAYS`) | `minio-init` (необязательная фаза) |

Ограничения: `FAILED_EVENTS_S3_API_CALL_TIMEOUT` < `FAILED_EVENTS_MAX_POLL_INTERVAL_MS`; бюджет одного цикла опроса процессора
`(FAILED_EVENTS_SCHEMA_REGISTRY_MAX_RETRIES + 1) × (FAILED_EVENTS_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS + FAILED_EVENTS_SCHEMA_REGISTRY_READ_TIMEOUT_MS)`
`+ FAILED_EVENTS_S3_API_CALL_TIMEOUT + FAILED_EVENTS_RETRY_MAX_INTERVAL` < `FAILED_EVENTS_MAX_POLL_INTERVAL_MS` (при `MAX_RETRIES = 0` ожидание между повторами
клиента Registry не возникает; с примерами: 6 + 8 + 10 = 24 с < 30 с; R25); срок бакета > срок системы (Mimir 365 < 380, Loki 90 < 100, Tempo 14 < 21).
Уже есть и используются: `KAFKA_TOPIC_PARTITIONS`, `MINIO_API_PORT`, `MINIO_CONSOLE_PORT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`,
`EVENTS_MAX_ATTEMPTS`, `DEVICES_MAX_ATTEMPTS`.

## Порядок старта

`nexus → avros-publish → schema-register`; `kafka → kafka-init` (темы, в том числе `failed-events`);
`minio (healthy) → minio-init (бакеты, учётки, ILM)`; `mimir → alloy → grafana`; `loki`, `tempo` — после `minio-init`;
`events-collector-service`, `device-collector-service`, `failed-events-processor ×1–3` — после `kafka-init`, `schema-register`, `minio-init`;
`backup-runner` — после шардов, ClickHouse, `minio-init`, `kafka`.

## Проверка

- Сборка и тесты: `./gradlew :services:failed-events-processor:check`, `:services:events-collector-service:check`, `:services:device-collector-service:check`.
- Стенд: `make up`, `make check-local`, `make dcs-quick-check`, `make ds-quick-check`, `make fep-quick-check`, `make minio-check`, `make obs-check`,
  `make backup-check` ([quickstart.md](quickstart.md)).
- Хостовые цели avros (`make avros-check`) — на macOS-хосте с его JDK.

## Интеграционные тесты сборщиков (R23)

IT сборщика (`It*Test`) поднимает Kafka, Schema Registry и MinIO (Testcontainers; MinIO — образ `pgsty/minio` через `asCompatibleSubstituteFor("minio/minio")`) и второй Spring-контекст процессора в той же JVM: `testImplementation(project(":services:failed-events-processor"))`, `SpringApplicationBuilder` с `setDefaultProperties`. Процессор читает конфигурацию `failed-events-processor.yaml` (`main` задаёт `spring.config.name=failed-events-processor`); `application.yaml` в ресурсах процессора не создаётся, чтобы на classpath не было двух `application.yaml`. Ожидается, что бины сборщика и процессора не пересекаются (разные пакеты), — это проверяет R23 (первая IT-задача P3). Сценарии: невалидное сообщение, исчерпаны попытки, нечитаемые байты — объект в MinIO с теми же байтами и `traceId`.

## Complexity Tracking

| Отступление | Зачем | Почему проще не подходит |
|---|---|---|
| Свой `FailedEventRecoverer` и обёртка-десериализатор вместо `DeadLetterPublishingRecoverer` (обёртка — и в процессоре, R24) | исходные байты и `traceId` для разобранного, но невалидного сообщения | штатный recoverer берёт байты только из `DeserializationException` (R1) |
| Две реализации `FailedEventRecoverer` (по одной на сборщик) | сервисы независимы, общий только контракт | общий модуль связал бы релизы двух сервисов (ADR-0013) |
| Третья копия `RawKeepingDeserializer` и `StrictUtf8StringDeserializer` (в процессоре) | исходные байты записи `failed-events` при любом пути в `unreadable` (ревью PR #7) | общий модуль трёх сервисов — то же возражение ADR-0013 |
| Образ MinIO — форк `pgsty` | официальный образ недоступен | свой образ или замороженный bitnami (R10) |
| Бакет `failed-events` с Object Lock с самого начала | lock нельзя включить на существующем бакете | включать позже — пересоздавать бакет с данными (R12) |
| Свой образ `backup-runner` | Kafka-инструменты, клиенты PostgreSQL, `mc` и cron в одном контейнере | три отдельных контейнера и общий том ради одного расписания (R18) |
