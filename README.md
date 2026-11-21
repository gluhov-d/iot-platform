# IoT Platform — events-collector-service, device-collector-service и device-service

Учебная микросервисная платформа сбора телеметрии IoT-устройств. Два сервиса:

* **events-collector-service** (ECS) — приём событий из Kafka, хранение в ClickHouse и публикация данных
  устройств в тему `devices` через outbox;
* **device-collector-service** (DCS) — реестр устройств: читает `devices` и пишет в шардированный PostgreSQL
  через ShardingSphere-Proxy (два шарда, у каждого шарда реплика только для чтения);
* **device-service** (DS) — REST-API управления устройствами: `GET/POST/PUT/DELETE /api/v1/devices`; запись — командой
  в `devices` (202 после подтверждения Kafka), чтение — из Redis и реплик шардов; доступ — сервисные токены Keycloak.

Где что искать: **порядок запуска** — раздел 10; **как проверить, на какой шард попал `deviceId`** — раздел 21;
реестр устройств — раздел 19; API устройств (device-service) — раздел 22; шарды, Proxy, миграции, реплики и резервные
копии — раздел 20; наблюдаемость и снимки экрана — раздел 16; диаграммы — раздел 17; критерии приёмки —
`docs/definition-of-done.md`.

---

## 1. Назначение events-collector-service

`events-collector-service`:

1. читает события устройств из Kafka-темы `events` (Avro `DeviceEvent` с блоком данных устройства, Schema Registry);
2. проверяет обязательные поля;
3. сохраняет каждое событие в ClickHouse (`iot_platform.device_events`), вместе со снимком устройства;
4. создаёт outbox-запись (`iot_platform.device_outbox`) — намерение опубликовать устройство;
5. подтверждает offset **только после** записи в обе таблицы;
6. фоновой задачей публикует `DeviceInfo` в тему `devices` — только если устройство новое или
   изменились его данные (отпечаток SHA-256 в Redis); один активный публикатор через Redis-аренду;
7. отдаёт метрики Prometheus, JSON-логи и трассировки OTLP.

### Гарантии

| Что | Гарантия |
|---|---|
| приём события в ClickHouse | at-least-once |
| публикация `deviceId` в `devices` | at-least-once |
| дубликаты | допустимы (гасятся `ReplacingMergeTree` и `FINAL`; потребители `devices` обязаны быть идемпотентными) |
| неизменные данные устройства | в `devices` не публикуются: уходит только новое устройство или изменившийся отпечаток |
| потеря валидного входного события | недопустима |

Общей транзакции между Kafka, ClickHouse и Redis нет, поэтому порядок шагов выбран так,
чтобы любой сбой приводил к **повтору**, а не к потере:

* `insert device_events` → `insert device_outbox` → `ack`. Падение до ack — сообщение будет прочитано
  снова, повторные строки схлопнутся по ключу сортировки.
* Начальная версия outbox — константа `state_version = 1`. Повторная доставка уже опубликованного
  события вставит `v1 NEW`, которая проиграет существующей `v2 SENT`, и состояние не откатится.
* Публикация: `send` (синхронно, `acks=all`) → одна атомарная запись в Redis (`SADD devices:published`,
  `HSET devices:snapshot-at` и `HSET devices:fingerprint` под проверкой аренды) → версия `SENT`. Падение между шагами даёт повторную отправку, но устройство не может оказаться «опубликованным»
  без сообщения в Kafka.

**Невалидные и недесериализуемые сообщения** (не Avro, схемы нет в Schema Registry, не прошли валидацию)
не повторяются: обработчик ошибок откладывает их сразу в тему `events-dlt` — недесериализуемые с исходными
байтами, невалидные как `DeviceEvent` в Avro, с заголовками `kafka_dlt-*` (исходные тема, раздел, offset, класс и
текст ошибки) — и только после успешной записи в DLT подтверждает их offset. Не удалось записать в DLT — сообщение
остаётся неподтверждённым и обрабатывается снова: раздел не идёт дальше, пока сообщение не сохранено. Лог —
со `status=REJECTED`; разбор и повторная обработка DLT — вне рамок модуля.

**Ошибка записи в ClickHouse** и непредвиденная ошибка обработки повторяются `EVENTS_MAX_ATTEMPTS` раз (по
умолчанию 3, включая первую попытку) с паузой `EVENTS_RETRY_INTERVAL` перед второй попыткой (по умолчанию 1 с;
дальше пауза растёт в `EVENTS_RETRY_MULTIPLIER` раз, по умолчанию 2: 1 с, 2 с). Не получилось — сообщение уходит
в `events-dlt`, offset подтверждается после записи в DLT, следующие сообщения раздела обрабатываются.

Остальные тайм-ауты и размеры тоже заданы переменными окружения (значения по умолчанию - в `.env.example`):

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `CLICKHOUSE_POOL_SIZE`, `CLICKHOUSE_CONNECTION_TIMEOUT_MS` | пул соединений с ClickHouse и ожидание свободного соединения, мс | `5`, `30000` |
| `REDIS_TIMEOUT` | тайм-аут команд Redis | `2s` |
| `KAFKA_SEND_TIMEOUT` | сколько публикация одного сообщения outbox ждёт подтверждения брокера | `20s` |
| `KAFKA_PRODUCER_MAX_BLOCK_MS`, `KAFKA_PRODUCER_REQUEST_TIMEOUT_MS`, `KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS` | продюсер Kafka: блокировка при отправке, ответ брокера, общий срок доставки, мс | `5000`, `5000`, `15000` |
| `OUTBOX_BATCH_SIZE`, `OUTBOX_MAX_ATTEMPTS`, `OUTBOX_LEASE_TTL_MS`, `OUTBOX_PUBLISH_INTERVAL_MS` | публикация outbox: размер пачки, попытки, срок аренды, период, мс | `100`, `10`, `30000`, `2000` |

**Schema Registry недоступен** (таймаут, обрыв соединения, ответ 5xx при десериализации) повторяется без
ограничения числа попыток с экспоненциальной паузой до 10 с: это сбой инфраструктуры, а не сообщения, и иначе в
`events-dlt` ушли бы все сообщения подряд. Что повторять, а что откладывать в DLT, решает `KafkaErrorClassifier`.

**Данные устройства и дедупликация.** `DeviceEvent` несёт блок `device` (снимок `DeviceSnapshot`: имя, ОС и её
версия, версия прошивки и остальные поля). Публикатор в `devices` считает отпечаток данных устройства —
SHA-256 от набора полей, описывающих само устройство, без `lastSeenAtMs` и показаний сенсоров, которые меняются с
каждым событием (`DeviceFingerprint`, формат `device-fingerprint-v1`). Сообщение в `devices` уходит, только если
устройство новое (`CREATED`) или отпечаток отличается от сохранённого в Redis `devices:fingerprint` (`UPDATED`);
иначе запись outbox получает статус `SENT` с исходом аудита `SKIPPED`. Новый отпечаток сохраняется только после
подтверждения Kafka, поэтому сбой отправки не «теряет» изменение.

## 2. Место сервиса в платформе

```
устройства → Device Collector → [events] → events-collector-service → ClickHouse
                                                     │
                                                     └─ outbox → [devices] → device-collector-service → ShardingSphere-Proxy
                                                                     │                                      ├─ shard_0 → реплика
                                                                     └─ [devices-dlt] ← невалидные и          └─ shard_1 → реплика
                                                                        необработанные сообщения
```

Недоставленные сообщения обоих сервисов уходят в `events-dlt` и `devices-dlt`. Схемы Avro для всех тем лежат в
проекте `avros` (раздел 6). Целевая архитектура платформы — контекстная и контейнерная диаграммы, устройство
сервисов — диаграммы компонентов и последовательностей; все в Mermaid (раздел 17).

## 3. Стек и версии

| Компонент | Версия |
|---|---|
| Java | 25 (Gradle toolchain, JDK скачивается автоматически через foojay) |
| Spring Boot | 4.0.3 (Spring Framework 7.0, Spring for Apache Kafka 4.0) |
| Apache Kafka | 4.2.0, KRaft (`apache/kafka:4.2.0`) |
| Kafka UI (профиль `tools`) | kafbat/kafka-ui v1.5.0 |
| Sonatype Nexus Repository Community Edition (базовый стенд) | 3.96.5 |
| Confluent Schema Registry | 8.2.4 |
| ClickHouse | 26.2.19 (`clickhouse/clickhouse-server:26.2.19-alpine`, на Apple M4 — `linux/amd64`, см. «Типовые проблемы»), JDBC `clickhouse-jdbc` 0.9.9 |
| Redis Open Source | 8.6.7 |
| PostgreSQL (шарды реестра устройств и их реплики) | 18.6 (`postgres:18.6-alpine`) |
| Apache ShardingSphere-Proxy | 5.5.3 (`apache/shardingsphere-proxy:5.5.3`), агент метрик — из образа |
| Flyway (db-migrator) | 12.11.0 |
| Avro | 1.12.1 |
| Lombok | 1.18.42 (версию задаёт Spring Boot) |
| Docker Compose | v2, Compose Specification (без поля `version`) |
| Prometheus / Grafana / Loki / Tempo / Alloy | v2.54.1 / 11.2.0 / 3.1.1 / 2.6.0 / v1.4.2 |
| kafka-exporter | v1.9.0 |
| postgres-exporter (шарды и реплики; платформенный экспортёр `postgres` — v0.15.0) | v0.20.1 (v0.15.0 не работает с PostgreSQL 18) |
| Testcontainers | 2.x (версию задаёт Spring Boot) |
| JaCoCo | 0.8.14 |

Ни один образ не использует тег `latest`. Все версии — в `.env.example`.

## 4. Структура репозитория

```
iot-platform/
 ├── avros/# Gradle-проект :avros -> артефакт avros (публикуется в Nexus), все Avro-схемы платформы
│   ├── Dockerfile                     # образ одноразовых задач avros-publish и schema-register
│   ├── build.gradle.kts               # генерация классов, jar со схемами, проверки совместимости (Nexus, SR), maven-publish, регистрация в SR
│   ├── scripts/
│   │   ├── container-publish.sh       # задача compose avros-publish: публикация в Nexus
│   │   ├── container-register.sh      # задача compose schema-register: регистрация схем в Schema Registry
│   │   ├── publish.sh                 # то же с хоста + тег релиза (make avros-release)
│   │   ├── check.sh                   # проверки схем без публикации: Nexus и Schema Registry (make avros-check)
│   │   ├── bootstrap.sh               # выпущенные версии из тегов avros/v* -> Nexus (make avros-bootstrap)
│   │   ├── schema-subjects.sh         # subject'ы Schema Registry (тема-value=ИмяЗаписи), общие для проверки и регистрации (source)
│   │   └── nexus-env.sh               # общая часть: Nexus, публикатор, Schema Registry и subject'ы из .env (source)
│   └── avro/                          # Avro-контракты — единый источник для кодогенерации
│       ├── device-event.avsc          # DeviceEvent (+ блок данных устройства)
│       ├── device-info.avsc           # DeviceInfo
│       └── device-snapshot.avsc       # DeviceSnapshot: имя, ОС, версия ОС, прошивка и остальные поля устройства
├── diagrams/                          # все диаграммы - Mermaid (раздел 17)
│   ├── context.mmd, containers.mmd    # контекст и контейнеры платформы
│   ├── component-events-collector.mmd, sequence-ingest.mmd, sequence-outbox-publish.mmd   # ECS
│   ├── component-device-collector.mmd, dcs-{success,duplicate,error}-sequence.mmd         # DCS
│   └── component-device-service.mmd, ds-{read,write,cache-consistency}-sequence.mmd       # DS (раздел 22)
├── docs/
│   ├── definition-of-done.md          # критерии приёмки со ссылками на файлы и тесты
│   └── screenshots/                   # снимки экрана Grafana и трейсов Tempo (раздел 16)
├── infrastructure/
│   ├── docker-compose.yaml            # стенд: базовый + профили observability, platform, tools
│   ├── .env -> ../.env                # симлинк на корневой .env (сам .env в Git не хранится)
│   ├── init/
│   │   ├── kafka/init-topics.sh       # создание тем events, devices, events-dlt, devices-dlt (контейнер kafka-init)
│   │   ├── nexus/init-nexus.sh        # пароль admin, EULA, пользователь публикации контрактов (контейнер nexus-init)
│   │   ├── clickhouse/01-schema.sh    # база iot_platform, таблицы device_events и device_outbox
│   │   ├── clickhouse/02-audit.sh     # таблица аудита публикаций device_publication_audit
│   │   ├── clickhouse/03-device-snapshot.sh, 04-device-profile.sh   # колонки снимка и данных устройства (ADD COLUMN IF NOT EXISTS)
│   │   ├── shards/shard-replication-init.sh   # роли replicator и exporter, слоты репликации (задача shard-replication-init)
│   │   ├── shards/replica-entrypoint.sh       # первый старт реплики: pg_basebackup с шарда, режим standby
│   │   └── postgres/                  # профиль platform (окружение платформы)
│   └── config/
│       ├── prometheus/prometheus.yml
│       ├── grafana/{provisioning,dashboards}   # дашборды ECS, DCS (device-collector), Proxy и шардов (device-shards)
│       ├── loki/loki-config.yaml
│       ├── tempo/tempo.yaml
│       ├── alloy/config.alloy
│       ├── shardingsphere/            # Proxy: global.yaml, database-devices.yaml (шаблоны), render-config.sh, proxy-sql.sh
│       ├── shardingsphere-agent/agent.yaml    # метрики Proxy для Prometheus
│       └── postgres/                  # pg_hba.conf шардов, replication-status.sh, shard-backup.sh, shard-restore.sh, shard-lib.sh
├── services/events-collector-service/ # модуль многомодульной Gradle-сборки
│   ├── build.gradle.kts, lombok.config
│   ├── Dockerfile                     # сборка из корня репозитория; контракты - из Nexus хоста
│   ├── scripts/check-local.sh         # быстрая проверка стенда
│   └── src/{main,test}/...
├── services/device-collector-service/ # реестр устройств (раздел 19): Dockerfile, build.gradle.kts, scripts/quick-check.sh, src/{main,test}/...
├── services/device-service/           # API устройств (раздел 22): Dockerfile, build.gradle.kts, scripts/quick-check.sh, src/{main,test}/...
├── services/device-service-client/     # клиент API: OpenAPI-контракт src/main/openapi/device-service.yaml; DTO и интерфейсы генерируются при сборке, в Git не попадают; scripts/publish.sh
├── services/db-migrator/              # Flyway: миграции db/migration/devices (V1, V2...) применяются к каждому шарду (раздел 20)
├── settings.gradle.kts, gradlew       # корень сборки: include("services:events-collector-service", "services:device-collector-service", "services:db-migrator", "avros")
├── gradle.properties                  # nexusUrl - адрес Nexus для обоих Gradle-проектов
├── Makefile
├── .env.example
└── README.md
```

**Расположение относительно задания.** Раскладка `infrastructure/` сохранена из окружения платформы, поэтому часть путей
отличается от разделов 5 и 22 задания:

| В задании | В репозитории |
|---|---|
| `infrastructure/compose.yaml` (раздел 14) | `infrastructure/docker-compose.yaml` (как в разделе 22) |
| `infrastructure/kafka/init-topics.sh` | `infrastructure/init/kafka/init-topics.sh` |
| `infrastructure/clickhouse/init/01-create-database.sql`, `02-device-events.sql`, `03-device-outbox.sql` | `infrastructure/init/clickhouse/01-schema.sh` — тот же DDL из раздела 8 одним скриптом; имя базы — из `CLICKHOUSE_DATABASE` (по умолчанию `iot_platform`); в `device_outbox` по согласованию с преподавателем добавлены `trace_id` и `span_id` для сквозного трейса |
| `infrastructure/{prometheus,grafana,loki,tempo,alloy}/...` | `infrastructure/config/{prometheus,grafana,loki,tempo,alloy}/...` |
| `infrastructure/postgres/...` | шарды, Proxy и скрипты резервного копирования — `infrastructure/init/shards/`, `infrastructure/config/{shardingsphere,postgres}/` |
| `services/events-collector-service/settings.gradle.kts`, свой `gradlew` | многомодульная сборка: `settings.gradle.kts` и `gradlew` в корне |
| `application.yml`, `application-local.yml`, `application-docker.yml` | те же профили с расширением `.yaml` |
| namespace Avro `com.proselyte.platform.events.avro` | `com.github.gluhov.iotplatform.events.avro` (поля и типы схем — как в задании) |

Пакеты events-collector-service (`com.github.gluhov.iotplatform.eventscollector`); пакеты device-collector-service — в разделе 19:

| Пакет | Содержимое |
|---|---|
| `domain.model` | `DeviceEventRecord`, `OutboxRecord` (версии и переходы), `OutboxStatus`, `OutboxPolicy`, `PublishOutcome`, `BatchResult`, `PublicationAuditRecord` |
| `domain.exception` | `InvalidEventException` (не повторяется), `EventPersistenceException` (повторяется) |
| `domain.port.in` / `out` | `IngestEventUseCase`, `PublishOutboxUseCase` / репозитории (события, outbox, аудит), аренда, учёт устройств, публикатор, метрики |
| `domain.service` | `EventIngestionService`, `OutboxPublishingService`, `DeviceEventValidator`, `DeviceEventMapper`, `DeviceFingerprint` (отпечаток данных устройства) |
| `adapter.in.kafka` | `KafkaEventListener` — принять, передать в сервис, ack |
| `adapter.in.scheduler` | `OutboxPublisherJob` |
| `adapter.out.clickhouse` | `ClickHouseEventRepository`, `ClickHouseOutboxRepository`, `ClickHousePublicationAuditRepository` |
| `adapter.out.kafka` | `KafkaDeviceInfoProducer`, `DeviceInfoPublishException` |
| `adapter.out.redis` | `RedisLeaseService`, `RedisPublishedDeviceStore` |
| `support` | `MetricsFacade` |
| `config` | `KafkaProperties`, `OutboxProperties`, `AppPropertiesConfig` (бины `Clock`, `OutboxPolicy`), `KafkaConfig` (обработчик ошибок и DLT), `KafkaErrorClassifier` (повтор или DLT), `ObservationConfig`, `SchedulingConfig` |

Доменные сервисы — бины Spring (`@Service`, `@Component`), но зависят только от портов: Kafka, ClickHouse
и Redis подключаются через адаптеры.

## 5. Kafka-темы

| Тема | Назначение | Ключ | cleanup.policy | Разделы | RF |
|---|---|---|---|---|---|
| `events` | сырой поток событий устройств | `deviceId` | `delete` | 3 | 1 |
| `devices` | `DeviceInfo`: данные устройства от events-collector-service (`operation` не задан) и команды CREATE / UPDATE / DELETE от device-service | `deviceId` | `delete` (`retention.ms` = `KAFKA_DEVICES_RETENTION_MS`, 30 суток) | 3 | 1 |
| `events-dlt` | отложенные сообщения `events`: невалидные и не записанные в ClickHouse за `EVENTS_MAX_ATTEMPTS` попыток (раздел — тот же, что у исходного) | `deviceId` | `delete` | 3 | 1 |
| `devices-dlt` | отложенные сообщения `devices`: невалидные и не записанные в PostgreSQL за `DEVICES_MAX_ATTEMPTS` попыток (device-collector-service) | `deviceId` | `delete` | 3 | 1 |

* Ключ `deviceId` обязателен: события одного устройства попадают в один раздел и читаются по порядку.
* Тема `devices` не сжимается (ADR-0011). Сжатие оставляло на ключ последнее сообщение и стирало порядок: команда
  `DELETE`, за которой пришёл снимок роутера, исчезала, и отставший или перечитывающий обработчик возвращал удалённое
  устройство. С политикой `delete` сообщения ключа лежат целиком и по порядку, пока не выйдут за срок хранения.
* `cleanup.policy=delete` и `retention.ms` темы `devices` задаёт `kafka-init` при каждом запуске
  (`kafka-configs --alter`, настройка `min.compaction.lag.ms` снимается), поэтому и на уже созданной теме политика
  исправляется. Копия шардов старше срока хранения — предупреждение `shard-restore.sh`.
* Смещения группы device-collector-service — без тихого сброса (`auto-offset-reset: none`, ADR-0011). Раздел без
  смещения (новая группа, новый раздел) читается с начала темы. Раздел, добавленный к теме у работающей группы
  (`kafka-topics --alter --partitions`), сервис начинает читать с начала сам, без перезапуска и при любом числе
  экземпляров: `Consumer` обёрнут (`NoOffsetRecoveringConsumer`), на `NoOffsetForPartitionException` в `position()` и
  `poll()` делается `seekToBeginning` для этих разделов (ADR-0011, R18). Новый раздел потребитель замечает при
  обновлении метаданных — не позже `DEVICES_CONSUMER_METADATA_MAX_AGE_MS`. Если смещение группы вне диапазона темы — обработчик
  отстал дольше срока хранения или группе вернули смещение копии старше срока, — часть сообщений удалена до обработки:
  обработчик останавливается, в логе ERROR `Committed offset is out of range`, health сервиса `DOWN` (контейнер
  unhealthy). Health `DOWN` и при остановке обработчика `devices` по любой другой причине. С `earliest` он молча перешёл бы на самое раннее сохранённое сообщение и, например, применил бы событие
  роутера после потерянного `DELETE` администратора. Выход: восстановить шарды из свежей копии (`make shard-restore`
  выставит смещения группы из её manifest) либо осознанно сдвинуть смещение группы
  (`kafka-consumer-groups --reset-offsets`) и перезапустить сервис.
* Хранение не заменяет идемпотентность потребителей: повторная доставка и повторное чтение темы дают тот же результат.
* Темы создаёт одноразовый контейнер `kafka-init` скриптом `infrastructure/init/kafka/init-topics.sh`
  после `kafka: service_healthy`; оба сервиса стартуют только после `kafka-init: service_completed_successfully`.
  Имена тем — переменные `KAFKA_EVENTS_TOPIC`, `KAFKA_DEVICES_TOPIC`, `KAFKA_EVENTS_DLT_TOPIC`, `KAFKA_DEVICES_DLT_TOPIC`.
  Автосоздание тем брокером выключено (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`).

## 6. Avro-схемы (avros)

| Файл | Запись | Поля |
|---|---|---|
| `avros/avro/device-event.avsc` | `DeviceEvent` | `eventId`, `deviceId`, `timestampMs` (Unix ms), `eventType`; необязательные, `default: null`: `device` (`DeviceSnapshot`), `sourceSystem`, `payload` |
| `avros/avro/device-info.avsc` | `DeviceInfo` | `deviceId`, `emittedAtMs` (время публикации); `eventType` (`DeviceUpsertEventType`, `default: SNAPSHOT`); необязательные: `eventId`, `eventTimeMs`, `device` (`DeviceSnapshot`), `operation` (`OperationType`, с 1.3.0; `null` по умолчанию) |
| `avros/avro/device-snapshot.avsc` | `DeviceSnapshot` | данные устройства: `deviceId`, `tenantId`, `deviceType`, `status` (`DeviceStatus`), `settings`, `sensors`, `attributes`; необязательные: `ownerId`, `serialNumber`, `manufacturer`, `model`, `firmwareVersion`, `name`, `osName`, `osVersion`, `registeredAtMs`, `lastSeenAtMs` |

Версии: `1.0.0` — схемы приёма событий; `1.1.0` — снимок устройства (`DeviceSnapshot`, блок `device` в `DeviceEvent`
и `DeviceInfo`); `1.2.0` — имя, операционная система и её версия (`name`, `osName`, `osVersion`); `1.3.0` — `operation` в `DeviceInfo`
(`CREATE` / `UPDATE` / `DELETE` от device-service; `null` у событий роутера). Новые поля
необязательные и со значением по умолчанию, существующие не удалялись и не переименовывались: сообщение предыдущей
версии читается новой схемой и наоборот (в сервисах это проверяет `ContractCompatibilityTest`).

Namespace — `com.github.gluhov.iotplatform.events.avro`. Классы `SpecificRecord` сервис получает зависимостью
`com.github.gluhov.iotplatform:avros:<avrosVersion>` из Nexus (`avrosVersion` — в
`services/*/build.gradle.kts`, сейчас `1.3.0` во всех трёх сервисах — явная версия, без `+` и диапазонов); сервис классы
не генерирует.
`stringType = String`: иначе поля были бы `CharSequence`, при чтении приходил бы `org.apache.avro.util.Utf8`
и `equals` со строкой ломался бы.

Группа `com.github.gluhov.iotplatform` подключена через `exclusiveContent`: её Gradle ищет только в Nexus
(`/repository/maven-public/`), а остальные зависимости — только в Maven Central и Confluent, поэтому контракт
нельзя подменить одноимённым артефактом из публичного репозитория. Адрес Nexus — свойство `nexusUrl`
в корневом `gradle.properties` (`http://localhost:8091`); при другом `NEXUS_PORT` — `-PnexusUrl=…` или
`ORG_GRADLE_PROJECT_nexusUrl`. Для первой загрузки контрактов Nexus должен быть поднят (`make nexus-up`),
дальше они лежат в кеше Gradle.

Схемы собирает отдельный Gradle-проект `:avros` — библиотека
`com.github.gluhov.iotplatform:avros` (версия в `avros/build.gradle.kts`, сейчас `1.3.0`):

* классы генерируются одним `SchemaParser` на все файлы — схема может ссылаться на тип из другого файла
  по имени, а повторное объявление одного полного имени (например, двух `DeviceInfo`) роняет сборку;
* в jar рядом с классами лежат сами схемы (`avro/*.avsc`) — по ним проверяется совместимость новой версии;
* Avro (`org.apache.avro:avro`) подключён как `api`: потребитель получает его транзитивно;
* `publish` выкладывает jar, sources, pom и Gradle module metadata в Nexus:
  релизы — в `maven-releases`, версии `-SNAPSHOT` — в `maven-snapshots`. Адрес — `-PnexusUrl`
  (по умолчанию `http://localhost:8091`), учётная запись — `ORG_GRADLE_PROJECT_nexusUsername` и
  `ORG_GRADLE_PROJECT_nexusPassword`; в репозитории их нет. Выпущенную версию Nexus перезаписать не даст.

```bash
./gradlew :avros:build    # build/libs/avros-<версия>.jar и -sources.jar; Nexus не нужен
```

#### Конвейер публикации

```bash
make nexus-up              # только Nexus (make up поднимает его сам)
make registry-up       # Kafka и Schema Registry: без неё публикация в Nexus не выполняется (ниже её поднимают сами цели avros-*)
make avros-check       # разбор схем + совместимость с Nexus и со Schema Registry (без публикации)
make avros-release     # проверки -> сборка -> публикация (avros/scripts/publish.sh)
make avros-up          # собрать и опубликовать текущую версию в Nexus задачей compose (make up вызывает сам)
make avros-bootstrap   # выпущенные версии из git-тегов, которых нет в Nexus (если сервис закреплён на старой версии)
```

`avros-release` берёт порт Nexus и учётную запись публикатора (`NEXUS_PUBLISH_USER`/`NEXUS_PUBLISH_PASSWORD`),
порт Schema Registry и имена тем из `.env` в том виде, как их разбирает Compose, проверяет, что Nexus принимает запись
и Schema Registry отвечает, и запускает `:avros:build :avros:publish`. Пароль передаётся Gradle через окружение —
в командной строке и выводе его нет. Обе проверки — `checkAvrosCompatibility` (Nexus) и `checkRegisteredSchemas`
(Schema Registry, ниже) — встроены в `publish` как зависимости, поэтому прямой `./gradlew :avros:publish` их тоже
не обходит: обе завершаются **до** загрузки артефакта в Nexus. Проверка по Nexus:

1. разбирает все `avros/avro/*.avsc` одним парсером;
2. находит последнюю выпущенную версию в `maven-releases` (`maven-metadata.xml`); если её нет — первая публикация,
   сравнивать не с чем;
3. отклоняет версию, которая уже выпущена или не больше последней (`-SNAPSHOT` тоже должен быть больше);
4. сверяет каждую запись со схемами `avro/*.avsc` из jar последней версии (`org.apache.avro.SchemaCompatibility`).

Уровень — `-PcompatibilityLevel`, по умолчанию `FULL`: новая схема читает данные старой и наоборот. Сервисы
переходят на новую версию контрактов независимо друг от друга, поэтому нужны оба направления; `BACKWARD` —
только то, что проверяет Schema Registry. Совместимые изменения — новое поле с `default` (например,
`["null", "string"]` с `null`), удаление поля, у которого был `default`. Удалённая схема или несовместимое
изменение — ошибка с перечнем полей; пропускается оно, только если поднята старшая версия (с предупреждением).

**Проверка по Schema Registry** (`checkRegisteredSchemas`, обязательная часть публикации). Проверка по Nexus смотрит
только на последнюю выпущенную версию, а история Nexus и Schema Registry может расходиться: несовместимая версия
попала бы в `maven-releases` (выпущенную версию не исправить), и только потом `schema-register` отказался бы её
регистрировать и остановил запуск сервисов. Поэтому до загрузки в Nexus текущие схемы (`SCHEMA$` сгенерированных
классов, как их регистрирует `registerSchemas`) сверяются со **всеми** версиями, зарегистрированными в subject
платформы, в обе стороны (`FULL`; для Schema Registry это `FULL_TRANSITIVE`, уровень — тот же `-PcompatibilityLevel`):

* subject берутся из `-PschemaSubjects` (`тема-value=ИмяЗаписи`; `avros/scripts/schema-subjects.sh`) — те же четыре,
  что регистрирует `schema-register`; адрес — `-PschemaRegistryUrl`. Скрипты и compose передают их сами;
* subject ещё не зарегистрирован (чистая Schema Registry) — сравнивать не с чем, проверка проходит;
* нет `-PschemaRegistryUrl`, Schema Registry недоступна или схема несовместима — ошибка **до** загрузки в Nexus
  (в перечне — subject, версия, направление и поле); исключения для старшей версии нет: такую схему
  Schema Registry в тот же subject всё равно не примет;
* `make avros-check` выполняет обе проверки без публикации.

| Изменение | Версия |
|---|---|
| совместимое изменение схем | следующая минорная: `1.2.0` → `1.3.0` |
| несовместимое изменение | следующая старшая: `1.x` → `2.0.0`; для Schema Registry это новый субъект/тема |
| проба до релиза | `X.Y.Z-SNAPSHOT` — в `maven-snapshots`, можно публиковать повторно |

**Релиз = коммит + тег + артефакт.** Версия без `-SNAPSHOT` собирается только из закоммиченного `avros/`
(иначе — «незакоммиченные изменения»), после публикации `avros-release` ставит на `HEAD` аннотированный тег
`avros/vX.Y.Z`; если тег уже есть — версия выпущена, нужно поднять `version`. Тег отправляется вместе
с веткой: `git push origin avros/vX.Y.Z`. `-SNAPSHOT` публикуется из рабочей папки и без тега.

История релизов хранится в git, Nexus — только хранилище артефактов. `make up` публикует текущую версию
(`make avros-up`), поэтому на чистом клоне и после `make clean` Nexus снова получает ту версию, от которой зависят
сервисы. Если сервис закреплён на более старой версии, `make avros-bootstrap` проходит по тегам `avros/v*` по
возрастанию версий и каждую версию, которой нет в `maven-releases`, собирает во временном `git worktree` на её теге и
публикует — своим `build.gradle.kts`, на котором поставлен тег (параметры Schema Registry передаются, но теги,
созданные до проверки `checkRegisteredSchemas`, её не знают: это восстановление уже выпущенных версий). Скрипту
нужна работающая Schema Registry (`make registry-up` выполняется сам). Уже опубликованные версии пропускаются; тег, в
котором `version` не совпадает с именем тега, — ошибка.

После релиза `make avros-check` на той же версии сообщает «Версия … уже опубликована»: перед изменением схем поднимите
`version` в `avros/build.gradle.kts`.

#### Публикация и регистрация в составе `make up`

Это две одноразовые задачи compose из образа `avros/Dockerfile` (порядок — раздел 10):

1. `avros-publish` (`avros/scripts/container-publish.sh`) — публикует текущую версию в Nexus. Ждёт `nexus-init` и
   `schema-registry: service_healthy` (Kafka и Schema Registry поднимаются до публикации). Идемпотентна: версия
   уже в Nexus и схемы совпадают — ничего не делает; версия есть, а схемы другие — ошибка (выпущенную версию не
   перезаписать, поднимите `version`); версии нет — проверка совместимости (`FULL`) с последней выпущенной версией
   в Nexus и со всеми схемами в Schema Registry, сборка, публикация. Не прошла любая проверка или Schema Registry
   не отвечает — код 1, в Nexus ничего не загружено.
2. `schema-register` (`avros/scripts/container-register.sh`) — после публикации регистрирует схемы в Schema Registry.
   Режим каждого субъекта задаётся явно — `FULL_TRANSITIVE` (`SCHEMA_COMPATIBILITY`), поэтому Schema Registry
   проверяет новую схему на совместимость **со всеми** зарегистрированными версиями; несовместимая схема — ошибка
   и код 1, дальше стенд не поднимается. Субъекты (стратегия `TopicNameStrategy`): `events-value` и
   `events-dlt-value` — `DeviceEvent`; `devices-value` и `devices-dlt-value` — `DeviceInfo`. Повторный запуск ничего
   не меняет.

Продюсеры обоих сервисов работают с `auto.register.schemas=false`: схему они только находят в Schema Registry, а
незарегистрированная схема — ошибка сериализации, а не молчаливая новая версия. В интеграционных тестах
автоматическая регистрация включена (на стенде её нет). Сериализация —
`KafkaAvroSerializer`/`KafkaAvroDeserializer` (Schema Registry, `specific.avro.reader=true`). Выполнить регистрацию
вручную — `make schemas-register`, публикацию — `make avros-up` (нужен `make nexus-up`).

## 7. Таблицы ClickHouse

База `iot_platform`, DDL — `infrastructure/init/clickhouse/01-schema.sh` и `02-audit.sh` (выполняются по порядку
и только на **пустом** томе).

**`device_events`** — `ReplacingMergeTree(ingested_at)`, `PARTITION BY event_date`,
`ORDER BY (device_id, event_date, timestamp_ms, event_id)`. Одно Kafka-сообщение — одна строка;
повтор того же события имеет тот же ключ сортировки и схлопывается. `event_date` вычисляется в UTC.

**`device_outbox`** — `ReplacingMergeTree(state_version)`, `ORDER BY (event_id)`. Журнал версий:
каждая смена статуса — `INSERT` новой строки с `state_version + 1`, `ALTER TABLE ... UPDATE` не используется.
Текущее состояние читается через `FINAL`. Колонки `trace_id` и `span_id` (`DEFAULT ''`) — расширение DDL задания:
спан приёма события, который продолжает публикация (раздел 16); все версии записи несут одно и то же значение.

```sql
SELECT event_id, device_id, toString(status) AS status, state_version, attempts, last_error
FROM iot_platform.device_outbox FINAL
WHERE event_id = 'evt-1';
```

| Переход | Версия | Статус | attempts |
|---|---|---|---|
| приём события | 1 | NEW | 0 |
| устройство уже опубликовано | v+1 | SENT | без изменений |
| публикация успешна | v+1 | SENT | без изменений |
| ошибка отправки | v+1 | FAILED | +1, `last_error` |

Записи с `attempts >= OUTBOX_MAX_ATTEMPTS` больше не выбираются (остаются FAILED; видны в логах и метриках).

**Данные устройства** добавлены в существующие таблицы обратно совместимыми миграциями
(`infrastructure/init/clickhouse/03-device-snapshot.sh`, `04-device-profile.sh`): только `ADD COLUMN IF NOT EXISTS`
со значениями по умолчанию, поэтому повторный запуск ничего не меняет, а строки, записанные раньше, читаются с
`DEFAULT ''`. В `device_events`: `device_snapshot` (снимок в Avro JSON), `source_system`, `payload`, а также отдельные
колонки `device_name`, `os_name`, `os_version`, `firmware_version` — для запросов без разбора JSON. В `device_outbox`:
`device_snapshot` и `device_snapshot_schema` (схема Avro, которой записан снимок). Скрипты выполняет entrypoint
образа на пустом томе и одноразовая задача `clickhouse-init` при каждом запуске стенда, поэтому на уже
инициализированный том изменения доходят без ручных команд.

```sql
SELECT event_id, device_id, device_name, os_name, os_version, firmware_version
FROM iot_platform.device_events FINAL
WHERE device_id = 'dev-1' ORDER BY timestamp_ms DESC LIMIT 5;
```

На стенде, поднятом до появления колонок трассировки, их нужно добавить один раз (данные сохраняются; записи,
принятые раньше, публикуются в отдельном трейсе, как до изменения):

```bash
docker compose -f infrastructure/docker-compose.yaml exec -T clickhouse sh -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DB"' <<'SQL'
ALTER TABLE device_outbox
    ADD COLUMN IF NOT EXISTS trace_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS span_id String DEFAULT ''
SQL
```

**`device_publication_audit`** — технический аудит публикаций: `MergeTree`, `PARTITION BY toYYYYMM(occurred_at)`,
`ORDER BY (device_id, occurred_at, event_id)`, `TTL` 30 дней. Каждая попытка публикации outbox-записи — отдельная
строка: исход `outcome` (`PUBLISHED`/`SKIPPED`/`FAILED`/`ERROR`), номер попытки `attempt` (`attempts + 1`), текст
ошибки, `instance_id` экземпляра и время. Строка пишется после смены статуса в `device_outbox`; если ClickHouse её
не принял, публикация всё равно считается выполненной, в логе — «Publication audit record not written».

```sql
SELECT occurred_at, toString(outcome) AS outcome, attempt, instance_id, error
FROM iot_platform.device_publication_audit
WHERE event_id = 'evt-1'
ORDER BY occurred_at;
```

На стенде, поднятом до появления таблицы, `02-audit.sh` не выполнялся (init-скрипты работают только на пустом
томе), а healthcheck ClickHouse ждёт именно `device_publication_audit`. Создать таблицу без потери данных:

```bash
docker compose -f infrastructure/docker-compose.yaml exec clickhouse bash /docker-entrypoint-initdb.d/02-audit.sh
```

## 8. Redis-ключи

| Ключ | Тип | Назначение |
|---|---|---|
| `outbox:lease` | String | аренда публикатора: значение — случайный `ownerToken` (UUID) |
| `devices:published` | Set | `deviceId`, для которых `DeviceInfo` уже отправлен в Kafka |
| `devices:snapshot-at` | Hash | `deviceId` → время исходного события последнего опубликованного снимка (мс Unix) |
| `devices:fingerprint` | Hash | `deviceId` → отпечаток опубликованных данных устройства (SHA-256, 64 символа hex) |
| `device:cache:v1:{deviceId}` | Hash | кэш карточки устройства для device-service: `body`, `version`; TTL — `DEVICE_CACHE_TTL` (раздел 22) |
| `device:floor:v1:{deviceId}` | String | нижняя граница версии для записи в кэш: пишет device-collector-service после `COMMIT`, удаляет device-service, записав версию не ниже порога; TTL — `DEVICE_CACHE_FLOOR_TTL` (больше `DEVICE_CACHE_TTL`, ADR-0003) |

Redis работает через Sentinel (ADR-0008): адрес основного узла клиенты получают у Sentinel (`REDIS_SENTINEL_MASTER`,
`REDIS_SENTINEL_NODES`). Ключи `device:cache:v1:*` и `device:floor:v1:*` пишут device-service и device-collector-service; ключи аренды и отметки публикации — только events-collector-service.

Префиксы `device:cache:` и `device:floor:` не являются началом друг друга: `deviceId` допускает `:` (FR-004), и при прежних
`device:v1:` и `device:v1:floor:` ключ кэша устройства `floor:dev-42` совпадал с ключом порога `dev-42` — сброс кэша
`dev-42` падал с `WRONGTYPE`, старая карточка отдавалась до истечения TTL. Переход со старых ключей: `device:v1:*` больше
не читаются и истекают сами (ответы — через `DEVICE_CACHE_TTL`, пороги — через `DEVICE_CACHE_FLOOR_TTL`); при желании их
можно удалить в `make redis-cli`: `--scan --pattern 'device:v1:*'` и `DEL` (по желанию, задача T133).

* Захват: `SET outbox:lease <ownerToken> NX PX <OUTBOX_LEASE_TTL_MS>`. Не получили — итерация пропускается.
* Продление — Lua-скрипт: `PEXPIRE outbox:lease <OUTBOX_LEASE_TTL_MS>`, только если значение совпадает с токеном.
  Аренду удерживает `LeaseGuard`, он создаётся сразу после захвата, ещё до выборки:
  * фоновое продление (поток `outbox-lease-heartbeat`) каждые `OUTBOX_LEASE_TTL_MS / 3` — аренда не истекает
    во время блокирующих вызовов: выборки из ClickHouse, ожидания подтверждения Kafka, записи версии;
  * контрольные точки — синхронное продление перед каждой записью пачки, в том числе первой (сразу после
    выборки), и перед каждой записью новой версии outbox (SENT после пропуска или после send + SADD,
    FAILED после ошибки отправки).
* Не продлилась или Redis не ответил — аренда считается потерянной до конца итерации: пачка останавливается,
  новая версия outbox и аудит не пишутся, необработанные записи остаются NEW/FAILED и попадут в следующую
  итерацию нового владельца. Если Kafka уже подтвердила отправку, а аренда потеряна до записи в Redis, в Redis
  не записывается ничего, и новый владелец опубликует запись заново (доставка at-least-once).
* Освобождение (в `finally`) — фоновое продление останавливается, затем Lua-скрипт удаляет ключ, только если
  значение совпадает с токеном. Если аренда истекла и её взял другой экземпляр, старый владелец чужую аренду не удалит.
* Отметки публикации — `SADD devices:published`, `HSET devices:snapshot-at`, `HSET devices:fingerprint` — **только
  после** подтверждения отправки в Kafka и **одним Lua-скриптом** (`RedisPublishedDeviceStore.MARK_PUBLICATION_SCRIPT`):
  * скрипт сначала сверяет значение `outbox:lease` с `ownerToken`: процесс, который замер дольше TTL, после
    возобновления ничего не запишет и не затрёт состояние, сохранённое новым владельцем (результат `LEASE_LOST`,
    версия outbox не пишется);
  * время снимка и отпечаток пишутся вместе и только если сохранённое время снимка не новее записываемого:
    более старый снимок их не откатывает (результат `STALE`, публикация при этом считается завершённой);
  * пропущенный снимок (данные не изменились) двигает под той же проверкой только `devices:snapshot-at`.
* Отпечаток считает `DeviceFingerprint` (раздел 1): совпал с сохранённым — сообщение в `devices` не уходит
  (`SKIPPED`), не совпал или устройства нет — публикуются `UPDATED` или `CREATED`.
* Redis не используется для устранения повторов до создания outbox-записи.

Благодаря фоновому продлению TTL не обязан превышать время выборки или отправки: он должен быть заметно больше
периода продления плюс задержка ответа Redis (при продлении каждые TTL/3 можно пропустить одно продление). Если
аренда всё же потеряна (Redis недоступен, процесс замер дольше TTL), в логе — «Outbox lease lost: …» и
«Outbox lease lost, batch stopped» или «Outbox lease lost while processing record, batch stopped».

Ограничение: проверка аренды — не fencing. Между последней контрольной точкой и `INSERT` в ClickHouse остаётся
короткое окно в одну запись: если процесс замрёт именно в нём дольше TTL, две версии с одинаковым `state_version`
всё же возможны (`ReplacingMergeTree` оставит одну из них). Закрыть окно полностью может только fencing-токен,
который проверяет само хранилище, а условной вставки в ClickHouse нет.

## 9. Масштабирование

* **Приём** — горизонтально: реплики в одной consumer group `KAFKA_CONSUMER_GROUP` делят 3 раздела `events`
  (больше 3 активных потребителей не будет).
* **Публикация outbox** — один активный экземпляр в каждый момент (Redis-аренда). Аренда берётся на одну
  итерацию и удерживается фоновым продлением и проверками перед каждой записью, поэтому публикатор может
  меняться от итерации к итерации, но каждая запись публикуется один раз.

```bash
docker compose -f infrastructure/docker-compose.yaml up -d --scale events-collector-service=2 events-collector-service
```

* **device-collector-service** — так же: реплики в одной consumer group `DEVICE_COLLECTOR_CONSUMER_GROUP` делят 3
  раздела `devices`; запись идемпотентна, поэтому повторная доставка при перераспределении разделов безопасна.
  `docker compose -f infrastructure/docker-compose.yaml up -d --scale device-collector-service=2 device-collector-service`.

У обоих сервисов нет `container_name` и фиксированного хост-порта (диапазоны `EVENTS_COLLECTOR_PORTS` и
`DEVICE_COLLECTOR_PORTS`), Alloy находит реплики через DNS Docker (`discovery.dns`) и собирает их метрики (раздел 16), `instanceId` по умолчанию —
hostname контейнера.
У остальных контейнеров стенда имена фиксированные (`iot-kafka`, `iot-clickhouse`, …).

## 10. Запуск инфраструктуры

Нужны Docker (Compose v2) и ≥ 5 ГБ памяти для базового стенда — около 1 ГБ из них занимает Nexus (≥ 7 ГБ с профилями).

```bash
cp .env.example .env          # при необходимости поменяйте пароли и порты

# базовый стенд: nexus, nexus-init, avros-publish, schema-register, kafka, kafka-init, schema-registry, clickhouse, redis,
# events-collector-service; шарды PostgreSQL и их реплики, shard-replication-init, db-migrator, shardingsphere-proxy,
# device-collector-service; keycloak, redis + redis-sentinel-1..3, redis-replica, device-service ×2 (раздел 22)
docker compose -f infrastructure/docker-compose.yaml up -d

# со стеком наблюдаемости: + prometheus, grafana, loki, tempo, alloy, kafka-exporter, postgres-exporter шардов и реплик
docker compose -f infrastructure/docker-compose.yaml --profile observability up -d
```

То же через Makefile — с ожиданием healthy и понятным кодом выхода:

```bash
make up                       # базовый стенд: Nexus -> публикация avros -> регистрация схем -> миграции -> сервисы
make nexus-up                 # только Nexus (http://localhost:8091)
make up-obs                   # с observability
make up PROFILES=tools        # плюс Kafka UI (http://localhost:8090)
make up PROFILES="observability platform"   # плюс окружение платформы (Postgres, Keycloak, Camunda, MinIO)
make health | make topics | make urls | make logs S=events-collector-service
make down                     # остановить (тома сохраняются)
make clean                    # остановить и удалить тома
```

Профиль `platform` — окружение платформы (Postgres, Camunda, MinIO, postgres-exporter), ни одним из сервисов не
используется. Keycloak входит в базовый стенд: без него device-service не принимает запросы. Если поднят только
`observability`, в Prometheus таргеты `minio` и `postgres` будут `DOWN` (`no such host`) — так и должно быть: их
сервисы входят в `platform`.

**Порядок старта.** Каждый шаг ждёт *успешного завершения* предыдущего (`service_completed_successfully` для
одноразовых задач, `service_healthy` для постоянных сервисов), а не просто запуска контейнера:

| Шаг | Контейнер | Ждёт | Что делает |
|---|---|---|---|
| 1 | `nexus`, затем `nexus-init` | `nexus: service_healthy` | Nexus; пароль `admin`, EULA, пользователь публикации |
| 2 | `avros-publish` | `nexus-init` завершён, `schema-registry: service_healthy` | сверяет схемы со Schema Registry и публикует avros в Nexus (раздел 6) |
| 3 | `schema-register` | `avros-publish` завершён, `schema-registry: service_healthy` | регистрирует схемы, `FULL_TRANSITIVE` |
| 4 | `db-migrator` | `schema-register` завершён, оба шарда `service_healthy` | Flyway применяет миграции к каждому шарду и сверяет версии (раздел 20) |
| 5 | `shardingsphere-proxy` | `db-migrator` завершён | логическая база `devices` поверх двух шардов |
| 6 | `device-collector-service` | `kafka-init`, `schema-register` завершены; `schema-registry`, `shardingsphere-proxy` — `service_healthy` | реестр устройств |
| 6 | `events-collector-service` | `kafka-init`, `clickhouse-init`, `schema-register` завершены; `schema-registry`, `redis` — `service_healthy` | приём событий |
| 6 | `device-service` ×2 | `schema-register`, `kafka-init` завершены; `keycloak`, `redis`, `redis-sentinel-1..3`, `shardingsphere-proxy` — `service_healthy` | API устройств (раздел 22) |
| — | `redis` → `redis-replica` → `redis-sentinel-1..3`; `keycloak` | не зависят от шардов | Redis с Sentinel (ADR-0008) и Keycloak с realm `iot-platform` (импорт при старте) |

Параллельно: `shard-replication-init` (роли `replicator` и `exporter`, слоты репликации; ждёт оба шарда) →
реплики `postgres-shard-N-replica`. Образов с тегом `latest` нет, секретов в репозитории нет — пароли и версии
берутся из `.env` (`.env.example` — шаблон со всеми переменными).

Ошибка на любом шаге останавливает цепочку: например, если регистрация схем завершилась с ошибкой, `db-migrator` и
сервисы не стартуют. Причину показывает `make logs S=<контейнер>` (например, `S=schema-register`).

Nexus — Maven-репозиторий, в который публикуется avros (Avro-схемы и классы платформы). Он входит в базовый
стенд: `make up` сначала поднимает его (`make nexus-up`), затем задача compose `avros-publish` (`make avros-up`; она ждёт и
поднимает Kafka и Schema Registry) сверяет схемы с зарегистрированными и публикует текущую версию avros
(раздел 6) и только потом собирает и запускает остальные сервисы. Одноразовый `nexus-init` (`infrastructure/init/nexus/init-nexus.sh`) после `nexus: healthy`:

* заменяет сгенерированный пароль `admin` на `NEXUS_ADMIN_PASSWORD`;
* принимает EULA Community Edition — только если в `.env` стоит `NEXUS_ACCEPT_EULA=true`. Без этого Nexus отвечает
  403 на любую загрузку и скачивание, поэтому `nexus-init` завершается с кодом 1, а `make nexus-up` и `make up` — с ошибкой и его логом.
  Условия: https://links.sonatype.com/products/nxrm/ce-eula — прочитайте и примите решение сами;
* включает анонимное чтение: контракты не секретны, сервисы скачивают их без пароля;
* создаёт роль `contracts-publisher` (запись только в `maven-releases` и `maven-snapshots`) и пользователя
  `NEXUS_PUBLISH_USER`; при каждом запуске синхронизирует его пароль с `.env`.

`maven-releases` не даёт перезаписать выпущенную версию (повторная загрузка — 409). `maven-public` объединяет
`maven-releases`, `maven-snapshots` и прокси Maven Central. Релиз avros с тегом — `make avros-release`
(раздел 6).

## 11. Запуск сервиса из IDE

1. Поднять стенд без сервиса (Nexus нужен для загрузки avros при сборке):
   `make nexus-up && make avros-up` (публикует текущую версию avros), затем
   `docker compose -f infrastructure/docker-compose.yaml up -d kafka kafka-init schema-registry clickhouse redis`
   и `make schemas-register` (продюсеры работают с `auto.register.schemas=false` — схемы должны быть в Schema Registry),
   (или остановить контейнер сервиса: `docker compose -f infrastructure/docker-compose.yaml stop events-collector-service`).
2. Main class: `com.github.gluhov.iotplatform.eventscollector.EventsCollectorApplication`, профиль `local`
   (добавьте `text`, чтобы видеть обычный текстовый лог вместо JSON). SDK модуля — JDK 25.
3. Переменные окружения — из `.env` (Run Configuration → Environment variables → путь к `.env`;
   в старых версиях IntelliJ IDEA — плагин EnvFile). Адреса по умолчанию для `local` заданы
   в `application-local.yaml`, пароли берутся только из окружения.

Из терминала (из корня репозитория):

```bash
set -a; source .env; set +a
SPRING_PROFILES_ACTIVE=local ./gradlew :services:events-collector-service:bootRun
```

device-collector-service из IDE: main class `com.github.gluhov.iotplatform.devicecollector.DeviceCollectorApplication`,
профиль `local` (порт 8095, Proxy — `localhost:${SHARDING_PROXY_PORT}`, по умолчанию 3307); нужен поднятый стенд
(`make up`) без контейнера `device-collector-service` (`docker compose -f infrastructure/docker-compose.yaml stop
device-collector-service`), переменные — из `.env`:
`SPRING_PROFILES_ACTIVE=local ./gradlew :services:device-collector-service:bootRun`.

## 12. Запуск через Docker Compose

Оба сервиса входят в базовый профиль; events-collector-service собирается из
`services/events-collector-service/Dockerfile`, device-collector-service — из `services/device-collector-service/Dockerfile`
(контекст — корень репозитория). Gradle внутри сборки берёт avros из Nexus хоста: compose передаёт
аргумент `NEXUS_URL=http://host.docker.internal:${NEXUS_PORT}` и `extra_hosts: host.docker.internal:host-gateway`
(внутри сборки `localhost` — сам контейнер сборки). Поэтому перед сборкой образа Nexus должен быть поднят и
содержать нужную версию avros — `make up` делает это сам. Пересобрать после изменений кода:

```bash
docker compose -f infrastructure/docker-compose.yaml up -d --build events-collector-service device-collector-service
```

В контейнере активен профиль `docker`, адреса зависимостей — внутренние (`kafka:9092`, `clickhouse:8123`,
`redis:6379`, у device-collector-service — `shardingsphere-proxy:3307`), healthcheck — `/actuator/health`.

## 13. Быстрая проверка стенда

```bash
services/events-collector-service/scripts/check-local.sh     # или make check-local
./gradlew :services:events-collector-service:checkLocal      # то же через gradle (группа verification)
```

Скрипт отправляет один `DeviceEvent` (уникальные `eventId` и `deviceId`, ключ `deviceId`) и проверяет:
сервис healthy → строка в `device_events` → запись в `device_outbox` → статус `SENT` → аудит `PUBLISHED` →
`DeviceInfo` в теме `devices` → `/actuator/health` = `UP`. Код выхода 0 — «All checks passed»; иначе печатается
упавший шаг.
Это быстрая проверка поднятого стенда, а не замена интеграционным тестам.

Корневой `.env` скрипт передаёт Compose явно (`--env-file`, как и Makefile), поэтому он одинаково работает
напрямую из любого каталога, через `make check-local` и через Gradle (задача запускает его из каталога сервиса)
и не зависит от симлинка `infrastructure/.env` — на Windows без поддержки симлинков на его месте оказывается
текстовый файл. Без `--env-file` Compose подставил бы пустые значения переменных и завершился бы с ошибкой
конфигурации. Если корневого `.env` нет, скрипт сразу сообщает об этом.

## 14. Модульные тесты

```bash
./gradlew :services:events-collector-service:test     # или make test; Docker не нужен
./gradlew :services:device-collector-service:test     # или make dcs-test
./gradlew :services:device-service:test                # или make ds-test
./gradlew :services:db-migrator:test
```

events-collector-service — 132 теста, device-collector-service — 142 (раздел 19.8), device-service — 100 (раздел 22), db-migrator — отдельные unit-тесты (по отчёту сборки). Числа — из отчётов сборки последнего прогона.
Сообщения журнала проверяются через `support/LogCapture` (Logback `ListAppender` со снимком MDC на момент записи).
Ключевые сценарии events-collector-service:

* `EventIngestionScenarioTest` — корректное событие, пустые `eventId`/`deviceId`, некорректный `timestampMs`,
  ошибки записи в `device_events` и `device_outbox`: проверяется, вызван ли `ack.acknowledge()`; спан приёма
  сохраняется в outbox-записи;
* `OutboxPublishingServiceTest` — новое устройство (порядок send → SADD → SENT), уже опубликованное,
  ошибка Kafka (FAILED, attempts + 1), аренда не получена, освобождение аренды при ошибке,
  продление аренды перед каждой записью (в том числе первой) и перед каждой записью версии; потеря аренды после
  выборки, посреди пачки, во время отправки, после ошибки Kafka и при ошибке Redis — пачка останавливается, версия
  и аудит не пишутся; отправка внутри трейса приёма, запись аудита (`PUBLISHED` с номером попытки, `SKIPPED`,
  `FAILED` с ошибкой) и публикация при недоступном аудите; ошибка Redis при обработке записи — в сообщении
  «Outbox record not processed» есть `eventId`, `deviceId`, `traceId` приёма, `status`, `errorClass`, `errorMessage`,
  аудит `ERROR` записан в том же MDC и трейсе;
* `LeaseGuardTest` — фоновое продление на настоящем планировщике с коротким TTL, обнаружение чужой аренды
  (дальше Redis не вызывается), ошибка Redis — потеря аренды, `close()` останавливает продление;
* `OutboxRecordTest` — версии записи: повторная доставка, обрезка ошибки, перенос контекста трассировки;
* `RedisLeaseServiceTest` — освобождение и продление аренды владельцем, попытка освободить или продлить чужую аренду;
* `KafkaErrorClassifierTest`, `KafkaConfigTest` — таймаут и ответ 5xx Schema Registry — временные (повтор без
  ограничения), не Avro и неизвестная схема — «ядовитые» (сразу `events-dlt`), ошибка записи в ClickHouse —
  `EVENTS_MAX_ATTEMPTS` попыток с растущей паузой (по умолчанию 1 с и 2 с), затем `events-dlt`;
* `DeviceFingerprintTest` — отпечаток не зависит от `lastSeenAtMs` и показаний сенсоров, меняется при изменении
  имени, ОС или прошивки; `OutboxPublishingServiceTest` — те же данные: публикации нет (`SKIPPED`), изменились:
  `UPDATED`, новое устройство: `CREATED`, сбой отправки: отпечаток не меняется;
* `ContractCompatibilityTest` — сообщение предыдущей версии читается новой схемой и наоборот (оба направления);
* `MetricsFacadeTest` — имена метрик в формате Prometheus;
* `ArchitectureTest` (ArchUnit, 9 правил) — слои и направления зависимостей: домен без Kafka, JDBC и Redis,
  адаптеры вызывают домен только через `port.in`, `@KafkaListener` и `@Scheduled` — только во входных адаптерах,
  порты `out` реализуют выходные адаптеры, между пакетами нет циклов.

## 15. Интеграционные тесты

```bash
./gradlew :services:events-collector-service:integrationTest   # или make integration-test; нужен запущенный Docker
./gradlew :services:events-collector-service:check             # все тесты + порог покрытия 70 % (make coverage)
./gradlew :services:device-collector-service:integrationTest   # или make dcs-it (раздел 19.8)
./gradlew :services:device-collector-service:check             # порог покрытия 70 % (make dcs-coverage)
./gradlew :services:device-service:integrationTest              # или make ds-it (Testcontainers, 49 тестов)
```

Классы `It*Test` events-collector-service (33 теста). Kafka, Schema Registry, ClickHouse и Redis поднимаются Testcontainers
(`config/*TestContainerConfig`, базовый класс `it/AbstractItTest`), заранее запущенный compose не нужен.
Используются те же `infrastructure/init/clickhouse/01-schema.sh` и `infrastructure/init/kafka/init-topics.sh`,
что и на стенде.

* `ItEventsCollectorTest` — приём события; публикация outbox и пропуск повторной публикации; повторная доставка
  того же `eventId`; ошибка публикации, повторная попытка и остановка после `OUTBOX_MAX_ATTEMPTS`; строки аудита
  каждой попытки (исход, номер попытки, `instance_id`); `DeviceInfo` уходит в трейсе приёма (`traceId` из
  `traceparent` совпадает с `trace_id` в outbox); аренда истекает во время `findPending` и её берёт другой
  экземпляр — ничего не отправлено, новой версии outbox и аудита нет, аренда нового владельца не тронута;
  ошибка Redis при публикации — в сообщении «Outbox record not processed» есть `eventId`, `deviceId` и `traceId`
  приёма (совпадает с `trace_id` в outbox), в аудите — `ERROR`;
* `ItKafkaErrorHandlingTest` — ошибки приёма на отдельной теме с одним разделом и своей consumer group, после
  каждой проверяется committed offset: однократный таймаут Schema Registry (`SocketTimeoutException`) — сообщение
  повторено и сохранено, offset подтверждён после обоих сообщений; не-Avro сообщение — в `events-dlt` с исходными
  байтами, его offset подтверждён сразу после записи в DLT; невалидное событие — в `events-dlt` в Avro; ошибка записи
  в ClickHouse — `EVENTS_MAX_ATTEMPTS` попыток, затем `events-dlt`, offset подтверждён (лаг группы 0); следующее
  корректное сообщение всякий раз обработано;
* `ItRedisLeaseServiceTest` — захват, освобождение владельцем, попытка удалить чужую аренду, истечение TTL,
  продление своей аренды, отказ продлить чужую или истёкшую; `LeaseGuard` удерживает аренду дольше TTL и замечает,
  что её взял другой экземпляр.

Покрытие строк (JaCoCo, unit + integration): порог 70 % у events-collector-service, device-collector-service и device-service
(`check` падает ниже порога); отчёт —
`services/<сервис>/build/reports/jacoco/test/html/index.html`.
Avro-классы приходят из `avros` и в отчёт не входят; код Lombok исключён из отчёта.

## 16. Наблюдаемость

| Что | Где |
|---|---|
| Health | http://localhost:&lt;порт&gt;/actuator/health |
| Метрики | http://localhost:&lt;порт&gt;/actuator/prometheus |
| Grafana | http://localhost:3000 → папка «IoT Platform»: «events-collector-service», «device-collector-service», «device-service», «Proxy и шарды реестра устройств», «Kafka», «PostgreSQL» (платформенный) |
| Prometheus | http://localhost:9090 → Status → Targets: jobs `shardingsphere-proxy`, `postgres-shards` (шарды и их реплики), `kafka` |
| Alloy | http://localhost:12345 (`ALLOY_PORT`) → Graph / компоненты `prometheus.scrape.*`: состояние сбора метрик трёх сервисов (по экземплярам: health и последняя ошибка). Ряд `up{job="events-collector-service"\|"device-collector-service"\|"device-service"}` в Prometheus формирует Alloy; страница Targets этих заданий не показывает |
| Loki | Grafana → Explore → Loki: `{service="events-collector-service"} \| json`, `{service="device-collector-service", shard="shard_0"} \| json` |
| Tempo | Grafana → Explore → Tempo: `{name="events process"}` (приём), `{name="publish devices"}` (публикация), `{name="devices process"}` (device-collector-service) |
| Kafka UI | http://localhost:8090 (профиль `tools`): темы, сообщения Avro как JSON, лаг consumer group, схемы |

Порт сервиса на хосте Docker выбирает из диапазона `EVENTS_COLLECTOR_PORTS` (по умолчанию 8085–8087) и
`DEVICE_COLLECTOR_PORTS` (по умолчанию 8095–8097); диапазон нужен для `--scale`. Фактический порт: `make urls` или
`docker compose -f infrastructure/docker-compose.yaml port events-collector-service 8080` (для device-collector-service —
`port device-collector-service 8080`).

У events-collector-service наружу открыты только `/actuator/health`, `/actuator/metrics`, `/actuator/prometheus`;
бизнесовых REST-точек нет. У device-collector-service те же технические точки и одна бизнесовая:
`GET /api/v1/devices/{deviceId}` (раздел 21).

**Метрики:** `events_processed_total`, `events_persist_fail_total`, `outbox_candidate_created_total`,
`outbox_publish_success_total`, `outbox_publish_fail_total`, `outbox_publish_skipped_total`,
`outbox_pending_count`, а также стандартные JVM, HikariCP и `spring_kafka_listener_seconds`.

> **Про `outbox_candidate_created_total`.** Spring Boot 4 использует Prometheus Java client 1.x, который
> запрещает имена метрик с суффиксом `_created` (он зарезервирован в OpenMetrics под время создания
> счётчика). Поэтому `/actuator/prometheus` отдаёт этот счётчик как `outbox_candidate_total`, а требуемое
> имя `outbox_candidate_created_total` восстанавливает правило `prometheus.relabel` в
> `infrastructure/config/alloy/config.alloy`. В Prometheus и Grafana метрика называется так, как
> требует задание. Остальные шесть имён совпадают уже в `/actuator/prometheus` (проверяет `MetricsFacadeTest`).

**Путь метрик сервисов платформы (ADR-0012).** events-collector-service, device-collector-service и device-service (все
экземпляры) собирает Alloy: находит экземпляры через DNS Docker, опрашивает `/actuator/prometheus` каждые 5 с и передаёт
метрики в Prometheus по remote write (Prometheus запущен с `--web.enable-remote-write-receiver`, Alloy стартует после него).
Метки прежние: `job` — имя сервиса, `instance` — адрес экземпляра; запросы панелей не менялись. Экспортёры, Proxy,
Keycloak, MinIO и компоненты наблюдаемости Prometheus по-прежнему опрашивает сам. Alloy остановлен — пропадают метрики
сервисов, логи и трассы; новых переменных нет (`DEVICE_SERVICE_MANAGEMENT_PORT` и `ALLOY_PORT` уже были; первая теперь
передаётся и Alloy).

**Метрики device-collector-service** (`/actuator/prometheus`, job `device-collector-service`): сообщения —
`devices_messages_received_total`, `devices_messages_processed_total` (метка `outcome`: применено, устаревшее),
`devices_messages_duplicate_total`, `devices_messages_failed_total`; `devices_dlt_published_total` (метка `reason`);
время обработки `devices_processing_seconds_*`; запись в шард — `devices_db_write_seconds_*` и транзакции с меткой
`shard`; ошибки — `devices_processing_errors_total`, `devices_shard_route_fail_total`; а также стандартные JVM,
HikariCP (`sharding-proxy`) и лаг consumer group из kafka-exporter.

**Метрики device-service** (job `device-service`, сбор — `infrastructure/config/alloy/config.alloy`, дашборд «IoT Platform — device-service» — `infrastructure/config/grafana/dashboards/device-service.json`):
Alloy забирает management-порт 8081 внутри сети Compose (`DEVICE_SERVICE_MANAGEMENT_PORT`, передаётся в Alloy через `sys.env`); порт на хост не публикуется,
а основной порт не отдаёт `/actuator` (раздел 22.1). Метрики: `device_commands_*` (команды: принято, отклонено, время публикации в Kafka)
и `device_cache_*` (исходы чтения, доля попаданий), HTTP-метрики Spring; в метках нет `deviceId`.

**Метрики ShardingSphere-Proxy** — агент метрик входит в образ Proxy 5.5.3 и включается переменной
`PROXY_AGENT_ENABLED=true` (параметр запуска `-g`, конфигурация — `infrastructure/config/shardingsphere-agent/agent.yaml`);
Prometheus забирает `shardingsphere-proxy:9090` (job `shardingsphere-proxy`): `proxy_current_connections`,
`proxy_requests_total`, `proxy_transactions_total` (метка `type`), `proxy_execute_latency_millis_*`, `proxy_state`,
`routed_sql_total` и `routed_result_total` (маршрутизация по шардам), JVM.

**Метрики шардов и реплик PostgreSQL** — по экспортёру `postgres-exporter` (v0.20.1) на каждый шард и каждую
реплику (контейнеры `postgres-exporter-shard-N` и `postgres-exporter-shard-N-replica`, job `postgres-shards`, метка
`instance`). Подключаются ролью `exporter` (членство в `pg_monitor`, пароль `SHARD_EXPORTER_PASSWORD`; роль не видит
данные таблиц, её создаёт `shard-replication-init`). Репликация: `pg_replication_is_replica`,
`pg_replication_lag_seconds` (отставание реплики), `pg_stat_replication_pg_wal_lsn_diff` (отставание по WAL),
`pg_replication_slots_*` (активность слота и запас WAL до лимита `max_slot_wal_keep_size`), `pg_up`.
Дашборд «Proxy и шарды реестра устройств» (`device-shards`) собирает Proxy, шарды и реплики на одном экране;
«Kafka» — метрики брокера и лаг групп из kafka-exporter.

**Логи** всех компонентов стенда — JSON (формат logstash) или текст в stdout, Alloy отправляет их в Loki
(по контейнеру: `{container="iot-shardingsphere-proxy"}` и т. д.). Поля логов сервисов: `service`, `instanceId`,
`traceId`, `spanId`, `eventId`, `deviceId`, `topic`, `partition`, `offset`, `status`, `errorClass`,
`errorMessage`; у device-collector-service ещё `shard` (база шарда: `shard_0`/`shard_1`; в Loki это метка —
значений всего два), `eventType`, `status` (`APPLIED`, `STALE`, `DUPLICATE` — исход обработки; `DLT` — сообщение отложено в `devices-dlt`; `REJECTED` и `FAILED` — ошибка). Значения `status`: `PROCESSED`, `REJECTED`, `FAILED` (приём), `PUBLISHED`, `SKIPPED`,
`FAILED`, `ERROR` (публикация). В Loki `level` — метка, `trace_id`/`event_id`/`device_id` — structured
metadata (не метки, чтобы не раздувать индекс). Секреты и строки подключения в логи не пишутся.
Публикатор пишет сообщения об обработке outbox-записи, в том числе об ошибке «Outbox record not processed»
(`status=ERROR`), и строку аудита внутри MDC и трейса её события: в сообщении есть `eventId`, `deviceId` и `traceId`
приёма, поэтому по журналу находится и сама запись outbox, и трейс её приёма в Tempo.
В Grafana поле `traceId` в строке лога — ссылка на трейс в Tempo.

**Трассировки** — Micrometer Tracing + OpenTelemetry, OTLP HTTP → Alloy (`:4320`) → Tempo, выборка 100 %.

* Сквозной трейс события (приём и публикация): `events process` → `validate` → `insert device_events` →
  `insert device_outbox`, а после итерации публикатора там же `outbox publish` (дочерний спан `events process`) →
  `redis devices:published check` → `publish devices` → `devices send` (спан Kafka-продюсера) →
  `redis devices:published add` → `redis outbox:lease extend` → `append SENT version`.
* Пропуск публикации — в трейсе своего события: `outbox publish` → `redis devices:published check` →
  `redis outbox:lease extend` → `append SENT version`.
* Трейс итерации публикатора: `task outboxPublisherJob.run` → `outbox batch` — выборка и контрольные продления
  аренды перед каждой записью; обработка записей уходит в трейсы их событий. Фоновое продление спанов не создаёт.
* **Один traceId от события до PostgreSQL.** `DeviceInfo` уходит в `devices` с заголовком `traceparent` (спан
  `devices send`), device-collector-service продолжает этот трейс: `devices process` → `validate` → `transaction` →
  `select device for update` → `insert processed_device_events` → `insert devices` (или `update devices: snapshot`).
  В Tempo это один трейс из спанов обоих сервисов: найти его можно по `traceId` из лога любого из них
  (в Grafana поле `traceId` в строке лога — ссылка на трейс). Прохождение до шарда видно и по метрике, и по метке
  `shard` в логе device-collector-service.

Запросы к `/actuator/*` и спаны команд Redis (Lettuce) не трассируются (`ObservationConfig`), чтобы в Tempo
оставались только пути приёма и публикации. Шаги с Redis видны через собственные спаны сервиса.

Публикация выполняется позже и в другом потоке, поэтому контекст трассировки переносится через саму outbox-запись:
при приёме `EventIngestionService` сохраняет `trace_id`/`span_id` текущего спана в `device_outbox`, при публикации
`OutboxPublishingService` делает спан `outbox publish` дочерним к нему (`TraceContextCarrier`, реализация —
`support/MicrometerTraceContextCarrier`). Связь задаёт контекст W3C, а не поиск по `eventId`. Заголовок `traceparent`
сообщения `DeviceInfo` содержит тот же `traceId`, поэтому потребители `devices` продолжают этот трейс, а если источник
событий присылает `traceparent` в `events`, трейс начинается ещё у него. Без профиля `observability` трейсы
не доставляются (Alloy не запущен): OpenTelemetry-экспортёр периодически пишет в лог ошибку
`Failed to export spans ... UnknownHostException: alloy` — на работу сервиса это не влияет.

**Снимки экрана** (раздел 22 задания) — `docs/screenshots/`:

| Файл | Что |
|---|---|
| `grafana-dashboard.png` | дашборд сервиса с метриками |
| `tempo-trace-end-to-end.png` | сквозной трейс успешного пути events-collector-service: приём (`events process`) и публикация (`outbox publish` — дочерний спан) в одном трейсе |
| `tempo-trace-skip.png` | пропуск публикации для уже опубликованного устройства — в трейсе своего события: `outbox publish` → `redis devices:published check` → `redis outbox:lease extend` → `append SENT version` → `append audit record`, без отправки в Kafka |

Снимки экрана реестра устройств (задание: «панели Grafana с метриками и одной сквозной трассой»):

| Файл | Что |
|---|---|
| `grafana-device-collector.png` | дашборд device-collector-service при нагрузке: сообщения, повторы, DLT, лаг, запись по шардам |
| `grafana-proxy-shards.png` | дашборд «Proxy и шарды реестра устройств»: Proxy, маршрутизация по шардам, шарды и реплики, отставание реплик |
| `grafana-kafka.png` | дашборд «Kafka»: темы и лаг consumer group |
| `tempo-trace-events-to-postgres.png` | сквозной трейс одного события: `events process` … `devices send` (events-collector-service) → `devices process` … `insert devices` (device-collector-service) |

Снимки экрана device-service:

| Файл | Что |
|---|---|
| `grafana-device-service.png` | дашборд device-service: запросы по статусам, отказы 401/403/5xx, задержки p95/p99, команды по операциям, публикация в Kafka, исходы кэша и доля попаданий |
| `grafana-device-collector-operations.png` | дашборд device-collector-service с командами администратора: добавлены исход `ignored` и сброс кэша устройства |
| `tempo-trace-device-service.png` | сквозной трейс одного PUT: `http put /api/v1/devices/{deviceId}` и проверка токена (device-service) → `devices send` → `devices process`, `select device for update`, `insert processed_device_events`, `update devices: command`, `evalsha` сброса кэша (device-collector-service) |

Снимать при идущей нагрузке (например, несколько раз `make check-local`), иначе панели без трафика пусты.

## 17. Диаграммы

* `diagrams/context.mmd` и `diagrams/containers.mmd` — контекст и контейнеры платформы: приём событий и реестр
  устройств, Nexus, ShardingSphere-Proxy, шарды PostgreSQL и реплики, db-migrator, темы DLT, наблюдаемость;
* events-collector-service (приём событий):
  * `diagrams/component-events-collector.mmd` — компоненты сервиса, Kafka, ClickHouse, Redis, наблюдаемость;
  * `diagrams/sequence-ingest.mmd` — приём события (снимок устройства, DLT, повторы);
  * `diagrams/sequence-outbox-publish.mmd` — публикация outbox: `DeviceInfo` с `CREATED` / `UPDATED` / `DELETED`;
* device-collector-service (реестр устройств):
  * `diagrams/component-device-collector.mmd` — компоненты сервиса, Proxy, шарды, db-migrator;
  * `diagrams/dcs-success-sequence.mmd` — новое событие устройства;
  * `diagrams/dcs-duplicate-sequence.mmd` — повторная доставка и устаревшее событие;
  * `diagrams/dcs-error-sequence.mmd` — невалидное сообщение, недоступный шард, DLT;
  * `diagrams/dcs-operations-sequence.mmd` — команды CREATE / UPDATE / DELETE и события роутера (ADR-0005);
  * PNG последовательностей — `diagrams/dcs-*.png` (рендер mermaid-cli);
* device-service (раздел 22): `diagrams/component-device-service.mmd`, `diagrams/ds-read-sequence.mmd`,
  `diagrams/ds-write-sequence.mmd`, `diagrams/ds-cache-consistency-sequence.mmd`; PNG рядом с каждым `.mmd`.

GitHub показывает `.mmd` сразу; в IntelliJ IDEA — плагин Mermaid; также можно вставить на https://mermaid.live.

## 18. Типовые проблемы

| Проблема | Что проверить |
|---|---|
| Schema Registry недоступен | `make health`; `curl localhost:8081/subjects`; у сервиса `SCHEMA_REGISTRY_URL` должен указывать на `http://schema-registry:8081` внутри сети compose и на `http://localhost:8081` из IDE; SR стартует после `kafka: healthy` |
| `nexus-init` завершился с кодом 1 | `make logs S=nexus-init`. «работает только после принятия EULA» — решение о `NEXUS_ACCEPT_EULA=true` в `.env` за вами; «NEXUS_ADMIN_PASSWORD из .env не подходит» — пароль `admin` уже сменён при первом запуске, верните прежнее значение в `.env` или удалите том `nexus-data` (артефакты Nexus будут потеряны) |
| сборка: `Could not resolve com.github.gluhov.iotplatform:avros` | Nexus не поднят или в нём нет версии: `make nexus-up && make avros-up` (для старой версии — `make avros-bootstrap`); другой порт Nexus — `-PnexusUrl=http://localhost:<NEXUS_PORT>`. В `docker build` — `Connection refused` к `host.docker.internal`: Nexus не поднят на хосте или сборка запущена без `extra_hosts` (собирайте через compose или `--add-host host.docker.internal:host-gateway`) |
| `make avros-release` / `avros-check` с ошибкой | «Nexus не готов к записи (HTTP 000)» или «Nexus недоступен» — `make nexus-up`, `make health`; «незакоммиченные изменения» — релиз собирается только из коммита, закоммитьте `avros/` (или используйте `-SNAPSHOT`); «тег … уже есть» — версия выпущена, поднимите `version`; «в теге … version = …» — тег поставлен не на тот коммит, перенесите его (`git tag -d`, затем `git tag -a`); «Версия … уже опубликована» — поднимите `version` в `avros/build.gradle.kts`; «должна быть больше последней выпущенной» — версия ниже релиза; «Схемы несовместимы» — в перечне запись, направление (`BACKWARD`/`FORWARD`) и поле: добавьте `default` или поднимите старшую версию; «Схемы несовместимы с зарегистрированными в Schema Registry» — то же для subject и версии в Schema Registry (проверка перед загрузкой в Nexus: ничего не опубликовано), посмотреть зарегистрированное — `make schemas`; «Schema Registry недоступна» / «Не задан -PschemaRegistryUrl» — `make registry-up`, прямой `./gradlew :avros:publish` без `-PschemaRegistryUrl`/`-PschemaSubjects` не работает; `401 Unauthorized` при PUT — пароль публикатора в Nexus не совпадает с `.env`, перезапустите `nexus-init` (`make nexus-up`) |
| Kafka-темы не создались | `docker compose -f infrastructure/docker-compose.yaml logs kafka-init` (код выхода — `make health`); скрипт `infrastructure/init/kafka/init-topics.sh` должен быть исполняемым; `make topics` — есть ли `events`/`devices` и `cleanup.policy`. `--if-not-exists` не меняет настройки существующей темы. Для `devices` политику `delete` и `retention.ms` `kafka-init` выставляет при каждом запуске; остальные настройки — `kafka-configs.sh --alter` или `make kafka-reset` (удалит данные Kafka) |
| ClickHouse недоступен | `make health` — healthcheck ждёт таблицу `device_outbox` в базе `CLICKHOUSE_DB`; порт HTTP `8123` (native на хосте — `9010`); `CLICKHOUSE_DATABASE=iot_platform`; `01-schema.sh` выполняется только на пустом томе — после его правки `make clean && make up` |
| outbox не публикуется | `make redis-cli` → `GET outbox:lease`, `PTTL outbox:lease` (кто держит аренду); метрика `outbox_pending_count` растёт; `outbox_publish_fail_total` и логи со `status=FAILED` (`last_error` в `device_outbox`); записи с `attempts >= OUTBOX_MAX_ATTEMPTS` больше не выбираются |
| аренда не освобождается или пачка останавливается | аренду продлевает фоновый поток каждые `OUTBOX_LEASE_TTL_MS / 3` и проверки перед каждой записью; «Outbox lease lost» в логе — продлить не удалось (Redis недоступен, процесс замер дольше TTL, ключ удалён вручную): пачка остановлена, записи выберет следующая итерация; «Outbox lease expired before the iteration finished» без «lease lost» — TTL слишком мал для периода продления; значение ключа — токен владельца, Lua-скрипты продлевают и удаляют ключ только при совпадении токена; после падения экземпляра аренда исчезнет сама через TTL |
| нет трассировок | запущен ли профиль `observability`; `MANAGEMENT_OTLP_ENDPOINT` (`http://alloy:4320/v1/traces` в compose, `http://localhost:4318/v1/traces` из IDE — напрямую в Tempo); Alloy: http://localhost:12345 → компоненты `otelcol.*`; Tempo healthy; интервал поиска в Grafana захватывает время событий |
| нет логов в Loki | `infrastructure/config/alloy/config.alloy`: фильтр по метке `com.docker.compose.project` = `COMPOSE_PROJECT_NAME`; Alloy видит `/var/run/docker.sock`; метки `service`/`container`; запрос `{service="events-collector-service"}` |
| панель логов в Grafana: `timeout awaiting response headers` | после долгого сбоя (например, ClickHouse недоступен) в логах много стек-трейсов, и ответ Loki превышает лимит gRPC; в `loki-config.yaml` лимит поднят до 16 МБ (`grpc_server_max_*_msg_size`) — после правки `docker compose -f infrastructure/docker-compose.yaml restart loki` |
| сборка образа: `failed to resolve source metadata for docker.io/docker/dockerfile` | временно недоступен Docker Hub (фронтенд `# syntax=docker/dockerfile:1.7`); повторить сборку или заранее `docker pull docker/dockerfile:1.7` |
| сервис не стартует: `input/output error` при `docker pull`/`build` | переполнен диск VM Docker или диск хоста; освободить место (`docker builder prune -af`, удаление неиспользуемых образов) и перезапустить Docker Desktop |
| нет строк в `device_publication_audit`, ClickHouse unhealthy после обновления | стенд поднят до появления таблицы: `02-audit.sh` выполняется только на пустом томе — создать таблицу вручную (раздел 7); при ошибке записи аудита в логе сервиса — «Publication audit record not written» |
| публикация в отдельном трейсе, а не в трейсе события | запись принята до добавления колонок `trace_id`/`span_id` (у неё они пустые) — так и должно быть; если пусто у новых записей — не выполнен `ALTER` из раздела 7 или ingest шёл без трассировки |
| сообщения events не читаются, лаг растёт | логи сервиса со `status=FAILED`: Schema Registry недоступен — раздел ждёт с backoff до 10 с без ограничения, после восстановления чтение продолжится; ошибка ClickHouse — `EVENTS_MAX_ATTEMPTS` попыток, затем сообщение уходит в `events-dlt` и раздел идёт дальше. Раздел стоит и тогда, когда сообщение не удаётся отложить в DLT — в логе «Dead-letter publication … failed», проверить тему `events-dlt` |
| сообщения в `events-dlt` | входное сообщение не десериализовалось, не прошло валидацию или не записалось в ClickHouse за `EVENTS_MAX_ATTEMPTS` попыток; причина — в заголовках `kafka_dlt-exception-*` (Kafka UI, профиль `tools`) и в логе со `status=REJECTED` или `FAILED`; после исправления источника сообщение можно отправить заново |
| сообщения в `devices-dlt` | то же для device-collector-service: невалидный `DeviceInfo`, нечитаемое сообщение или запись в шард не удалась `DEVICES_MAX_ATTEMPTS` раз (шард или Proxy недоступны); причину показывает заголовок `kafka_dlt-exception-*` и лог со `status=REJECTED`/`FAILED`; поле `reason` метрики `devices_dlt_published_total` |
| `shard-replication-init` завершился с кодом ≠ 0, экспортёры и реплики не стартуют | `make logs S=shard-replication-init`. «`SHARD_EXPORTER_PASSWORD не задан`» — переменной нет в `.env` (compose не читает `.env.example`); пароль экспортёра — без пробелов (`malformed dsn`). Скрипт идемпотентен: после исправления `make up` |
| реплика шарда не в режиме `streaming` | `make replication-status` (код выхода 1 при сбое): на шарде — клиент `walreceiver` и слот `replica_N`, на реплике — режим восстановления. Слот со `wal_status: lost` пересоздаётся `shard-replication-init`; реплика отстала дольше `max_slot_wal_keep_size` (1 ГБ WAL) — удалите её контейнер и том (`postgres-shard-N-replica-data`), `make up` снимет новый `pg_basebackup` |
| device-collector-service не стартует, `db` в `/actuator/health` = `DOWN` | Proxy не готов или шарды недоступны: `make health`, `make logs S=shardingsphere-proxy`; Proxy ждёт `db-migrator` — при ошибке миграций (`schema versions differ between shards`) стенд дальше не поднимается: `make logs S=db-migrator` |
| в Prometheus таргеты `minio`, `keycloak`, `postgres` — `DOWN` | сервисы входят в профиль `platform`, который не запущен; на реестр устройств и приём событий не влияют (раздел 10) |
| `alloy` в состоянии `Restarting`, логи и трейсы не доходят | `make logs S=alloy`: `/etc/alloy/config.alloy:N:M: missing ','` — в `infrastructure/config/alloy/config.alloy` у полей `expressions = { … }` запятая нужна после каждого элемента, включая последний |
| метрик шардов или Proxy нет в Prometheus | профиль `observability` не запущен; `postgres-exporter-shard-*` стартует после `shard-replication-init`; Proxy отдаёт метрики, только если `PROXY_AGENT_ENABLED=true` (`docker compose … exec shardingsphere-proxy env \| grep AGENT`) |
| лаг consumer group отрицательный (`-1`) | у группы ещё нет подтверждённого offset в разделе (в раздел не приходили сообщения) — kafka-exporter отдаёт `-1`; после первого обработанного сообщения лаг станет 0 |
| порт занят | поменять `*_PORT` / `EVENTS_COLLECTOR_PORTS` в `.env` |
| `check-local.sh`: «.env not found» | создать корневой `.env`: `cp .env.example .env` |
| ClickHouse падает (`Exited (132)`, в `clickhouse-server.err.log` — `Illegal instruction`) на `SELECT ... FINAL` | Apple M4 + Docker Desktop: arm64-сборка ClickHouse 26.2 получает SIGILL, amd64-образ через Rosetta работает. В `.env` задано `CLICKHOUSE_PLATFORM=linux/amd64`; интеграционные тесты на macOS с Apple Silicon тоже берут amd64 (переопределяется переменной `CLICKHOUSE_PLATFORM`). На Linux/x86 amd64 — нативная платформа |
| `No space left on device` в логах Kafka/ClickHouse | заполнен диск VM Docker (`docker system df`) — освободить место или увеличить Disk usage limit в Docker Desktop, затем пересоздать том Kafka (`make kafka-reset`): брокер помечает каталог логов как сбойный |

## 19. device-collector-service (реестр устройств)

### 19.1 Что делает

1. читает `devices` по одному сообщению (`max-poll-records: 1`, `enable-auto-commit: false`, `ack-mode: manual_immediate`);
2. проверяет `DeviceInfo` (`DeviceInfoValidator`): невалидное сообщение сразу уходит в `devices-dlt`;
3. применяет его к реестру в **одной локальной транзакции на шарде устройства** через ShardingSphere-Proxy:
   `SELECT … FOR UPDATE` строки устройства → исход события → запись в журнал `processed_device_events`
   (`INSERT … ON CONFLICT DO NOTHING`) → вставка или обновление `devices`;
4. подтверждает offset **только после `COMMIT`** (либо после записи в `devices-dlt`);
5. адресов шардов не знает: один datasource — логическая база `devices` в Proxy (`DEVICES_DB_URL`); какой шард выполнил
   запрос, видно из `RETURNING current_database()`.

Тема `devices` несёт и события роутера (поле `operation` не задано), и команды device-service (`CREATE`, `UPDATE`,
`DELETE`; ADR-0002, ADR-0005). Команда администратора применяется по `managed_at` и `deleted_at`; событие роутера
не воскрешает устройство, удалённое администратором, и не перезаписывает поля, заданные администратором.
После `COMMIT` сервис сбрасывает кэш device-service в Redis: `floor = max(floor, version)` в отдельном ключе
`device:floor:v1:{deviceId}`, старые данные из кэша удаляются (Lua, `raise-floor.lua`). Порог живёт дольше кэша
(`DEVICE_CACHE_FLOOR_TTL`): в одном ключе с ответом он истекал вместе с ним, и отставание реплики дольше
`DEVICE_CACHE_TTL` снова возвращало старые данные (ADR-0003). Известное ограничение: если отставание переживёт и
`DEVICE_CACHE_FLOOR_TTL`, старая версия может лечь в кэш ещё на один `DEVICE_CACHE_TTL` — срок выбирается больше
худшего ожидаемого отставания. Ошибка Redis не задерживает offset: пишется WARN и
`devices_cache_invalidations_total{result=error}`.
Схема потоков — `diagrams/dcs-operations-sequence.mmd`.

**Идемпотентность.** Исход события определяется состоянием строки: повторная доставка того же `eventId` (журнал вернул
0 строк) — `DUPLICATE`, ничего не меняется и `seen_count` не растёт; событие со снимком старше последнего применённого
(`last_state_at`) — `STALE`: только `seen_count + 1`, более старое сообщение не перезаписывает более новое; иначе —
`APPLIED`; `IGNORED` — команда по удалённому устройству, событие роутера для устройства, удалённого администратором, или событие
роутера `DELETE` без снимка для управляемого устройства (учитывается в `seen_count`, состояние не меняется). Снимок регистрации, пришедший после более поздней телеметрии, применяется. События одного устройства
обрабатываются строго по очереди (`FOR UPDATE`, ключ `deviceId` в теме).

### 19.2 Ошибки и DLT

| Ошибка | Что происходит |
|---|---|
| невалидное сообщение (нет обязательных полей, время в далёком будущем) или нечитаемое (не Avro, схемы нет) | сразу в `devices-dlt`, без повторов; `status=REJECTED` |
| ошибка записи в PostgreSQL (шард или Proxy недоступны, тайм-аут, сбой маршрутизации) и непредвиденная ошибка | `DEVICES_MAX_ATTEMPTS` попыток с паузой `DEVICES_RETRY_INTERVAL`, дальше вдвое (по умолчанию 3 попытки: паузы 1 с и 2 с), затем `devices-dlt`; остальные сообщения раздела продолжают обрабатываться |
| Schema Registry недоступен | повтор без ограничения с паузой до 10 с: сбой инфраструктуры, а не сообщения |
| не удалось записать в `devices-dlt` | сообщение не подтверждено и будет обработано снова |

Сообщение попадает в DLT с исходным ключом и заголовками `kafka_dlt-*` (исходные тема, раздел, offset, класс и текст
ошибки); offset подтверждается только после успешной записи в `devices-dlt`. Недоступность одного шарда не останавливает
устройства другого шарда (`ItDeviceCollectorTest.givenShardDown_whenConsumed_thenOtherShardProcessed`).

### 19.3 Параметры

Подключения и пароли — только из переменных окружения (в репозитории их нет), значения по умолчанию — в `.env.example`:

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL` | Kafka и Schema Registry | задаёт compose / `.env` |
| `DEVICE_COLLECTOR_CONSUMER_GROUP` | consumer group | `device-collector` |
| `KAFKA_DEVICES_TOPIC`, `KAFKA_DEVICES_DLT_TOPIC` | входная тема и DLT | `devices`, `devices-dlt` |
| `DEVICES_DB_URL`, `DEVICES_DB_USERNAME`, `DEVICES_DB_PASSWORD` | логическая база `devices` в Proxy (пароль — `SHARDING_PROXY_PASSWORD`); `connectTimeout` и `socketTimeout` в URL compose берёт из двух переменных ниже | задаёт compose |
| `DEVICES_DB_CONNECT_TIMEOUT_S`, `DEVICES_DB_SOCKET_TIMEOUT_S` | `connectTimeout` и `socketTimeout` соединения сервиса с Proxy, секунды; сокет держать больше `SHARD_STATEMENT_TIMEOUT_MS` | `3`, `15` |
| `DEVICES_MAX_ATTEMPTS` | всего попыток записи, включая первую | `3` |
| `DEVICES_RETRY_INTERVAL` | пауза перед второй попыткой | `1s` |
| `DEVICES_RETRY_MULTIPLIER` | во сколько раз растёт каждая следующая пауза | `2.0` |
| `DEVICES_MAX_FUTURE_SKEW` | на сколько время события может опережать часы сервиса; позже - сообщение невалидно | `1d` |
| `DEVICES_SHARD_CACHE_TTL` | как долго сервис помнит шард устройства (определяется через `PREVIEW` в Proxy) | `5m` |
| `DEVICES_DB_POOL_SIZE` | размер пула соединений с Proxy | `5` |
| `DEVICES_DB_CONNECTION_TIMEOUT_MS` | сколько ждать свободное соединение из пула, мс | `2000` |
| `DEVICES_DLT_MAX_BLOCK_MS`, `DEVICES_DLT_REQUEST_TIMEOUT_MS`, `DEVICES_DLT_DELIVERY_TIMEOUT_MS` | продюсер `devices-dlt`: блокировка при отправке, ответ брокера, общий срок доставки, мс | `5000`, `5000`, `15000` |
| `REDIS_TIMEOUT`, `REDIS_PASSWORD`, `REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_NODES` | Redis через Sentinel: тайм-аут команд, пароль узлов, имя основного узла и узлы Sentinel (профиль `docker`) | `2s`; задаёт compose / `.env` |
| `DEVICE_CACHE_FLOOR_TTL` | срок ключа порога `device:floor:v1:{deviceId}`; тот же срок задаёт device-service | `24h` |
| `DEVICES_CONSUMER_METADATA_MAX_AGE_MS` | как часто потребитель `devices` обновляет метаданные: добавленный к теме раздел читается с начала не позже этого срока, мс | `60000` |
| `APP_INSTANCE_ID` | `instanceId` в метриках и логах | hostname |
| `MANAGEMENT_OTLP_ENDPOINT` | OTLP-экспорт трейсов | `http://alloy:4320/v1/traces` в compose |

Тайм-ауты запросов к шардам задаёт Proxy (`database-devices.yaml`: `connectionTimeoutMilliseconds`,
`statement_timeout`, `lock_timeout` в URL источников), значения берутся из `.env` и передаются контейнеру Proxy:

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `SHARD_STATEMENT_TIMEOUT_MS` | запрос дольше этого срока отменяет сам шард (отмена со стороны клиента через Proxy не работает) | `5000` |
| `SHARD_LOCK_TIMEOUT_MS` | ожидание блокировки на шарде | `3000` |
| `SHARD_CONNECTION_TIMEOUT_MS` | сколько Proxy ждёт соединение с шардом | `3000` |

По истечении этих сроков обработка устройства прерывается, а после `DEVICES_MAX_ATTEMPTS` попыток сообщение уходит в
`devices-dlt`. `connectTimeout` и `socketTimeout` соединения сервиса с Proxy - переменные `DEVICES_DB_CONNECT_TIMEOUT_S`
и `DEVICES_DB_SOCKET_TIMEOUT_S` (в `DEVICES_DB_URL`, собирает `docker-compose.yaml`). Остальные тайм-ауты, число попыток, паузы и их рост, пул - переменные из
таблицы выше; значения по умолчанию в `application.yaml` совпадают с `.env.example`. Фиксированы только значения,
на которых держится поведение сервиса: `max-poll-records: 1`, ручное подтверждение offset, `acks: all`,
`auto.register.schemas: false`.

### 19.4 Данные в PostgreSQL

Схема одинакова на каждом шарде (миграции db-migrator, раздел 20); шардируются по `device_id`:

* `devices` — текущее состояние устройства: последний применённый снимок (имя, ОС и её версия, прошивка, статус,
  настройки, атрибуты, сенсоры), `seen_count`, `first_seen_at`, `last_event_at`, `last_state_at`, мягкое удаление `deleted_at`, `version` (растёт при каждом изменении; по нему сбрасывается кэш), `managed_at` (время
  последней команды администратора), `deleted_by` (`ADMIN` у удаления администратором) — миграция `V3__device_management.sql`;
* `processed_device_events` — журнал обработанных событий `(device_id, event_key)` с исходом (`APPLIED`, `STALE`, `IGNORED`) и
  положением в Kafka (тема, раздел, offset): защита от повторной доставки. Журнал и устройство лежат на одном шарде
  (связанные таблицы Proxy), транзакция не распределённая.

### 19.5 Наблюдаемость

Метрики, логи и трейсы — в разделе 16: job `device-collector-service`, дашборд «IoT Platform — device-collector-service»,
метка `shard` в логах и метриках записи, трейс `devices process` продолжает трейс events-collector-service.

### 19.6 Пакеты

`com.github.gluhov.iotplatform.devicecollector`: `domain.model` (`DeviceRecord`, `DeviceChange`, `ProcessingOutcome`,
`FailureReason`, …), `domain.exception` (`InvalidDeviceInfoException` — без повтора, `DeviceNotStoredException`,
`StorageAccessException` — повтор), `domain.port.in`/`out` (`ProcessDeviceInfoUseCase`, `FindDeviceUseCase` /
`DeviceRepository`, `TransactionRunner`, `ShardResolver`, `DeviceMetrics`), `domain.service` (`DeviceRegistryService`,
`DeviceInfoValidator`, `DeviceInfoMapper`, `DeviceQueryService`), `adapter.in.kafka` (`DeviceInfoListener`),
`adapter.in.web` (`DeviceController`), `adapter.out.postgres` (`JdbcDeviceRepository`, `JdbcTransactionRunner`,
`ProxyShardResolver`), `adapter.out.redis` (`RedisDeviceCacheInvalidator`, Lua `raise-floor.lua`; порт `DeviceCacheInvalidator`), `config` (`KafkaConfig` — ошибки и DLT, `KafkaErrorClassifier`, `ProcessingProperties`,
`ObservationConfig`), `support` (`MetricsFacade`). Домен не зависит от Kafka и JDBC (`ArchitectureTest`).

### 19.7 Миграций в сервисе нет

В `services/device-collector-service` нет Flyway и DDL: схему создаёт только `db-migrator` (раздел 20).

### 19.8 Тесты

* Unit (142): `DeviceRegistryServiceTest` (исходы `APPLIED`/`STALE`/`DUPLICATE`, транзакция), `DeviceInfoValidatorTest`,
  `DeviceInfoMapperTest`, `DeviceInfoListenerTest` (ack только после записи, отправка в DLT), `KafkaConfigTest` (невалидное —
  сразу в `devices-dlt`; сбой хранилища — повторы и DLT; ошибка записи в DLT — не подтверждено; паузы 1 с и 2 с),
  `KafkaErrorClassifierTest`, `JdbcTransactionRunnerTest`, `ProxyShardResolverTest`, `SqlErrorsTest`,
  `ContractCompatibilityTest`, `DeviceControllerTest`, `MetricsFacadeTest`, `NoOffsetRecoveringConsumerTest`
  (раздел без смещения — в начало, выход за диапазон пробрасывается), `DevicesConsumerOffsetsTest`, `ArchitectureTest`.
* Интеграционные (23, Testcontainers — Kafka, Schema Registry, **два контейнера PostgreSQL и ShardingSphere-Proxy**,
  схема — через db-migrator): `ItDeviceCollectorTest` — устройство сохранено на ожидаемом шарде;
  `givenDeviceInfo_whenConsumed_thenStoredOnItsShard` проверяет **напрямую в шардах**, что запись есть на шарде,
  который вернул Proxy, и её нет на другом; много устройств распределяются по обоим шардам; повторное сообщение не
  меняет запись (`seen_count`), более старое не перезаписывает более новое; невалидное и нечитаемое сообщение — в
  `devices-dlt`, следующее обработано; остановленный шард не мешает устройствам другого; метрики; `health` = `DOWN`,
  когда Proxy недоступен.
* `make dcs-test`, `make dcs-it`, `make dcs-coverage` (порог 70 %).
* Быстрая проверка поднятого стенда — `make dcs-quick-check` (то же: `services/device-collector-service/scripts/quick-check.sh`
  из любого каталога или `./gradlew :services:device-collector-service:quickCheck`). Скрипт подбирает по правилу
  шардирования по одному устройству для каждого шарда, отправляет их в `devices` и для каждого ждёт сохранения
  (`CHECK_TIMEOUT`, по умолчанию 60 с), проверяет **напрямую в шардах**, что строка есть на ожидаемом шарде и её нет на
  другом, затем читает устройство через Proxy и REST (поле `shard`). Успех — `All checks passed`, иначе номер упавшего
  шага. Устройства `quick-check-*` остаются в реестре.

## 20. База данных реестра устройств: шарды, Proxy, миграции, реплики, резервные копии

### 20.1 Шарды и ShardingSphere-Proxy

* Два шарда PostgreSQL 18: контейнеры `postgres-shard-0` и `postgres-shard-1`, базы `shard_0` и `shard_1`,
  пользователь `SHARD_DB_USER`.
* **ShardingSphere-Proxy 5.5.3 — отдельный контейнер** `shardingsphere-proxy` (хост-порт `SHARDING_PROXY_PORT`, по
  умолчанию 3307). Конфигурация — в репозитории, `infrastructure/config/shardingsphere/`: `global.yaml` (пользователи) и
  `database-devices.yaml` (источники данных и правила шардирования). Это шаблоны: пароли и имена пользователей
  подставляет `render-config.sh` из переменных окружения при старте контейнера (Proxy 5.5.3 сам не раскрывает
  переменные окружения в YAML); в репозитории паролей нет.
* Приложение видит логическую базу `devices` как обычный PostgreSQL. Таблицы `devices` и `processed_device_events`
  шардируются по **`device_id`** стратегией по хешу: `shard_${(device_id.hashCode() % 2).abs()}` (алгоритм `INLINE`:
  `HASH_MOD` в 5.5.3 допустим только для autoTables, которые дают разные имена таблиц на шардах). Таблицы связаны
  (`bindingTables`): запись и журнал устройства всегда на одном шарде. Запрос по `device_id` уходит на один шард.
* В журнале Proxy (`sql-show: true`) для каждого запроса есть логический и фактический SQL с именем шарда:
  `Actual SQL: shard_0 ::: …` (`make logs S=shardingsphere-proxy`).
* Проверить маршрутизацию и выполнить SQL через Proxy: `make proxy-sql`, `make shard-of` (раздел 21).

### 20.2 Миграции — db-migrator (Flyway)

* Отдельная одноразовая задача `db-migrator` (образ из `services/db-migrator`), миграции —
  `services/db-migrator/src/main/resources/db/migration/devices/` (`V1__devices.sql` — таблицы `devices` и
  `processed_device_events`, `V2__device_profile.sql` — колонки `name`, `os_name`, `os_version` через
  `ADD COLUMN IF NOT EXISTS`). В device-collector-service миграций нет.
* **Как применяется к шардам:** одни и те же миграции выполняются **напрямую на каждый шард по очереди**, не через
  Proxy: DDL должен попасть на все узлы, а Proxy маршрутизирует запросы по ключу шардирования. После применения задача
  сверяет версию схемы на всех шардах; расхождение — код выхода 1, и стенд дальше не поднимается (Proxy и сервис ждут
  успешного завершения `db-migrator`). Подключение к только что стартовавшему шарду повторяется до ≈ 20 с.
* Повторный запуск применяет 0 миграций (Flyway пропускает применённые версии), изменённая задним числом миграция —
  ошибка `validate`. Правило для новых миграций: только вперёд, обратно совместимые (новые колонки необязательные),
  с `IF NOT EXISTS`.

### 20.3 Реплики шардов

* У каждого шарда есть **реплика только для чтения с потоковой репликацией**: `postgres-shard-0-replica` и
  `postgres-shard-1-replica`. Чтение направляется на реплики только у device-service: логическая база `devices_read`
  в Proxy (`infrastructure/config/shardingsphere/database-devices_read.yaml`) указывает лишь на реплики, а запись идёт через
  базу `devices` на primary (`database-devices.yaml`), поэтому device-service не обращается к основным узлам шардов (ADR-0004).
* Роль `replicator` (`LOGIN REPLICATION`, пароль `SHARD_REPLICATION_PASSWORD`) и физический слот `replica_N` на каждом шарде
  создаёт идемпотентная задача `shard-replication-init`; `config/postgres/pg_hba.conf` пускает `replicator` только в
  репликацию, обычный вход этой ролью отклонён. Реплика на первом старте снимает `pg_basebackup` с шарда
  (`init/shards/replica-entrypoint.sh`) и работает в режиме standby (`hot_standby=on`, `hot_standby_feedback=on`).
* `max_slot_wal_keep_size=1GB`: остановленная реплика не заполнит диск шарда — после 1 ГБ WAL слот отключается (реплику
  пересоздают, раздел 18).
* Проверка: `make replication-status` — на шарде клиент `walreceiver` в состоянии `streaming`, слот `replica_N`
  активен; на реплике режим восстановления (только чтение) и приёмник WAL `streaming`; код выхода 1 при сбое.
  Реплика отвергает запись (`SHOW transaction_read_only` = `on`), строка, записанная через Proxy, появляется на реплике
  своего шарда.

### 20.4 Резервное копирование и восстановление

* `make shard-backup` (`infrastructure/config/postgres/shard-backup.sh`) создаёт **копию каждого шарда** в
  `backups/<UTC-время>/` (каталог не в Git): `shard_0.dump`, `shard_1.dump` (`pg_dump`, формат custom) и
  `manifest.txt` — число строк `devices` и `processed_device_events`, версия схемы Flyway, SHA-256 файлов, до 5
  `deviceId` каждого шарда для проверки и позиции группы `device-collector` в теме `devices` **по каждому разделу**
  (подтверждённый offset, а у раздела без коммита - его начало). Не получилось прочитать Kafka - копия не создаётся.
  Копия снимается **с реплики**, сервисы не останавливаются; незаконченная копия остаётся `<время>.partial` и
  `shard-restore` её не возьмёт.
* `make shard-restore CONFIRM=yes [BACKUP=backups/<время>]` **удаляет данные шардов и их реплик** и восстанавливает из
  копии (без `CONFIRM=yes` — отказ). Порядок: проверка SHA-256, остановка device-collector-service и Proxy, чистые
  шарды и `pg_restore` одной транзакцией, пересоздание ролей, слотов и реплик, возврат позиций группы по всем
  разделам к моменту копии (в том числе у разделов, где коммитов тогда ещё не было, - к началу) и сверка с Kafka,
  запуск Proxy и сервиса; манифест без позиций или с другим набором разделов отвергается до остановки сервисов; устройства из manifest проверяются чтением через Proxy. Журнал `processed_device_events`
  отсекает повторы при повторной обработке сообщений после копии.
* Это логическая копия на момент запуска, а не восстановление на момент времени (PITR: base backup + архив WAL).

## 21. Как проверить, на какой шард попал `deviceId`

Правило: `shard_N`, где `N = abs(hashCode(deviceId) % 2)` (Java `String.hashCode()`, знак остатка у делимого, затем
модуль). Например, `dev-1` и `sensor-42` → `shard_1`, `abc` и `device-7` → `shard_0`. Практически — четырьмя способами:

**1. Через Proxy — до записи и после.** `PREVIEW` показывает, куда Proxy отправит запрос, не выполняя его:

```bash
make shard-of ID=dev-1
#  data_source_name |                         actual_sql
# ------------------+-------------------------------------------------------------
#  shard_1          | SELECT * FROM devices WHERE device_id = 'dev-1'
```

**2. Через REST device-collector-service** — в ответе поле `shard` (база шарда, вернувшая строку):

```bash
curl -s http://localhost:8095/api/v1/devices/<deviceId>     # порт — make urls (DEVICE_COLLECTOR_PORTS)
# {"deviceId":"…","shard":"shard_0","seenCount":2, …}       # 404 — устройства нет в реестре
```

**3. Напрямую в шардах** — запись есть на ожидаемом шарде и нет на другом (так же проверяет интеграционный тест):

```bash
set -a; source .env; set +a
ID=<deviceId>
for n in 0 1; do
  echo -n "shard_$n: "
  docker compose -f infrastructure/docker-compose.yaml exec -T postgres-shard-$n \
    psql -U "$SHARD_DB_USER" -d "shard_$n" -tA -c "SELECT count(*) FROM devices WHERE device_id = '$ID'"
done
# shard_0: 1
# shard_1: 0
```

**4. По логам.** device-collector-service пишет шард в каждую строку обработки (`shard`): в Loki
`{service="device-collector-service"} | json | deviceId="<deviceId>"`; Proxy — в журнале (`Actual SQL: shard_0 ::: …`).

Через Proxy можно посмотреть и сам реестр: `make proxy-sql SQL="SELECT device_id, seen_count, last_event_type FROM devices LIMIT 5"`.

## 22. device-service (управление устройствами)

REST-API устройств для сервисов-потребителей (например, API Orchestrator). `GET` читает карточку: Redis-кэш, при промахе — реплики шардов через логическую базу `devices_read` в Proxy (ADR-0004). `POST`, `PUT` и `DELETE` не пишут в базу: они публикуют команду `DeviceInfo` (`operation` = `CREATE` / `UPDATE` / `DELETE`) в тему `devices` и отвечают `202` после подтверждения Kafka. Команду применяет device-collector-service (раздел 19), поэтому изменение видно в `GET` не сразу. Принимаются только сервисные токены Keycloak (ADR-0007). Контракт — `services/device-service-client/src/main/openapi/device-service.yaml`; клиент публикуется `make ds-client-release` (необратимо, выполняет человек).

### 22.1 Запуск

device-service входит в базовый стенд вместе с Keycloak (realm `iot-platform` импортируется из `infrastructure/config/keycloak/iot-platform-realm.json`), Redis с Sentinel и двумя экземплярами сервиса:

```bash
make up                       # с трассами и метриками device-service: make up PROFILES=observability
make ds-quick-check           # токен, CRUD через REST, данные в реплике за 5 с, 401 без токена
docker compose --env-file .env -f infrastructure/docker-compose.yaml ps device-service
```

Порты на хосте берутся из диапазона `DEVICE_SERVICE_PORTS` (по умолчанию `8105-8106`). Порт узнавать командой, не числом; `--index` — номер экземпляра:

```bash
docker compose --env-file .env -f infrastructure/docker-compose.yaml port --index 1 device-service 8080
```

`make urls` порт device-service пока не выводит. Management-порт (`DEVICE_SERVICE_MANAGEMENT_PORT`, по умолчанию 8081) отдаёт `/actuator/health` и `/actuator/prometheus` без токена; на хост он не публикуется, доступен из сети Compose (healthcheck и Prometheus). На основном порту actuator не открыт.

### 22.2 Сервисный токен

Токен получают по client credentials. Клиенты realm `iot-platform`:

| Клиент | Секрет (переменная `.env`) | Роли |
|---|---|---|
| `device-service-consumer` | `DEVICE_SERVICE_CONSUMER_SECRET` | `devices.read`, `devices.write` |
| `device-service-reader` | `DEVICE_SERVICE_READER_SECRET` | `devices.read` |

Значения секретов — в вашем `.env` (в `.env.example` стоят заглушки `change_me_*`); в командах и документах их не пишут.

```bash
set -a; . ./.env; set +a
KC=http://localhost:${KEYCLOAK_PORT}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token
TOKEN=$(curl -s -X POST "$KC" -d grant_type=client_credentials -d client_id=device-service-consumer \
  -d client_secret="${DEVICE_SERVICE_CONSUMER_SECRET}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')
PORT=$(docker compose --env-file .env -f infrastructure/docker-compose.yaml port --index 1 device-service 8080 | sed 's/.*://')
API=http://localhost:${PORT}/api/v1/devices
```

Токен берётся с хоста по `localhost:${KEYCLOAK_PORT}`: `iss` должен совпадать с `KEYCLOAK_ISSUER_URI`. Аудитория — `device-service` (`DEVICE_SERVICE_AUDIENCE`).

### 22.3 Примеры запросов

```bash
# без токена -> 401
curl -s -o /dev/null -w '%{http_code}\n' "$API/dev-42"

# POST -> 202, заголовок Location: /api/v1/devices/dev-42; тело: deviceId, operation, commandId, acceptedAt
curl -s -i -X POST "$API" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"deviceId":"dev-42","tenantId":"t-1","deviceType":"router","status":"ACTIVE","name":"Роутер склада"}'

# GET -> 200 (повторять до 5 с: команда применяется асинхронно; до этого возможен 404)
curl -s "$API/dev-42" -H "Authorization: Bearer $TOKEN"

# тот же POST ещё раз -> 409 DEVICE_ALREADY_EXISTS

# PUT заменяет поля администратора целиком; tenantId, deviceType, status обязательны -> 202
curl -s -i -X PUT "$API/dev-42" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"tenantId":"t-1","deviceType":"router","status":"MAINTENANCE","name":"Роутер цеха"}'

# PUT без обязательных полей -> 400 VALIDATION_FAILED, поля перечислены в errors

# DELETE -> 202; после применения GET -> 404 DEVICE_NOT_FOUND
curl -s -i -X DELETE "$API/dev-42" -H "Authorization: Bearer $TOKEN"
```

| Код | Когда | `code` (Problem Details) |
|---|---|---|
| 202 | команда принята Kafka (POST, PUT, DELETE); `Location` указывает на ресурс | — |
| 200 | GET, устройство есть | — |
| 400 | тело не прошло проверку (в том числе `null` в `settings`, `attributes` или среди их значений) | `VALIDATION_FAILED` |
| 401 | нет токена, он недействителен или выдан для другой аудитории | `UNAUTHORIZED` |
| 403 | пользовательский токен, не сервисный | `SERVICE_TOKEN_REQUIRED` |
| 403 | сервисный токен без нужной роли (например, `device-service-reader` на POST) | `FORBIDDEN` |
| 404 | устройства нет или оно удалено | `DEVICE_NOT_FOUND` |
| 409 | POST для уже существующего устройства | `DEVICE_ALREADY_EXISTS` |
| 503 | Kafka не подтвердила команду за `DEVICE_COMMAND_SEND_TIMEOUT` (повтор безопасен) | `COMMAND_NOT_ACCEPTED` |
| 503 | GET: Proxy или реплика шарда недоступны при промахе кэша; PUT, POST, DELETE: реплика недоступна, а карточки в кэше нет (или недоступен и Redis) | `READ_MODEL_UNAVAILABLE` |

Тело запроса и `null`: `settings` и `attributes` — карты строк, `null` у самой карты и у любого её значения контракт не
допускает — `400 VALIDATION_FAILED` с полем в `errors[]`, команда не публикуется. Поля, которого нет в теле, — пустая
карта (в сообщении `devices` — пустые карты).

Предварительная проверка записи (PUT, POST, DELETE): есть ли устройство, решает реплика (`devices_read`). Реплика
недоступна — карточка ищется в кэше: нашлась — команда проходит проверку, не нашлась или Redis тоже недоступен —
`503 READ_MODEL_UNAVAILABLE`, команда не публикуется. Ответ по кэшу считает счётчик
`device_command_precheck_cache_total{operation,result=hit|miss|error}`; пока реплика доступна, кэш при записи не
читается. Проверка предварительная: окончательно решает владелец данных — device-collector-service.

### 22.4 Переменные окружения

Добавлены в `.env.example` относительно ветки `module-4-design`. Compose не подставляет значения по умолчанию, кроме двух: `COMPOSE_PROJECT_NAME` (по умолчанию `iot-platform`) и `DEVICE_COLLECTOR_CONSUMER_GROUP` (по умолчанию `device-collector`). Остальные перенесите в свой `.env`, иначе стенд не стартует. Значения-заглушки — в `.env.example`.

| Переменные | Назначение | Где описано |
|---|---|---|
| `DEVICE_SERVICE_IMAGE_TAG`, `DEVICE_SERVICE_PORTS` | тег образа; диапазон портов хоста (два экземпляра) | `.env.example`; plan.md, «Переменные окружения» |
| `DEVICE_SERVICE_MANAGEMENT_PORT` | management-порт (health, prometheus), не публикуется | `.env.example`; `infrastructure/config/alloy/config.alloy` (`sys.env`), healthcheck |
| `DEVICE_COMMAND_SEND_TIMEOUT` | ожидание подтверждения Kafka при записи | `.env.example`; раздел 22.3 |
| `SCHEMA_REGISTRY_HTTP_CONNECT_TIMEOUT_MS`, `SCHEMA_REGISTRY_HTTP_READ_TIMEOUT_MS`, `SCHEMA_REGISTRY_MAX_RETRIES` | клиент Schema Registry в device-service: соединение и чтение (мс), повторы (`1000`, `2000`, `0`) | `.env.example`; инвариант ниже |
| `DEVICE_CACHE_TTL` | срок кэша карточки устройства в device-service | `.env.example`; раздел 19.3 |
| `DEVICES_READ_DB_URL`, `DEVICES_READ_DB_USER`, `DEVICES_READ_DB_PASSWORD` | логическая база `devices_read` в Proxy | `.env.example`; ADR-0004 |
| `DEVICES_READ_DB_POOL_CONNECTION_TIMEOUT_MS`, `DEVICES_READ_DB_INIT_FAIL_TIMEOUT_MS` | тайм-аут получения соединения из пула (мс) и тайм-аут проверки при старте пула (`-1` — не ждать и не падать при старте) | `.env.example` |
| `SHARD_READER_PASSWORD` | пароль роли `device_reader` на шардах и реплике | `.env.example`; ADR-0004 |
| `KEYCLOAK_REALM`, `KEYCLOAK_ISSUER_URI`, `KEYCLOAK_JWK_SET_URI` | realm, `iss` токенов и адрес ключей подписи | `.env.example`; ADR-0007 |
| `DEVICE_SERVICE_AUDIENCE`, `JWT_CLOCK_SKEW` | аудитория токена; допустимое расхождение часов | `.env.example`; ADR-0007 |
| `KEYCLOAK_JWK_CONNECT_TIMEOUT`, `KEYCLOAK_JWK_READ_TIMEOUT` | тайм-ауты запроса ключей подписи | `.env.example` |
| `DEVICE_SERVICE_CONSUMER_SECRET`, `DEVICE_SERVICE_READER_SECRET` | секреты клиентов realm | `.env.example` (заглушки); раздел 22.2 |
| `REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_NODES` | имя основного узла и узлы Sentinel для всех клиентов Redis | `.env.example`; ADR-0008 |
| `REDIS_MASTER_IP`, `REDIS_REPLICA_IP`, `REDIS_SENTINEL_1_IP`, `REDIS_SENTINEL_2_IP`, `REDIS_SENTINEL_3_IP`, `REDIS_SENTINEL_IPS` | статические адреса узлов Redis и Sentinel в `iot-net` | `.env.example`; ADR-0008; раздел 22.6 |
| `REDIS_MAXMEMORY` | предел памяти узла Redis (вытеснение только ключей с TTL) | `.env.example`; ADR-0008 |
| `IOT_NET_SUBNET` | подсеть сети `iot-net` (свободная `/24`) | `.env.example`; раздел 22.6 |
| `IOT_NET_IP_RANGE` | диапазон динамических адресов контейнеров (верхняя половина подсети: `/25`); статические адреса Redis и Sentinel остаются в нижней | `.env.example` |
| `KAFKA_DEVICES_RETENTION_MS` | `retention.ms` темы `devices` без сжатия (`2592000000`) | `.env.example`; ADR-0011; раздел 5 |
| `DEVICE_CACHE_FLOOR_TTL` | срок ключа порога версии в кэше (`24h`, больше `DEVICE_CACHE_TTL`) | `.env.example`; ADR-0003; разделы 11, 19 |
| `DEVICES_CONSUMER_METADATA_MAX_AGE_MS` | обновление метаданных потребителя `devices` (`60000` мс): срок, за который работающая группа замечает добавленный раздел | `.env.example`; ADR-0011; раздел 5 |

Изменены значения: `KEYCLOAK_VERSION` — `26.0.8`; `SHARD_EXPORTER_PASSWORD` — заглушка с пробелом (ломала DSN экспортёра) заменена на `change_me_exporter`.

Инвариант: `KAFKA_PRODUCER_MAX_BLOCK_MS` ≤ `DEVICE_COMMAND_SEND_TIMEOUT`. Иначе продюсер может ждать метаданные Kafka дольше срока ожидания подтверждения, и `503 COMMAND_NOT_ACCEPTED` придёт позже `DEVICE_COMMAND_SEND_TIMEOUT`. В `.env.example` сейчас `KAFKA_PRODUCER_MAX_BLOCK_MS=5000` и `DEVICE_COMMAND_SEND_TIMEOUT=5s` — инвариант соблюдён.

Второй инвариант: `SCHEMA_REGISTRY_HTTP_CONNECT_TIMEOUT_MS` + `SCHEMA_REGISTRY_HTTP_READ_TIMEOUT_MS` (при `SCHEMA_REGISTRY_MAX_RETRIES=0`) < `DEVICE_COMMAND_SEND_TIMEOUT`. Первая команда экземпляра запрашивает у Schema Registry id схемы до передачи сообщения в Kafka, и срок `DEVICE_COMMAND_SEND_TIMEOUT` на этот запрос не действует; с настройками клиента по умолчанию (60 с, 60 с, 3 повтора) ответа не было и через 20 с (`ItSchemaRegistryDownTest` до правки). Дальше id схемы берётся из кэша. Сейчас `1000` + `2000` мс < 5 с.

Инварианты проверяют каждое ожидание отдельно. Если при первой команде экземпляра метаданные Kafka приходят почти через `KAFKA_PRODUCER_MAX_BLOCK_MS`, а затем медленно отвечает Schema Registry, ожидания складываются: `503` придёт позже `DEVICE_COMMAND_SEND_TIMEOUT` (с текущими значениями — до 5 + 3 с). Это известное ограничение: оба сбоя должны совпасть на первой команде после старта.

### 22.5 Цели make

| Цель | Что делает |
|---|---|
| `make ds-test` | unit-тесты device-service (Docker не нужен) |
| `make ds-it` | интеграционные тесты (Testcontainers, нужен Docker) |
| `make ds-coverage` | все тесты, порог покрытия, HTML-отчёт |
| `make ds-quick-check` | проверка поднятого стенда (`services/device-service/scripts/quick-check.sh`) |
| `E2E_NEXUS_ACCEPT_EULA=true ./gradlew :services:device-service:e2eTest` | сквозной тест `ItEndToEndTest`: Testcontainers поднимает полный стенд (проект `iot-e2e`) из общего Compose-файла, затем POST → PUT → DELETE с чтением на другой реплике, изменение видно за ≤ 5 с. В `check` не входит. Перед запуском: `make down` (фиксированные имена и порты), образы собраны (`make up`), нужен JDK 25 и Docker Compose v2; `E2E_NEXUS_ACCEPT_EULA=true` — принятие лицензии одноразового Nexus теста |
| `make ds-client-check` | сборка и тесты клиента `device-service-client`, без публикации |
| `make ds-client-release` | публикация клиента в Nexus; необратимо |
| `make redis-cli` | `redis-cli` к текущему основному узлу (адрес у Sentinel) |
| `make dcs-quick-check` | проверка device-collector-service (раздел 19.8) |

### 22.6 Эксплуатация Redis и Proxy

* **Redis (ADR-0008).** Основной узел выбирает Sentinel (кворум 2); клиенты узнают его адрес у Sentinel и переподключаются без перезапуска сервисов. Репликация асинхронная: при переключении последние записи могут потеряться — для кэша это промах, для аренды outbox — повторная аренда. После переключения узлы Redis **не пересоздавать**: роль узла при старте он спрашивает у Sentinel (`infrastructure/config/redis/redis-entrypoint.sh`).
* **Proxy (ADR-0004).** При запуске Proxy подключается ко всем источникам обеих логических баз, поэтому не стартует, если реплика шарда недоступна. Правило эксплуатации: при неисправной реплике **не перезапускать Proxy**; сначала вернуть реплику (`make replication-status`). Пока Proxy работает, остановка реплики не задевает запись; чтение по её шарду даёт `503 READ_MODEL_UNAVAILABLE`.
* **Сеть `iot-net` (обновление с предыдущей версии).** Подсеть и статические адреса Redis и Sentinel задаёт `IOT_NET_SUBNET`; существующую сеть Compose не пересоздаёт. При первом обновлении:

  ```bash
  make down                    # останавливает контейнеры всех профилей; тома сохраняются
  docker network rm iot-net
  make up
  ```

  Тома не удаляются. `make clean` и `docker compose down -v` для этого не нужны. Подсеть `IOT_NET_SUBNET` не должна пересекаться с сетями хоста.

### 22.7 Типовые проблемы

| Симптом | Что проверить |
|---|---|
| 401 с токеном от `device-service-consumer` | `iss` не совпадает с `KEYCLOAK_ISSUER_URI`: токен брать с хоста по `localhost:${KEYCLOAK_PORT}` (раздел 22.2); срок токена истёк — получить новый |
| 403 `SERVICE_TOKEN_REQUIRED` | токен пользовательский; нужен client credentials |
| 403 `FORBIDDEN` на POST / PUT / DELETE | клиент `device-service-reader` — для записи нужен `device-service-consumer` |
| 503 `COMMAND_NOT_ACCEPTED` | Kafka или Schema Registry не отвечают: `make logs S=device-service`, `make health`; повтор безопасен |
| 503 `READ_MODEL_UNAVAILABLE` | Proxy или реплика шарда: `make replication-status`, `make logs S=shardingsphere-proxy` |
| команда не видна в `GET` долго | `make logs S=device-collector-service`; `make dcs-quick-check` |
| контейнер не `healthy` | `make logs S=device-service`; healthcheck — liveness на management-порту |

Метрики: `device_commands_*` и `device_cache_*` (без `deviceId` в метках) — job `device-service` (собирает Alloy, ADR-0012) в Prometheus и дашборд «IoT Platform — device-service» (`infrastructure/config/grafana/dashboards/device-service.json`).

Окружение платформы (C4, Postgres/Camunda/MinIO) описано в истории Git и в профиле `platform`.

### 22.8 Известные ограничения

* **Keycloak по HTTP, `sslRequired: none`.** Стенд локальный (одна машина или доверенная сеть, HTTP, без сертификата), поэтому
  в `infrastructure/config/keycloak/iot-platform-realm.json` задано `"sslRequired": "none"`. По умолчанию (`external`)
  Keycloak пропускает HTTP только с частных адресов, а адрес, с которого Docker доставляет запрос с хоста в контейнер, от
  проекта не зависит (Docker Desktop, VPN) — отсюда `403 HTTPS required`. Интеграционные тесты переводят в `none` и realm
  `master` (`kcadm.sh update realms/master -s sslRequired=NONE`; он нужен только им для административного токена), и
  realm, который создают сами. Нелокальному развёртыванию нужны сертификат и `external`.

* **Коды ошибок для 405, 406, 415 и неизвестного пути.** Эти ответы отдаются с кодом `VALIDATION_FAILED`, хотя в контракте для них кода нет. Решение сознательное: контракт OpenAPI не меняется, клиенты получают Problem Details с известным кодом. Если понадобится отдельный код, сначала меняется контракт (и клиент `device-service-client`).
* **`deviceId` с пробелом или `/` из событий роутера.** device-collector-service принимает любой непустой `deviceId` до 128 символов, а device-service — только без пробелов и `/` (шаблон контракта; `/` ломает путь `/api/v1/devices/{deviceId}`). Такое устройство есть в реестре, но `GET`, `PUT` и `DELETE` отвечают на него `400 VALIDATION_FAILED`, создать его через `POST` тоже нельзя (ADR-0006).
