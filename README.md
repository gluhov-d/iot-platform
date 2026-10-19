# IoT Platform — events-collector-service

Учебная микросервисная платформа сбора телеметрии IoT-устройств.
Модуль 2: сервис **events-collector-service** — приём событий из Kafka, хранение в ClickHouse
и публикация производного потока устройств через outbox.

---

## 1. Назначение сервиса

`events-collector-service`:

1. читает события устройств из Kafka-темы `events` (Avro `DeviceEvent`, Schema Registry);
2. проверяет обязательные поля;
3. сохраняет каждое событие в ClickHouse (`iot_platform.device_events`);
4. создаёт outbox-запись (`iot_platform.device_outbox`) — намерение опубликовать устройство;
5. подтверждает offset **только после** записи в обе таблицы;
6. фоновой задачей публикует `DeviceInfo` в compacted-тему `devices` — один активный публикатор
   через Redis-аренду, учёт опубликованных устройств в Redis;
7. отдаёт метрики Prometheus, JSON-логи и трассировки OTLP.

### Гарантии

| Что | Гарантия |
|---|---|
| приём события в ClickHouse | at-least-once |
| публикация `deviceId` в `devices` | at-least-once |
| дубликаты | допустимы (гасятся `ReplacingMergeTree` и `FINAL`; потребители `devices` обязаны быть идемпотентными) |
| потеря валидного входного события | недопустима |

Общей транзакции между Kafka, ClickHouse и Redis нет, поэтому порядок шагов выбран так,
чтобы любой сбой приводил к **повтору**, а не к потере:

* `insert device_events` → `insert device_outbox` → `ack`. Падение до ack — сообщение будет прочитано
  снова, повторные строки схлопнутся по ключу сортировки.
* Начальная версия outbox — константа `state_version = 1`. Повторная доставка уже опубликованного
  события вставит `v1 NEW`, которая проиграет существующей `v2 SENT`, и состояние не откатится.
* Публикация: `send` (синхронно, `acks=all`) → `SADD devices:published` → версия `SENT`.
  Падение между шагами даёт повторную отправку, но устройство не может оказаться «опубликованным»
  без сообщения в Kafka.

**Невалидные и недесериализуемые сообщения** (не Avro, схемы нет в Schema Registry, не прошли валидацию)
не повторяются: обработчик ошибок откладывает их в тему `events.DLT` — недесериализуемые с исходными байтами,
невалидные как `DeviceEvent` в Avro, с заголовками `kafka_dlt-*` (исходные тема, раздел, offset, класс и текст
ошибки) — и только после успешной записи в DLT подтверждает их offset. Не удалось записать в DLT — сообщение
остаётся неподтверждённым и обрабатывается снова: раздел не идёт дальше, пока сообщение не сохранено. Лог —
со `status=REJECTED`; разбор и повторная обработка DLT — вне рамок модуля.

**Временные ошибки** (ClickHouse недоступен; Schema Registry не ответил при десериализации — таймаут, обрыв
соединения, ответ 5xx) повторяются без ограничения числа попыток с экспоненциальной паузой до 10 с: раздел ждёт
восстановления, offset за сообщением не подтверждается. Что повторять, а что откладывать в DLT, решает
`KafkaErrorClassifier`.

## 2. Место сервиса в платформе

```
устройства → Device Collector → [events] → events-collector-service → ClickHouse
                                                     │
                                                     └─ outbox → [devices] → Device Service и др.
```

Целевая архитектура платформы — C4-диаграммы `diagrams/context.puml` и `diagrams/containers.puml`
(на контейнерной диаграмме сервис — `events-collector-service`, маршруты T4–T7).
Устройство сервиса и его зависимостей — Mermaid-диаграммы (раздел 17).

## 3. Стек и версии

| Компонент | Версия |
|---|---|
| Java | 25 (Gradle toolchain, JDK скачивается автоматически через foojay) |
| Spring Boot | 4.0.3 (Spring Framework 7.0, Spring for Apache Kafka 4.0) |
| Apache Kafka | 4.2.0, KRaft (`apache/kafka:4.2.0`) |
| Kafka UI (профиль `tools`) | kafbat/kafka-ui v1.5.0 |
| Confluent Schema Registry | 8.2.4 |
| ClickHouse | 26.2.19 (`clickhouse/clickhouse-server:26.2.19-alpine`, на Apple M4 — `linux/amd64`, см. «Типовые проблемы»), JDBC `clickhouse-jdbc` 0.9.9 |
| Redis Open Source | 8.6.7 |
| Avro | 1.12.1 |
| Lombok | 1.18.42 (версию задаёт Spring Boot) |
| Docker Compose | v2, Compose Specification (без поля `version`) |
| Prometheus / Grafana / Loki / Tempo / Alloy | v2.54.1 / 11.2.0 / 3.1.1 / 2.6.0 / v1.4.2 |
| kafka-exporter | v1.9.0 |
| Testcontainers | 2.x (версию задаёт Spring Boot) |
| JaCoCo | 0.8.14 |

Ни один образ не использует тег `latest`. Все версии — в `.env.example`.

## 4. Структура репозитория

```
iot-platform/
├── contracts/avro/                    # Avro-контракты — единый источник для кодогенерации
│   ├── device-event.avsc
│   └── device-info.avsc
├── diagrams/
│   ├── component-events-collector.mmd # компоненты сервиса и окружение
│   ├── sequence-ingest.mmd            # приём события
│   ├── sequence-outbox-publish.mmd    # публикация outbox
│   └── context.puml, containers.puml  # C4 платформы + PNG
├── docs/
│   ├── definition-of-done.md          # критерии приёмки со ссылками на файлы и тесты
│   └── screenshots/                   # дашборд Grafana и трейсы Tempo (раздел 22 задания)
├── infrastructure/
│   ├── docker-compose.yaml            # стенд: базовый + профили observability, platform
│   ├── .env -> ../.env                # симлинк на корневой .env (сам .env в Git не хранится)
│   ├── init/
│   │   ├── kafka/init-topics.sh       # создание тем events, devices, events.DLT (контейнер kafka-init)
│   │   ├── clickhouse/01-schema.sh    # база iot_platform, таблицы device_events и device_outbox
│   │   ├── clickhouse/02-audit.sh     # таблица аудита публикаций device_publication_audit
│   │   └── postgres/                  # профиль platform (окружение модуля 1)
│   └── config/
│       ├── prometheus/prometheus.yml
│       ├── grafana/{provisioning,dashboards}
│       ├── loki/loki-config.yaml
│       ├── tempo/tempo.yaml
│       └── alloy/config.alloy
├── services/events-collector-service/ # модуль многомодульной Gradle-сборки
│   ├── build.gradle.kts, lombok.config
│   ├── Dockerfile                     # сборка из корня репозитория (нужны contracts/ и корневой Gradle)
│   ├── scripts/check-local.sh         # быстрая проверка стенда
│   └── src/{main,test}/...
├── settings.gradle.kts, gradlew       # корень сборки: include("services:events-collector-service")
├── Makefile
├── .env.example
└── README.md
```

**Расположение относительно задания.** Раскладка `infrastructure/` сохранена из модуля 1, поэтому часть путей
отличается от разделов 5 и 22 задания:

| В задании | В репозитории |
|---|---|
| `infrastructure/compose.yaml` (раздел 14) | `infrastructure/docker-compose.yaml` (как в разделе 22) |
| `infrastructure/kafka/init-topics.sh` | `infrastructure/init/kafka/init-topics.sh` |
| `infrastructure/clickhouse/init/01-create-database.sql`, `02-device-events.sql`, `03-device-outbox.sql` | `infrastructure/init/clickhouse/01-schema.sh` — тот же DDL из раздела 8 одним скриптом; имя базы — из `CLICKHOUSE_DB` (по умолчанию `iot_platform`); в `device_outbox` по согласованию с преподавателем добавлены `trace_id` и `span_id` для сквозного трейса |
| `infrastructure/{prometheus,grafana,loki,tempo,alloy}/...` | `infrastructure/config/{prometheus,grafana,loki,tempo,alloy}/...` |
| `services/events-collector-service/settings.gradle.kts`, свой `gradlew` | многомодульная сборка: `settings.gradle.kts` и `gradlew` в корне |
| `application.yml`, `application-local.yml`, `application-docker.yml` | те же профили с расширением `.yaml` |
| namespace Avro `com.proselyte.platform.events.avro` | `com.github.gluhov.iotplatform.events.avro` (поля и типы схем — как в задании) |

Пакеты сервиса (`com.github.gluhov.iotplatform.eventscollector`):

| Пакет | Содержимое |
|---|---|
| `domain.model` | `DeviceEventRecord`, `OutboxRecord` (версии и переходы), `OutboxStatus`, `OutboxPolicy`, `PublishOutcome`, `BatchResult`, `PublicationAuditRecord` |
| `domain.exception` | `InvalidEventException` (не повторяется), `EventPersistenceException` (повторяется) |
| `domain.port.in` / `out` | `IngestEventUseCase`, `PublishOutboxUseCase` / репозитории (события, outbox, аудит), аренда, учёт устройств, публикатор, метрики |
| `domain.service` | `EventIngestionService`, `OutboxPublishingService`, `DeviceEventValidator`, `DeviceEventMapper` |
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
| `devices` | производный поток обнаруженных устройств | `deviceId` | `compact` | 3 | 1 |
| `events.DLT` | отложенные сообщения `events`, которые нельзя обработать (раздел — тот же, что у исходного) | `deviceId` | `delete` | 3 | 1 |

* Ключ `deviceId` обязателен: события одного устройства попадают в один раздел и читаются по порядку.
  Для compacted-темы сообщение без ключа брокер отклонит.
* Compaction оставляет последнее значение по ключу, но не заменяет идемпотентность потребителей.
* Темы создаёт одноразовый контейнер `kafka-init` скриптом `infrastructure/init/kafka/init-topics.sh`
  после `kafka: service_healthy`; сервис стартует только после `kafka-init: service_completed_successfully`.
  Автосоздание тем брокером выключено (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`).

## 6. Avro-контракты

| Файл | Запись | Поля |
|---|---|---|
| `contracts/avro/device-event.avsc` | `DeviceEvent` | `eventId`, `deviceId`, `timestampMs` (Unix ms), `eventType` |
| `contracts/avro/device-info.avsc` | `DeviceInfo` | `deviceId`, `emittedAtMs` (время публикации) |

Namespace — `com.github.gluhov.iotplatform.events.avro`. Классы `SpecificRecord` генерирует задача Gradle
`generateAvro` прямо из `contracts/avro` (схемы не копируются). `stringType = String`: иначе поля были бы
`CharSequence`, при чтении приходил бы `org.apache.avro.util.Utf8` и `equals` со строкой ломался бы.

Сериализация — `KafkaAvroSerializer`/`KafkaAvroDeserializer` (Schema Registry, `specific.avro.reader=true`),
схемы регистрируются автоматически в субъектах `events-value` и `devices-value`, совместимость — `backward`.

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
  итерацию нового владельца. Если Kafka уже подтвердила отправку, устройство есть в `devices:published`, и новый
  владелец запишет SENT как пропуск.
* Освобождение (в `finally`) — фоновое продление останавливается, затем Lua-скрипт удаляет ключ, только если
  значение совпадает с токеном. Если аренда истекла и её взял другой экземпляр, старый владелец чужую аренду не удалит.
* `SADD devices:published` — **только после** подтверждения отправки в Kafka.
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

У сервиса нет `container_name` и фиксированного хост-порта (диапазон `EVENTS_COLLECTOR_PORTS`), Prometheus
находит реплики через DNS (`dns_sd_configs`), `instanceId` по умолчанию — hostname контейнера.
У остальных контейнеров стенда имена фиксированные (`iot-kafka`, `iot-clickhouse`, …).

## 10. Запуск инфраструктуры

Нужны Docker (Compose v2) и ≥ 4 ГБ памяти для базового стенда (≥ 6 ГБ с профилями).

```bash
cp .env.example .env          # при необходимости поменяйте пароли и порты

# базовый стенд: kafka, kafka-init, schema-registry, clickhouse, redis, events-collector-service
docker compose -f infrastructure/docker-compose.yaml up -d

# со стеком наблюдаемости: + prometheus, grafana, loki, tempo, alloy, kafka-exporter
docker compose -f infrastructure/docker-compose.yaml --profile observability up -d
```

То же через Makefile — с ожиданием healthy и понятным кодом выхода:

```bash
make up                       # базовый стенд
make up-obs                   # с observability
make up PROFILES=tools        # плюс Kafka UI (http://localhost:8090)
make up PROFILES="observability platform"   # плюс окружение модуля 1 (Postgres, Keycloak, Camunda, MinIO)
make health | make topics | make urls | make logs S=events-collector-service
make down                     # остановить (тома сохраняются)
make clean                    # остановить и удалить тома
```

Профиль `platform` — окружение модуля 1, в модуле 2 не используется.

## 11. Запуск сервиса из IDE

1. Поднять стенд без сервиса:
   `docker compose -f infrastructure/docker-compose.yaml up -d kafka kafka-init schema-registry clickhouse redis`
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

## 12. Запуск через Docker Compose

Сервис входит в базовый профиль и собирается из `services/events-collector-service/Dockerfile`
(контекст — корень репозитория). Пересобрать после изменений кода:

```bash
docker compose -f infrastructure/docker-compose.yaml up -d --build events-collector-service
```

В контейнере активен профиль `docker`, адреса зависимостей — внутренние (`kafka:9092`, `clickhouse:8123`,
`redis:6379`), healthcheck — `/actuator/health`.

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

## 14. Модульные тесты

```bash
./gradlew :services:events-collector-service:test     # или make test; Docker не нужен
```

Классы `*Test` (70 тестов). Ключевые сценарии:

* `EventIngestionScenarioTest` — корректное событие, пустые `eventId`/`deviceId`, некорректный `timestampMs`,
  ошибки записи в `device_events` и `device_outbox`: проверяется, вызван ли `ack.acknowledge()`; спан приёма
  сохраняется в outbox-записи;
* `OutboxPublishingServiceTest` — новое устройство (порядок send → SADD → SENT), уже опубликованное,
  ошибка Kafka (FAILED, attempts + 1), аренда не получена, освобождение аренды при ошибке,
  продление аренды перед каждой записью (в том числе первой) и перед каждой записью версии; потеря аренды после
  выборки, посреди пачки, во время отправки, после ошибки Kafka и при ошибке Redis — пачка останавливается, версия
  и аудит не пишутся; отправка внутри трейса приёма, запись аудита (`PUBLISHED` с номером попытки, `SKIPPED`,
  `FAILED` с ошибкой) и публикация при недоступном аудите;
* `LeaseGuardTest` — фоновое продление на настоящем планировщике с коротким TTL, обнаружение чужой аренды
  (дальше Redis не вызывается), ошибка Redis — потеря аренды, `close()` останавливает продление;
* `OutboxRecordTest` — версии записи: повторная доставка, обрезка ошибки, перенос контекста трассировки;
* `RedisLeaseServiceTest` — освобождение и продление аренды владельцем, попытка освободить или продлить чужую аренду;
* `KafkaErrorClassifierTest` — таймаут и ответ 5xx Schema Registry — временные (повтор), не Avro и неизвестная
  схема — «ядовитые» (DLT), ошибка хранилища — повтор;
* `MetricsFacadeTest` — имена метрик в формате Prometheus;
* `ArchitectureTest` (ArchUnit, 9 правил) — слои и направления зависимостей: домен без Kafka, JDBC и Redis,
  адаптеры вызывают домен только через `port.in`, `@KafkaListener` и `@Scheduled` — только во входных адаптерах,
  порты `out` реализуют выходные адаптеры, между пакетами нет циклов.

## 15. Интеграционные тесты

```bash
./gradlew :services:events-collector-service:integrationTest   # или make integration-test; нужен запущенный Docker
./gradlew :services:events-collector-service:check             # все тесты + порог покрытия 65 % (make coverage)
```

Классы `It*Test` (17 тестов). Kafka, Schema Registry, ClickHouse и Redis поднимаются Testcontainers
(`config/*TestContainerConfig`, базовый класс `it/AbstractItTest`), заранее запущенный compose не нужен.
Используются те же `infrastructure/init/clickhouse/01-schema.sh` и `infrastructure/init/kafka/init-topics.sh`,
что и на стенде.

* `ItEventsCollectorTest` — приём события; публикация outbox и пропуск повторной публикации; повторная доставка
  того же `eventId`; ошибка публикации, повторная попытка и остановка после `OUTBOX_MAX_ATTEMPTS`; строки аудита
  каждой попытки (исход, номер попытки, `instance_id`); `DeviceInfo` уходит в трейсе приёма (`traceId` из
  `traceparent` совпадает с `trace_id` в outbox); аренда истекает во время `findPending` и её берёт другой
  экземпляр — ничего не отправлено, новой версии outbox и аудита нет, аренда нового владельца не тронута;
* `ItKafkaErrorHandlingTest` — ошибки приёма на отдельной теме с одним разделом и своей consumer group, после
  каждой проверяется committed offset: однократный таймаут Schema Registry (`SocketTimeoutException`) — сообщение
  повторено и сохранено, offset подтверждён после обоих сообщений; не-Avro сообщение — в DLT с исходными байтами,
  его offset подтверждён сразу после записи в DLT; невалидное событие — в DLT в Avro; следующее корректное
  сообщение всякий раз обработано;
* `ItRedisLeaseServiceTest` — захват, освобождение владельцем, попытка удалить чужую аренду, истечение TTL,
  продление своей аренды, отказ продлить чужую или истёкшую; `LeaseGuard` удерживает аренду дольше TTL и замечает,
  что её взял другой экземпляр.

Покрытие строк (JaCoCo, unit + integration): порог 65 %, фактически около 98 %; отчёт —
`services/events-collector-service/build/reports/jacoco/test/html/index.html`.
Сгенерированные Avro-классы и код Lombok исключены из отчёта.

## 16. Наблюдаемость

| Что | Где |
|---|---|
| Health | http://localhost:&lt;порт&gt;/actuator/health |
| Метрики | http://localhost:&lt;порт&gt;/actuator/prometheus |
| Grafana | http://localhost:3000 → папка «IoT Platform» → «IoT Platform — events-collector-service» |
| Prometheus | http://localhost:9090 (job `events-collector-service`) |
| Loki | Grafana → Explore → Loki: `{service="events-collector-service"} \| json` |
| Tempo | Grafana → Explore → Tempo: `{name="publish devices"}` (публикация), `{name="events process"}` (приём) |
| Kafka UI | http://localhost:8090 (профиль `tools`): темы, сообщения Avro как JSON, лаг consumer group, схемы |

Порт сервиса на хосте Docker выбирает из диапазона `EVENTS_COLLECTOR_PORTS` (по умолчанию 8085–8087,
диапазон нужен для `--scale`). Фактический порт: `make urls` или
`docker compose -f infrastructure/docker-compose.yaml port events-collector-service 8080`.

Наружу открыты только `/actuator/health`, `/actuator/metrics`, `/actuator/prometheus`; бизнесовых REST-точек нет.

**Метрики:** `events_processed_total`, `events_persist_fail_total`, `outbox_candidate_created_total`,
`outbox_publish_success_total`, `outbox_publish_fail_total`, `outbox_publish_skipped_total`,
`outbox_pending_count`, а также стандартные JVM, HikariCP и `spring_kafka_listener_seconds`.

> **Про `outbox_candidate_created_total`.** Spring Boot 4 использует Prometheus Java client 1.x, который
> запрещает имена метрик с суффиксом `_created` (он зарезервирован в OpenMetrics под время создания
> счётчика). Поэтому `/actuator/prometheus` отдаёт этот счётчик как `outbox_candidate_total`, а требуемое
> имя `outbox_candidate_created_total` восстанавливает `metric_relabel_configs` в
> `infrastructure/config/prometheus/prometheus.yml`. В Prometheus и Grafana метрика называется так, как
> требует задание. Остальные шесть имён совпадают уже в `/actuator/prometheus` (проверяет `MetricsFacadeTest`).

**Логи** — JSON (формат logstash) в stdout, Alloy отправляет их в Loki. Поля: `service`, `instanceId`,
`traceId`, `spanId`, `eventId`, `deviceId`, `topic`, `partition`, `offset`, `status`, `errorClass`,
`errorMessage`. Значения `status`: `PROCESSED`, `REJECTED`, `FAILED` (приём), `PUBLISHED`, `SKIPPED`,
`FAILED`, `ERROR` (публикация). В Loki `level` — метка, `trace_id`/`event_id`/`device_id` — structured
metadata (не метки, чтобы не раздувать индекс). Секреты и строки подключения в логи не пишутся.
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
| `tempo-trace-end-to-end.png` | сквозной трейс успешного пути: приём (`events process`) и публикация (`outbox publish` — дочерний спан) в одном трейсе |
| `tempo-trace-skip.png` | пропуск публикации для уже опубликованного устройства — в трейсе своего события: `outbox publish` → `redis devices:published check` → `append SENT version` → `append audit record`, без отправки в Kafka |

## 17. Диаграммы

* `diagrams/component-events-collector.mmd` — компоненты сервиса, Kafka, ClickHouse, Redis, наблюдаемость;
* `diagrams/sequence-ingest.mmd` — последовательность приёма события и ветки ошибок;
* `diagrams/sequence-outbox-publish.mmd` — последовательность публикации outbox;
* `diagrams/containers.puml` (+ `Containers.png`, `make diagrams`) — C4 платформы.

GitHub показывает `.mmd` сразу; в IntelliJ IDEA — плагин Mermaid; также можно вставить на https://mermaid.live.

## 18. Типовые проблемы

| Проблема | Что проверить |
|---|---|
| Schema Registry недоступен | `make health`; `curl localhost:8081/subjects`; у сервиса `SCHEMA_REGISTRY_URL` должен указывать на `http://schema-registry:8081` внутри сети compose и на `http://localhost:8081` из IDE; SR стартует после `kafka: healthy` |
| Kafka-темы не создались | `docker compose -f infrastructure/docker-compose.yaml logs kafka-init` (код выхода — `make health`); скрипт `infrastructure/init/kafka/init-topics.sh` должен быть исполняемым; `make topics` — есть ли `events`/`devices` и `cleanup.policy`. `--if-not-exists` не меняет настройки существующей темы — исправить `kafka-configs.sh --alter` или `make kafka-reset` |
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
| сообщения events не читаются, лаг растёт | логи сервиса со `status=FAILED` — временная ошибка ClickHouse или Schema Registry, раздел ждёт с backoff до 10 с; после восстановления чтение продолжится автоматически. Раздел стоит и тогда, когда сообщение не удаётся отложить в DLT — в логе «Dead-letter publication … failed», проверить тему `events.DLT` |
| сообщения в `events.DLT` | входное сообщение не десериализовалось или не прошло валидацию; причина — в заголовках `kafka_dlt-exception-*` (Kafka UI, профиль `tools`) и в логе со `status=REJECTED`; после исправления источника сообщение можно отправить заново |
| лаг consumer group отрицательный (`-1`) | у группы ещё нет подтверждённого offset в разделе (в раздел не приходили сообщения) — kafka-exporter отдаёт `-1`; после первого обработанного сообщения лаг станет 0 |
| порт занят | поменять `*_PORT` / `EVENTS_COLLECTOR_PORTS` в `.env` |
| ClickHouse падает (`Exited (132)`, в `clickhouse-server.err.log` — `Illegal instruction`) на `SELECT ... FINAL` | Apple M4 + Docker Desktop: arm64-сборка ClickHouse 26.2 получает SIGILL, amd64-образ через Rosetta работает. В `.env` задано `CLICKHOUSE_PLATFORM=linux/amd64`; интеграционные тесты на macOS с Apple Silicon тоже берут amd64 (переопределяется переменной `CLICKHOUSE_PLATFORM`). На Linux/x86 amd64 — нативная платформа |
| `No space left on device` в логах Kafka/ClickHouse | заполнен диск VM Docker (`docker system df`) — освободить место или увеличить Disk usage limit в Docker Desktop, затем пересоздать том Kafka (`make kafka-reset`): брокер помечает каталог логов как сбойный |

Модуль 1 (C4, окружение Postgres/Keycloak/Camunda/MinIO) описан в истории Git и в профиле `platform`.
