# Tasks: failed-events — обработка ошибок и архив в объектном хранилище

**Input**: [spec.md](spec.md), [plan.md](plan.md), [data-model.md](data-model.md), [research.md](research.md),
[failed-event.avsc](contracts/failed-event.avsc), [quickstart.md](quickstart.md), правила агента — [AGENTS.md](../../AGENTS.md).

Формат: `- [ ] TNNN [P?] [Mac?] действие — файлы (требования)`. `[Mac]` — задача нужна полному стенду, см. «Место выполнения». `[P]` — можно параллельно: другие файлы, нет зависимости от
незавершённых задач. Тест пишется раньше кода, который он проверяет, и сначала падает. **ЧЕЛОВЕК** — шаг выполняет
человек: агент останавливается и ждёт. `(Rn)` — задача проверяет предположение research.md: не подтвердилось — остановиться и
сообщить. Решения со статусом «Проверено» в research.md (R1–R15, R17, R19) в задачах записаны как требования; «Частично» и
«Предположение» (R16, R18, R20–R23) проверяют задачи с пометкой `(Rn)`.

**Место выполнения.** По умолчанию — VPS. Задачи с пометкой `[Mac]` нужны полному стенду (observability, снимки, приёмка, e2e, arm64) и выполняются на стенде владельца (macOS-хост, arm64).

**Пакеты для исполнителей.** Файлы пакетов не пересекаются; `docker-compose.yaml`, `.env.example`, `Makefile` в каждый момент
правит один владелец, по порядку P2 → P6 → P7 → P8. Пакеты P3, P4, P5 их не трогают: новые переменные называют в отчёте
(имена уже зафиксированы в plan.md и внесены P2). Порядок: P1 → P2 ‖ P3 (unit) ‖ P4 (unit) ‖ P5 → IT P3 и IT P4 (после P5) → P6 → P7 → P8 → P9 (необязательно) → converge.
Отчёт исполнителя: изменённые файлы, отмеченные `[X]`, RED → GREEN, числа тестов из `build/test-results/**/*.xml`,
вывод команд Checkpoint, «не проверено».

Сокращения путей:

| Сокращение | Путь |
|---|---|
| `AV` | `avros` |
| `ECS` | `services/events-collector-service` |
| `ECSJ` | `ECS/src/main/java/com/github/gluhov/iotplatform/eventscollector` |
| `ECST` | `ECS/src/test/java/com/github/gluhov/iotplatform/eventscollector` |
| `DCS` | `services/device-collector-service` |
| `DCSJ` | `DCS/src/main/java/com/github/gluhov/iotplatform/devicecollector` |
| `DCST` | `DCS/src/test/java/com/github/gluhov/iotplatform/devicecollector` |
| `FEP` | `services/failed-events-processor` |
| `FEPJ` | `FEP/src/main/java/com/github/gluhov/iotplatform/failedeventsprocessor` |
| `FEPT` | `FEP/src/test/java/com/github/gluhov/iotplatform/failedeventsprocessor` |
| `INF` | `infrastructure` |

## Phase 1 (P1): Контракт FailedEvent

- [ ] T001 Схема `FailedEvent` — JSON дословно из [failed-event.avsc](contracts/failed-event.avsc), версия avros `1.4.0` —
  `AV/avro/failed-event.avsc`, `AV/build.gradle.kts` (FR-001, FR-002)
- [ ] T002 [P] Регистрация subject `failed-events-value=FailedEvent` вместо `events-dlt-value`, `devices-dlt-value`; переменная
  `KAFKA_FAILED_EVENTS_TOPIC` вместо `KAFKA_EVENTS_DLT_TOPIC`, `KAFKA_DEVICES_DLT_TOPIC` — `AV/scripts/schema-subjects.sh`,
  `AV/scripts/container-register.sh`, `AV/scripts/container-publish.sh`, `AV/scripts/nexus-env.sh` (FR-001, FR-003)
- [ ] T003 (R9) Проверить совместимость без публикации: `make avros-check` — код 0; сборка генерирует классы `FailedEvent`,
  `ErrorCode`, `ErrorType` (`./gradlew :avros:build`) (FR-001)
- [ ] T004 **ЧЕЛОВЕК** (необратимо: версия в Nexus не перезаписывается) Опубликовать и зарегистрировать avros 1.4.0: `make avros-up`;
  проверить `curl -s localhost:8081/subjects/failed-events-value/versions/latest` (FR-001)

**Checkpoint**: `make avros-check` и `./gradlew :avros:build` проходят; после T004 subject `failed-events-value` есть в Schema Registry.

## Phase 2 (P2): Стенд — тема, MinIO, переменные

- [ ] T005 Все новые переменные из plan.md («Переменные окружения») в `.env.example` (без секретов: пароли — `change_me_*`),
  удалить `KAFKA_EVENTS_DLT_TOPIC`, `KAFKA_DEVICES_DLT_TOPIC`, `DEVICES_DLT_*`, `MINIO_BUCKETS`, `PROMETHEUS_*` — `.env.example` (FR-003, FR-014, FR-022)
- [ ] T006 Тема `failed-events`: создание (3 раздела, `cleanup.policy=delete`, `retention.ms`, без `retention.bytes`) и удаление
  прежних `events-dlt`, `devices-dlt` (`--delete --if-exists`); `KAFKA_FAILED_EVENTS_*` в `kafka-init`, DLT-переменные убрать из
  `avros-publish`, `schema-register`, `kafka-init` — `INF/init/kafka/init-topics.sh`, `INF/docker-compose.yaml` (FR-003).
  Проверка на своём стенде (не стенд владельца): `make topics` показывает `failed-events` и не показывает DLT; повторный запуск `kafka-init` — код 0.
  **Необратимо на стенде владельца** (удаляет DLT-темы вместе с данными): агент проверяет только на своём проекте compose
- [ ] T007 Образ MinIO `pgsty/minio:${MINIO_VERSION}` в базовом стенде (профиль `platform` убрать), `MINIO_PROMETHEUS_AUTH_TYPE: public`,
  том `minio-data`, healthcheck `mc ready local`, порты из `.env.example` — `INF/docker-compose.yaml` (FR-014; R10)
- [ ] T008 Скрипт проверки прав и сроков: каждая учётка — свой бакет `200`, чужой бакет и чужой префикс — `403`; `mc ilm rule ls`
  по каждому бакету; условная запись `If-None-Match: *` на `failed-events` (с lock и версиями) — второй `PUT` `412`, версия одна; бакет `failed-events` без lock → init с кодом ≠ 0 и текстом причины;
  `make minio-check` — `INF/config/minio/check.sh` (100755), `Makefile` (FR-015, FR-016, FR-008, SC-005)
- [ ] T009 Init MinIO на чистом `sh` в образе MinIO (нет `grep`, `sed`, `awk`; R10): бакеты (`failed-events` — с `--with-lock`, версии
  включены), пользователи и политики, ILM через `mc ilm import`; идемпотентен; бакеты не очищает — `INF/init/minio/init.sh` (100755),
  `INF/config/minio/policies/{failed-events-processor,mimir,loki,tempo,backup-postgres,backup-clickhouse}.json`,
  `INF/config/minio/ilm/{failed-events,backups,mimir-blocks,loki-chunks,tempo-traces}.json` (FR-014, FR-015, FR-016; R11; R12 — см. research.md). Бакет `failed-events` уже существует без lock: init завершается с кодом ≠ 0 и текстом причины (проверка — T008).
  Сервис `minio-init` на образе MinIO в compose, `restart: "no"` — `INF/docker-compose.yaml`

**Checkpoint**: `make check`; на своём проекте compose `make up` поднимает MinIO и `minio-init` (`exited (0)`);
`make minio-check` проходит; повторный запуск `minio-init` ничего не меняет.

## Phase 3 (P3): events-collector-service — FailedEvent вместо events-dlt

Эталон — проверенные решения research.md R1–R4; образец сервиса — `DCS`. Unit-задачи — после T004; IT-задачи (T013, T012) — после P5 (процессор вторым контекстом).

- [ ] T010 [P] Тест: `ECST/adapter/out/kafka/FailedEventRecovererTest.java` — для каждой строки таблицы «исключение → код» (data-model.md)
  поля `FailedEvent` (`errorCode`, `errorType`, `attempts`, `sourceService`, координаты, `sourceTimestamp`, `deviceId`, `description` ≤ 2000); `traceId` без `traceparent` и без спана — `null`;
  `givenPublishFails_whenRecover_thenExceptionAndNoCommit`; байты `key`/`value` побайтно равны исходным (FR-002, FR-004, FR-005; R1, R3)
- [ ] T011 [P] Тест: `ECST/config/ConsumerDeserializationTest.java` — ключ не строгий UTF-8 (`FF FE 41`) → `DESERIALIZATION_FAILED`
  с исходными байтами; сырой массив сохранён обёрткой до разбора (FR-005; R1, R2)
- [ ] T012 (R23) Проверка до IT сборщиков (после P5, T040): процессор поднимается вторым Spring-контекстом в IT сборщика — `testImplementation(project(":services:failed-events-processor"))`, `SpringApplicationBuilder` с `setDefaultProperties`; конфигурация `failed-events-processor.yaml` читается, бины сборщика и процессора не конфликтуют. Зонды: `ECST/it/ItSecondContextProbeTest.java`, `DCST/it/ItSecondContextProbeTest.java` — контекст поднимается, в нём есть `FailedEventListener`. Не подтвердилось — остановиться; запасной вариант — сквозной тест на стенде (`e2eTest`) на macOS-хосте, решение человека (FR-023, R23)
- [ ] T013 Тесты DLT удалить или переписать на `failed-events`: `ECST/config/KafkaConfigTest.java`, `ECST/service/config/KafkaErrorClassifierTest.java`,
  `ECST/service/adapter/out/kafka/KafkaDeviceInfoProducerTest.java` (конструктор `KafkaProperties`), `ECST/it/ItKafkaErrorHandlingTest.java`
  (темы `events-errors-it`, subject `failed-events-value`; сценарии: невалидное, хранилище недоступно после N попыток, нечитаемые байты, другой тип
  Avro, Schema Registry временно недоступен без FailedEvent; до объекта в MinIO — процессор вторым контекстом, после P5 и проверки R23), `ECST/it/AbstractItTest.java` (subjects `*-dlt-value`) (FR-004, FR-023)
- [ ] T014 Контракт: `ECST/service/contract/ContractCompatibilityTest.java` — запись `FailedEvent` версии 1.4.0 читается, неизвестный `errorCode` → `UNKNOWN` (FR-001; R9)
- [ ] T015 Зависимость `avrosVersion = "1.4.0"` — `ECS/build.gradle.kts` (FR-001)
- [ ] T016 Сырые байты до разбора: `RawKeepingDeserializer` и `StrictUtf8StringDeserializer` (ключ) — `ECSJ/config/RawKeepingDeserializer.java`,
  `ECSJ/config/StrictUtf8StringDeserializer.java`, `ECS/src/main/resources/application.yaml`; сырые байты — в заголовках `x-raw-key`, `x-raw-value`, из `headers` `FailedEvent` исключаются (FR-005; R1, R2)
- [ ] T017 `FailedEventRecoverer` (`ConsumerRecordRecoverer`): синхронный `send().get(FAILED_EVENTS_PUBLISH_TIMEOUT)`, ключ сообщения
  `<topic>-<partition>-<offset>`, `traceId` из текущего спана, иначе `traceparent`; метрика `failed.events.published{error_code,error_type}` —
  `ECSJ/adapter/out/kafka/FailedEventRecoverer.java`, `ECSJ/support/MetricsFacade.java` (FR-004, FR-005, FR-006; R3, R4)
- [ ] T018 `KafkaConfig`: `DeadLetterPublishingRecoverer` и шаблоны DLT заменены на `FailedEventRecoverer`; классификация и повторы как
  прежде (`storageBackOff` = `maxAttempts − 1` повторов); `setCommitRecovered(true)`; лог `status=FAILED_EVENT` вместо `REJECTED` —
  `ECSJ/config/KafkaConfig.java`, `ECSJ/config/KafkaProperties.java` (`failedEventsTopic`, без `@DefaultValue` — AGENTS.md), `ECSJ/config/KafkaErrorClassifier.java`,
  `ECSJ/config/ProcessingProperties.java` (Javadoc) (FR-004, FR-006)
- [ ] T019 Конфигурация: `failed-events-topic: ${KAFKA_FAILED_EVENTS_TOPIC}`, тайм-ауты продюсера ошибок `FAILED_EVENTS_*` —
  `ECS/src/main/resources/application.yaml`, `application-docker.yaml` (если есть) (FR-003, FR-004)
- [ ] T020 Подсказка о DLT: `ECS/scripts/check-local.sh` (комментарий о теме `devices`), документация в Javadoc — `ECS/scripts/check-local.sh` (FR-003)

**Checkpoint**: `./gradlew :services:events-collector-service:test :services:events-collector-service:integrationTest :services:events-collector-service:check`
проходят; `git grep -i -E 'dlt|dead.?letter' -- services/events-collector-service` пуст.

## Phase 4 (P4): device-collector-service — FailedEvent вместо devices-dlt

Эталон — P3 (тот же механизм), смещения и `DevicesErrorHandler` не меняются. После T004; IT-задача T023 — после P5.

- [ ] T021 [P] Тест: `DCST/adapter/out/kafka/FailedEventRecovererTest.java` — как T010 (включая `traceId == null` без трассы); плюс `UNKNOWN` и `CREATE`/`UPDATE` без данных устройства →
  `VALIDATION_FAILED`/`PERMANENT`; ошибка хранилища `shard` → `STORAGE_UNAVAILABLE`/`TRANSIENT` (FR-002, FR-004, FR-005; R1, R3)
- [ ] T022 [P] Тест: ключ не строгий UTF-8 и мусор в значении — `DCST/config/ConsumerDeserializationTest.java` (новый, по образцу ECS) (FR-005; R1, R2)
- [ ] T023 Тесты DLT переписать на `failed-events`: `DCST/config/KafkaConfigTest.java`, `DCST/support/MetricsFacadeTest.java` (метрика
  `failed_events_published_total`), `DCST/it/ItDeviceCollectorTest.java` (до объекта в MinIO — процессор вторым контекстом, после P5; невалидное, нечитаемое, другой Avro, шард недоступен — после N попыток
  `STORAGE_UNAVAILABLE`), `DCST/it/ItDeviceOperationsTest.java` (`UNKNOWN` → `VALIDATION_FAILED`), `DCST/it/ItConsumerOffsetsTest.java`
  (раздел `failed-events` вместо `devices-dlt`), `DCST/it/AbstractItTest.java` (FR-004, FR-023)
- [ ] T024 Зависимость `avrosVersion = "1.4.0"` — `DCS/build.gradle.kts` (FR-001)
- [ ] T025 `RawKeepingDeserializer`, `StrictUtf8StringDeserializer` — `DCSJ/config/RawKeepingDeserializer.java`, `DCSJ/config/StrictUtf8StringDeserializer.java`,
  `DCS/src/main/resources/application.yaml` (FR-005; R1, R2)
- [ ] T026 `FailedEventRecoverer` по образцу T017; метрика `failed.events.published{error_code,error_type}` вместо `devices.dlt.published{reason}` —
  `DCSJ/adapter/out/kafka/FailedEventRecoverer.java`, `DCSJ/domain/port/out/DeviceMetrics.java`, `DCSJ/support/MetricsFacade.java` (FR-004, FR-006; R3, R4)
- [ ] T027 `KafkaConfig`: `deadLetters` и шаблоны заменены на `FailedEventRecoverer`; `DevicesErrorHandler`, `NoOffsetRecoveringConsumer`,
  `DevicesConsumerOffsets` без изменений поведения; лог `status=FAILED_EVENT` вместо `DLT` — `DCSJ/config/KafkaConfig.java`,
  `DCSJ/config/KafkaProperties.java`, `DCSJ/config/KafkaErrorClassifier.java`, `DCSJ/config/ProcessingProperties.java`,
  `DCSJ/domain/model/FailureReason.java` (метки `reason` остаются, Javadoc обновить) (FR-004, FR-006)
- [ ] T028 Конфигурация: `failed-events-topic`, тайм-ауты `FAILED_EVENTS_*` вместо `DEVICES_DLT_*` — `DCS/src/main/resources/application.yaml` (FR-003, FR-004)
- [ ] T029 Комментарии и Javadoc с «devices-dlt» — `DCSJ/adapter/in/kafka/DeviceInfoListener.java`, `DCSJ/domain/service/DeviceInfoValidator.java`,
  `DCSJ/domain/service/DeviceRegistryService.java`, `DCSJ/domain/exception/DeviceNotStoredException.java`,
  `DCSJ/domain/exception/InvalidDeviceInfoException.java` (FR-003)
- [ ] T030 Скрипты: подсказки «topic devices-dlt» — `DCS/scripts/quick-check.sh`, `services/device-service/scripts/quick-check.sh` (FR-003)

**Checkpoint**: `./gradlew :services:device-collector-service:test :services:device-collector-service:integrationTest :services:device-collector-service:check`
проходят; `git grep -i -E 'dlt|dead.?letter' -- services/device-collector-service services/device-service` пуст; `git grep -n -i 'failed' -- services/device-service` пусто (FR-006).

## Phase 5 (P5): failed-events-processor — новый сервис

Образец структуры и сборки — `DCS`; код прототипа — как требования (research.md R5–R8), не копировать. После T004.

- [ ] T031 Сборка: Java 25, Spring Boot 4.0.3, `spring-boot-starter-kafka`, `kafka-avro-serializer` 8.2.4, AWS SDK for Java v2 `s3` (версию — явно, одну, BOM
  `software.amazon.awssdk:bom`), `micrometer-registry-prometheus`, `opentelemetry`, ArchUnit, Testcontainers (`kafka`, `testcontainers-minio` 2.0.3,
  Schema Registry), JaCoCo 0.8.14 ≥ 0,70 по строкам, задача `integrationTest` (`It*Test`), avros 1.4.0 — как у `DCS` — `FEP/build.gradle.kts` (FR-022, FR-023)
- [ ] T032 Подпроект: `include(":services:failed-events-processor")`; `COPY services/failed-events-processor/build.gradle.kts services/failed-events-processor/`
  во все Dockerfile других подпроектов (`grep -L` пуст; после T031 — нужен `build.gradle.kts` процессора) — `settings.gradle.kts`, `AV/Dockerfile`, `ECS/Dockerfile`, `DCS/Dockerfile`,
  `services/db-migrator/Dockerfile`, `services/device-service/Dockerfile` (FR-022)
- [ ] T033 [P] Правила слоёв: домен не зависит от Kafka, S3 SDK, Spring Kafka; `@KafkaListener` только в `adapter.in.kafka` — `FEPT/architecture/ArchitectureTest.java` (FR-022)
- [ ] T034 [P] Тест ключа: `givenRecord_whenKey_thenFromFailedEventsPartitionOffsetAndRecordTimestampUtc`, `…WhenUnreadableAndReadableOfSameRecord_thenSameKey`,
  `…WhenRedelivered_thenSameKeyAndSameDate`, `…WhenNoDeviceId…` — `FEPT/domain/model/ArchiveKeyTest.java` (FR-007, FR-008)
- [ ] T035 [P] Тест JSON-документа: все поля `FailedEvent` присутствуют, `base64(value)` и `base64(key)` равны байтам, `null` у `traceId`/`key`/`value`, заголовки,
  метаданные объекта — `FEPT/domain/service/ArchiveServiceTest.java` (FR-007; R5)
- [ ] T036 Контракт: запись `FailedEvent` 1.4.0 читается процессором; неизвестный `errorCode` → `UNKNOWN`, в объекте `UNKNOWN` и исходное число — `FEPT/contract/FailedEventContractTest.java` (FR-001; R9)
- [ ] T037 IT (Kafka, Schema Registry, MinIO — `pgsty/minio` через `asCompatibleSubstituteFor`; R19): объект создан, JSON полон, байты побайтно (FR-007); повторная доставка → один объект, одна версия (FR-008); запись, сначала нечитаемая, затем разобранная (схема появилась), → один объект (FR-008, FR-010); нечитаемые байты → объект с `kind = unreadable` (FR-010) — `FEPT/it/ItFailedEventsProcessorTest.java`,
  `FEPT/it/AbstractItTest.java`, `FEPT/it/MinioTestContainerConfig.java` (FR-007, FR-008, FR-010, FR-023, SC-002)
- [ ] T038 IT недоступности: MinIO на `docker pause` дольше `max.poll.interval.ms` — смещение не движется, группа `Stable`, после снятия паузы объект появился и смещение
  продвинулось (SC-003); перезапуск процессора над непустым бакетом — объекты на месте; Schema Registry на паузе — объект `unreadable` не создаётся — `FEPT/it/ItFailedEventsOutageTest.java` (FR-009, FR-010; R6, R7)
- [ ] T039 IT трассы: запись с `traceparent` — объект содержит тот же `traceId`, спан `s3.put-object` — потомок спана слушателя — `FEPT/it/ItFailedEventsTraceTest.java` (FR-011; R8)
- [ ] T040 [P] Точка входа и конфигурация только из переменных окружения (plan.md), JSON-логи с `traceId`/`spanId`, `max.poll.interval.ms`,
  `enable-auto-commit: false`, `ack-mode: manual_immediate`, `max-poll-records: 1`, `auto-offset-reset: earliest` —
  `FEPJ/FailedEventsProcessorApplication.java`, `FEP/src/main/resources/failed-events-processor.yaml`, `failed-events-processor-docker.yaml`, `logback-spring.xml` (FR-022, FR-009); `application.yaml` не создаётся (R23). `main` задаёт `spring.config.name=failed-events-processor`. Тайм-ауты S3 и интервалы повторов — только переменные `FAILED_EVENTS_S3_*`, `FAILED_EVENTS_RETRY_*`, `FAILED_EVENTS_MAX_POLL_INTERVAL_MS`; в файле конфигурации только ссылки на них
- [ ] T041 [P] Dockerfile в два этапа (JDK 25 → JRE 25, слои Spring Boot, непривилегированный пользователь) по образцу `DCS/Dockerfile` — `FEP/Dockerfile` (FR-022)
- [ ] T042 Домен: `FailedRecord`, `ArchiveKey`, порт `FailedEventArchive`, `ArchiveService` (ключ + JSON + метаданные), без Spring Kafka и SDK —
  `FEPJ/domain/{model,port,service}` (FR-007)
- [ ] T043 [P] Тест адаптера S3: `PutObject` с `If-None-Match: *`, `412` → `DUPLICATE`, тайм-ауты `apiCallTimeout`/`apiCallAttemptTimeout` заданы, метаданные
  `x-amz-meta-*`, спан `s3.put-object` с тегом `bucket` — `FEPT/adapter/out/s3/S3FailedEventArchiveTest.java` (FR-008, FR-009, FR-011; R5, R6, R8)
- [ ] T044 Адаптер S3: `S3FailedEventArchive`, `S3Config` (клиент с тайм-аутами, ключи из `FAILED_EVENTS_S3_*`, path-style) —
  `FEPJ/adapter/out/s3/S3FailedEventArchive.java`, `FEPJ/config/S3Config.java` (FR-007…FR-009, FR-011)
- [ ] T045 [P] Тест слушателя и обработчика ошибок: смещение подтверждается только после записи; MinIO недоступен — повторы с `ContainerPausingBackOffHandler`,
  бесконечный `ExponentialBackOff` (интервалы — `FAILED_EVENTS_RETRY_*`); временная ошибка Schema Registry — повторы без `unreadable`; мусор/404 схемы — `unreadable` и смещение —
  `FEPT/adapter/in/kafka/FailedEventListenerTest.java`, `FEPT/config/KafkaConfigTest.java` (FR-009, FR-010; R6, R7)
- [ ] T046 Слушатель и конфигурация: `FailedEventListener` (`@KafkaListener`, `FailedEvent`), `UnreadableRecorder`, `KafkaConfig` — `FEPJ/adapter/in/kafka/FailedEventListener.java`,
  `FEPJ/config/KafkaConfig.java`, `FEPJ/domain/service/UnreadableRecorder.java` (FR-009, FR-010, FR-011)
- [ ] T047 [P] Метрики: `failed.events.archived{source_service,error_code,outcome=created|duplicate|unreadable}`, `failed.events.archive.errors{reason}`;
  таймер наблюдения `s3.put-object` — `FEPJ/support/MetricsFacade.java`, `FEPT/support/MetricsFacadeTest.java` (FR-011, FR-019)
- [ ] T048 [Mac] Скрипт стенда `make fep-quick-check` (по образцу `DCS/scripts/quick-check.sh`): невалидное событие в `events` с заданным `traceparent`, невалидное сообщение в `devices`,
  байты-мусор в `events` → три объекта в `failed-events` (`mc`), `traceId` в JSON, `base64(value)` равно отправленному — `FEP/scripts/quick-check.sh` (100755) (FR-007, FR-011, FR-023)

**Checkpoint**: `./gradlew :services:failed-events-processor:test :services:failed-events-processor:integrationTest :services:failed-events-processor:check` проходят;
`grep -L 'failed-events-processor/build.gradle.kts' services/*/Dockerfile avros/Dockerfile` пуст.

## Phase 6 (P6): Наблюдаемость — Mimir вместо Prometheus, Loki и Tempo в MinIO

После P2.

- [ ] T049 [Mac] Проверка наблюдаемости: скрипт — все PromQL всех дашбордов (200, нет 400), источники Grafana, `up{job}` сервисов, объекты в `mimir-blocks`, `loki-chunks`,
  `tempo-traces`; запуск `make obs-check` — `INF/config/observability/check.sh` (100755), `Makefile` (FR-017…FR-019)
- [ ] T050 (R13) Mimir `grafana/mimir:${MIMIR_VERSION}`: монолит `-target=all`, порт 8080, blocks в префиксе бакета `mimir-blocks`, ruler — префиксом того же бакета, ограничение хранения
  `MIMIR_RETENTION_PERIOD`, учётка `MIMIR_S3_*`; без healthcheck (нет shell); зависимые — `service_started` — `INF/config/mimir/mimir.yaml`, `INF/docker-compose.yaml` (FR-017, FR-018; R13)
- [ ] T051 Alloy: `prometheus.remote_write` → `http://mimir:8080/api/v1/push`; скрейп MinIO (`/minio/v2/metrics/cluster`, `/minio/metrics/v3/cluster/usage/buckets`) и прежних
  заданий Prometheus (прокси, экспортёры PostgreSQL/Kafka, Keycloak, Loki, Tempo, Grafana, сам Alloy, Mimir) — `INF/config/alloy/config.alloy` (FR-017; R13, R16)
- [ ] T052 Grafana: источник `uid: prometheus`, тип `prometheus`, адрес `http://mimir:8080/prometheus`; зависимость от Mimir вместо Prometheus —
  `INF/config/grafana/provisioning/datasources/datasources.yaml`, `INF/docker-compose.yaml` (FR-017; R13)
- [ ] T053 Prometheus удалить: сервис, том, `INF/config/prometheus/`, `PROMETHEUS_*`, зависимости Alloy и Grafana, `serviceMap` Tempo (источник тот же uid) —
  `INF/docker-compose.yaml`, `INF/config/prometheus/`, `INF/config/tempo/tempo.yaml` (FR-017)
- [ ] T054 Loki: хранилище S3 (бакет `loki-chunks`, учётка `LOKI_S3_*`), срок `LOKI_RETENTION_PERIOD` (90 суток), без `flush_on_shutdown`; локальный том остаётся для WAL —
  `INF/config/loki/loki-config.yaml` (FR-018; R14)
- [ ] T055 Tempo: хранилище S3 (бакет `tempo-traces`, `TEMPO_S3_*`), срок `TEMPO_BLOCK_RETENTION` (14 суток) — `INF/config/tempo/tempo.yaml` (FR-018; R15)
- [ ] T056 Дашборд `failed-events-processor` (поток по `source_service` и `error_code`, исходы `created/duplicate/unreadable`, задержка `s3_put_object_seconds`, отставание группы
  `kafka_consumergroup_lag{consumergroup="failed-events-processor"}`, ошибки записи, логи `{service="failed-events-processor"}`) — `INF/config/grafana/dashboards/failed-events-processor.json` (FR-019)
- [ ] T057 [Mac] Панели MinIO (первым шагом (R16): прогнать PromQL панелей MinIO через Mimir `/prometheus/api/v1/query`, каждое выражение возвращает ряды. Панели: объём и число объектов по бакетам — v3 `minio_cluster_usage_buckets_*`, запросы `minio_s3_requests_total`, ошибки 4xx/5xx, `ttfb`) —
  `INF/config/grafana/dashboards/minio.json` (FR-019; R16)
- [ ] T058 Панели DLT заменить: `device-collector.json` («В devices-dlt», «devices-dlt, ops/s» → `failed_events_published_total`), `events-collector.json` (панель
  публикации ошибок), `kafka.json` (тема `failed-events`) — `INF/config/grafana/dashboards/{device-collector,events-collector,kafka}.json` (FR-019)
- [ ] T059 Alloy: метки логов процессора (`service`, `trace_id`, `device_id`) — `stage.match` для `failed-events-processor` — `INF/config/alloy/config.alloy` (FR-011)

**Checkpoint** ([Mac]): `make check`; `make up PROFILES=observability` на своём проекте compose; `make obs-check` проходит; `git grep -n -i prometheus -- infrastructure`
не содержит сервиса `prometheus` (остаётся только формат метрик).

## Phase 7 (P7): Копии баз в MinIO

После P6.

- [ ] T060 [Mac] (R18) Проверка на стенде (первой в P7, до кода T061–T066; проверяет непроверенное из research.md R18): `make backup` → в `backups/` две копии с manifest; расписание срабатывает (журнал); `flock` — второй запуск отказывается; после удаления
  данных `make shard-restore FROM=minio …` и `make clickhouse-restore …` возвращают число устройств и строк из manifest, смещения группы возвращены — `INF/config/backup-runner/check.sh` (100755), `Makefile` (`make backup-check`) (FR-020, FR-021, SC-004)
- [ ] T061 Образ `backup-runner`: `FROM apache/kafka:4.2.0`, клиенты PostgreSQL 18.6 (копия из `postgres:18.6-alpine`), `mc` из `pgsty/mc`, busybox `crond`, root; entrypoint пишет
  `/etc/backup.env` — `INF/config/backup-runner/Dockerfile`, `INF/config/backup-runner/entrypoint.sh` (100755) (FR-020; R18)
- [ ] T062 Копия PostgreSQL: логика `shard-backup.sh` (с реплик, manifest со смещениями группы, `*.partial` → переименование) → `mc` в `backups/postgres/<метка>/`; один общий скрипт (вызывают `make shard-backup` и backup-runner, без второй реализации);
  блокировка `flock` против двух одновременных копий — `INF/config/backup-runner/backup-postgres.sh` (100755) (FR-020; R18)
- [ ] T063 Копия ClickHouse: `BACKUP DATABASE … TO S3(backup_s3, …)` через HTTP, ключи — named collection с `from_env`, `named_collection_control` пользователю, уникальный id —
  `INF/config/clickhouse/users.d/backup.xml`, `INF/config/clickhouse/config.d/backup_s3.xml`, `INF/config/backup-runner/backup-clickhouse.sh` (100755) (FR-020; R17)
- [ ] T064 Сервис `backup-runner` в compose (расписание `BACKUP_SCHEDULE`, учётки `BACKUP_*_S3_*`, зависимости от шардов, ClickHouse, MinIO, Kafka), цели `make backup`,
  `make backup-list` — `INF/docker-compose.yaml`, `Makefile` (FR-020)
- [ ] T065 Восстановление PostgreSQL из MinIO: `FROM=minio` в `shard-restore.sh` (скачать → проверка sha256 → прежний путь, сброс смещений группы `--reset-offsets --from-file`),
  `make shard-restore FROM=minio BACKUP=<метка>` — `INF/config/postgres/shard-restore.sh`, `Makefile` (FR-021; R18)
- [ ] T066 Восстановление ClickHouse: `make clickhouse-restore BACKUP=<метка>` — `INF/config/backup-runner/restore-clickhouse.sh` (100755), `Makefile` (FR-021; R17)

**Checkpoint** ([Mac]): `make check`; `make backup-check` на своём проекте compose проходит; `git diff --summary | grep mode` — режимы `100755` у новых скриптов.

## Phase 8 (P8): Сборка стенда, документы, диаграммы, проверки

После P3–P7.

- [ ] T067 Сервис `failed-events-processor` в compose: 1–3 экземпляра (`deploy.replicas`), диапазон портов `FAILED_EVENTS_PROCESSOR_PORTS`, зависимости (`kafka-init`, `schema-register`,
  `minio-init`), healthcheck через `/actuator/health/liveness`, `OTLP` на Alloy, переменные без значений по умолчанию; Alloy находит экземпляры и собирает метрики —
  `INF/docker-compose.yaml`, `INF/config/alloy/config.alloy` (FR-022)
- [ ] T068 Makefile: только `make help` и проверка, что все цели есть (цели создают свои пакеты): `fep-quick-check`, `minio-check`, `obs-check`, `backup*`, `topics` — `Makefile` (FR-022)
- [ ] T069 [Mac] `make up` на чистом стенде (свой проект compose): порядок старта, все контейнеры `healthy`; `make check-local`, `dcs-quick-check`, `ds-quick-check`,
  `fep-quick-check` — вывод в отчёт (FR-022, FR-007)
- [ ] T070 [Mac] (R20) Всплеск: Proxy остановлен, N событий в `events` → N записей `STORAGE_UNAVAILABLE` после `EVENTS_MAX_ATTEMPTS` попыток, N объектов после; замер скорости процессора
  (объектов/с), отставание на панели; результат — раздел «Результаты проверок на стенде» в research.md; число записей `failed-events` равно числу объектов (FR-012, SC-001)
- [ ] T071 [Mac] Поиск через полгода: команды `mc ls`, `mc find --metadata` по сервису, дате, коду, `deviceId`, `traceId` проверены на тестовом сообщении — раздел README и
  quickstart (FR-013)
- [ ] T072 Диаграммы привести к реализации: `diagrams/containers.mmd`, `component-failed-events-processor.mmd`, `fep-error-to-minio-sequence.mmd`, прежние компонентные и sequence;
  `grep -rn -i -E 'dlt|iot\.dlt' diagrams` пуст; PNG перерендерить рядом (неотслеживаемые) (FR-003)
- [ ] T073 README: порядок запуска, как вызвать ошибку и найти объект, восстановление баз из копии, ограничения (одна нода MinIO, `pg_dump` не PITR), таблица «отличается от
  иллюстративных примеров исходной постановки» (research.md), удалить разделы о DLT — `README.md` (FR-013, FR-021)
- [ ] T074 Определение готовности: секция `docs/definition-of-done.md` — ссылка на spec.md, правки прошлых секций (DLT → `failed-events`, Prometheus → Mimir, MinIO в базовом
  стенде, копии, судьба `make shard-backup`) на месте; `docs/adr/README.md` — 0013–0019 — `docs/definition-of-done.md`, `docs/adr/README.md`
- [ ] T075 Конституция: «Технологии» — Mimir/Loki/Tempo/Grafana, MinIO; версия 1.2.0 — `.specify/memory/constitution.md`; `AGENTS.md` — таблица команд (`make minio-check`,
  `fep-quick-check`, `backup-check`, `obs-check`) — `AGENTS.md`
- [ ] T076 `analyze`: проход `/speckit-analyze` (Spec Kit) и ручная сверка — дописать в `analyze-report.md` проход «после реализации» (FR-001…FR-023)
- [ ] T077 **ЧЕЛОВЕК** Снимки для сдачи: новые панели Grafana, сквозная трасса от события в `events` до `s3.put-object`, содержимое бакетов (объект ошибки, `mimir-blocks`,
  `loki-chunks`, `tempo-traces`, `backups`) — каталог `docs/screenshots/` (если принят в репозитории), ссылка в PR
- [ ] T078 **ЧЕЛОВЕК** На стенде владельца после принятия PR: `make down`, `make up` (удалит темы `events-dlt`, `devices-dlt` вместе с данными — убедиться, что они не нужны), приёмка на Mac
  по Checkpoint фаз; образ `pgsty/minio` запускается на arm64 macOS-хоста (R22)

**Checkpoint** ([Mac] для `make up` и проверок стенда): `make check`; `./gradlew :services:events-collector-service:check :services:device-collector-service:check :services:failed-events-processor:check`;
`make up`; `make check-local dcs-quick-check ds-quick-check fep-quick-check minio-check obs-check backup-check`.

## Phase 9 (P9, необязательно): Дополнительно — возврат объекта и Object Lock

Раздел «Дополнительно» задания: необязательный, в обязательный минимум и в результат converge по P1–P8 (FR-001…FR-023) не входит. По spec берём возврат одного
объекта и запрет удаления, каскад не берём (ADR-0019). Бакет `failed-events` уже создан с lock (T009), здесь — режим и команда.

- [ ] T079 [P] Тест возврата: ключ объекта → сообщение в исходном топике с теми же байтами `key`/`value` (без пересериализации), `traceparent` из заголовков, заголовок `x-replayed-from`,
  повторный возврат помечен; неизвестный ключ → код 2 — `FEP/scripts/replay-test.sh` (100755) (FR-024)
- [ ] T080 Команда `make failed-replay KEY=<ключ объекта>` на `kafka-console-producer`/`kcat` образа `backup-runner`: читает объект через `mc`, декодирует base64,
  публикует в `sourceTopic` байты исходного ключа и значения (раздел выбирает ключ) — `FEP/scripts/replay.sh` (100755), `Makefile` (FR-024)
- [ ] T081 Object Lock: режим GOVERNANCE, `FAILED_EVENTS_LOCK_DAYS` (180) — `INF/config/minio/` (политика), `INF/init/minio/init.sh`; проверка: удаление объекта
  учётками процессора и администратора без обхода режима отклоняется — `INF/config/minio/check.sh` (FR-025; R12, R21)
- [ ] T082 Решение по ILM под Object Lock: срок ILM и `FAILED_EVENTS_LOCK_DAYS` равны (180); истечение под lock не проверено — ждать 180 суток нельзя; записать в ADR-0019 «Последствия» — `docs/adr/0019-optional-return-and-object-lock.md` (FR-025; R21)

**Checkpoint** ([Mac]): `make failed-replay KEY=…` на своём проекте compose возвращает сообщение побайтно; `make minio-check` с проверкой запрета удаления проходит.

## Phase 10: converge

- [ ] T083 `/speckit-converge` по `spec.md`, `plan.md`, `tasks.md`; расхождения кода исправить в коде, документов — остановиться и сообщить; результат записать в раздел
  «Результат converge» ниже

## Результат converge

Статус: **не проведён** — дизайн на проверке, код не написан.

| Проход | Находки | Итог |
|---|---|---|

## Dependencies & Execution Order

- P1 → всё остальное (T004 нужен сборщикам и процессору, потому что Nexus отдаёт avros 1.4.0).
- P2 ‖ P3 ‖ P4 ‖ P5 — разные файлы; unit не требуют стенда. IT сборщиков (P3, P4: T013, T023, T012) — после P5 (процессор вторым контекстом, R23); IT процессора — в P5 (Testcontainers).
- P6 после P2 (бакеты и учётки), P7 после P6 (backup-runner использует MinIO и ClickHouse), P8 после P3–P7, P9 после P8.
- Тяжёлый Docker (стенд, IT) — по очереди, под общим `flock` на lock-файле Docker-запусков.

## Implementation Strategy

MVP — P1 + P2 (тема, MinIO) + P3 + P5: ошибка событий доходит до объекта. Затем P4 (реестр устройств), P6 (телеметрия на MinIO), P7 (копии), P8 (стенд и документы).
