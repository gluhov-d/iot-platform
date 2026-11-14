# Tasks: device-service — управление устройствами

**Input**: [spec.md](spec.md), [plan.md](plan.md), [data-model.md](data-model.md), [research.md](research.md),
[contracts/device-service.yaml](contracts/device-service.yaml), [quickstart.md](quickstart.md), правила агента —
[AGENTS.md](../../AGENTS.md).

Формат: `- [ ] TNNN [P?] [USn?] действие — файлы (требования)`. `[P]` — можно параллельно: другие файлы, нет
зависимости от незавершённых задач. Тест пишется раньше кода, который он проверяет, и сначала падает.
**ЧЕЛОВЕК** — шаг выполняет человек: агент останавливается и ждёт. `(Rn)` — задача проверяет предположение
research.md: не подтвердилось — остановиться и сообщить. Решения, проверенные на прототипе (research.md R2, R4–R6,
R8, R9), в задачах записаны как требования.

Сокращения путей:

| Сокращение | Путь |
|---|---|
| `CL` | `services/device-service-client` |
| `DS` | `services/device-service/src/main/java/com/github/gluhov/iotplatform/deviceservice` |
| `DSR` | `services/device-service/src/main/resources` |
| `DST` | `services/device-service/src/test/java/com/github/gluhov/iotplatform/deviceservice` |
| `DCS` | `services/device-collector-service/src/main/java/com/github/gluhov/iotplatform/devicecollector` |
| `DCSR` | `services/device-collector-service/src/main/resources` |
| `DCST` | `services/device-collector-service/src/test/java/com/github/gluhov/iotplatform/devicecollector` |
| `ECS` | `services/events-collector-service` |
| `INF` | `infrastructure` |

## Phase 1: Setup

- [ ] T001 Подключить подпроекты `services:device-service-client` и `services:device-service` — `settings.gradle.kts` (FR-025)
- [ ] T002 [P] Добавить `COPY` файлов `build.gradle.kts` обоих новых подпроектов — `avros/Dockerfile`,
  `ECS/Dockerfile`, `services/device-collector-service/Dockerfile`, `services/db-migrator/Dockerfile` (FR-025)
- [ ] T003 Перенести контракт: `git mv specs/001-device-service/contracts/device-service.yaml
  CL/src/main/openapi/device-service.yaml`; поправить ссылки — `specs/001-device-service/{spec,plan,research,tasks}.md` (FR-011)
- [ ] T004 Библиотека клиента: `java-library`, OpenAPI Generator 7.17.0 в два прогона (модели: `spring`,
  `useBeanValidation=true`, `useJakartaEe=true`, `openApiNullable=false`; интерфейс: `library=spring-http-interface`
  без моделей), версии зависимостей — `api(platform("org.springframework.boot:spring-boot-dependencies:…"))` без плагина
  dependency-management, пакеты `…deviceservice.api` и `…deviceservice.api.model`, вывод в `build/generated`, версия проекта =
  `info.version` контракта (задача сборки падает при расхождении) — `CL/build.gradle.kts` (FR-011)
- [ ] T005 Публикация в Nexus (`maven-publish`, репозиторий и запрет перезаписи версии — как в `avros/build.gradle.kts`),
  цели `ds-client-check`, `ds-client-release` — `CL/build.gradle.kts`, `CL/scripts/publish.sh` (100755), `Makefile` (FR-012)
- [ ] T006 Сборка сервиса: зависимости из plan.md, `project(":services:device-service-client")`, JaCoCo 0.70 по строкам,
  задача `integrationTest` (`It*Test`), `testImplementation(project(":services:db-migrator"))` — как у
  device-collector-service — `services/device-service/build.gradle.kts` (FR-026)
- [ ] T007 [P] Точка входа и конфигурация только из переменных окружения (plan.md, «Переменные окружения»), JSON-логи с
  `traceId`/`spanId` — `DS/DeviceServiceApplication.java`, `DSR/application.yaml`, `DSR/application-docker.yaml`,
  `DSR/logback-spring.xml` (FR-024, FR-025)
- [ ] T008 [P] Dockerfile в два этапа (JDK 25 → JRE 25, слои Spring Boot, непривилегированный пользователь) — по образцу
  `services/device-collector-service/Dockerfile` — `services/device-service/Dockerfile` (FR-025)
- [ ] T009 [P] Правила слоёв (порты и адаптеры) — `DST/architecture/ArchitectureTest.java` (FR-026)
- [ ] T010 Каркас контроллера `implements DevicesApi` (методы отвечают 501, без `@RequestMapping`; тело — `@Valid` на
  параметре реализации; `deviceId` из пути проверяется явно через `jakarta.validation.Validator` — ограничений на
  `@PathVariable` в реализации нет, research.md R4) и тест маршрутов: четыре метода контракта находят обработчик,
  невалидное тело и `deviceId` длиннее 128 — 400 — `DS/adapter/in/web/DeviceController.java`,
  `DST/adapter/in/web/DeviceControllerRoutingTest.java` (FR-011)

**Checkpoint**: `./gradlew :services:device-service-client:build :services:device-service:test` проходит;
`git status --short` не содержит сгенерированных файлов.

## Phase 2: Foundational

### Контракт сообщений

- [ ] T011 Поле `operation` (JSON — дословно из data-model.md) и версия `1.3.0` — `avros/avro/device-info.avsc`,
  `avros/build.gradle.kts` (FR-013)
- [ ] T012 Проверить совместимость без публикации: `make avros-check` — код 0 (FR-013)
- [ ] T013 **ЧЕЛОВЕК** (необратимо: версия в Nexus не перезаписывается) Опубликовать и зарегистрировать avros 1.3.0:
  `make avros-up`; проверить `curl -s localhost:8081/subjects/devices-value/versions/latest` (FR-013)
- [ ] T014 [P] events-collector-service: `avrosVersion = "1.3.0"`; тест «1.2.0 читается схемой 1.3.0 с `operation = null`
  и наоборот» — `ECS/build.gradle.kts`,
  `ECS/src/test/java/com/github/gluhov/iotplatform/eventscollector/service/contract/ContractCompatibilityTest.java` (FR-014)
- [ ] T015 [P] Тема `devices`: `min.compaction.lag.ms=${KAFKA_DEVICES_MIN_COMPACTION_LAG_MS}` при создании и
  `kafka-configs --alter` для существующей; переменная в задаче `kafka-init` и `.env.example` (604800000) —
  `INF/init/kafka/init-topics.sh`, `INF/docker-compose.yaml`, `.env.example` (FR-022)
- [ ] T016 [P] Восстановление: если копия старше `KAFKA_DEVICES_MIN_COMPACTION_LAG_MS` (время из manifest) — предупреждение
  о возможной потере ручных изменений до остановки сервисов — `INF/config/postgres/shard-restore.sh` (FR-022)

### База данных и чтение из реплик

- [ ] T017 Миграция (SQL — дословно из data-model.md) —
  `services/db-migrator/src/main/resources/db/migration/devices/V3__device_management.sql` (FR-015, FR-016)
- [ ] T018 [P] Роль `device_reader` (`LOGIN`, пароль `SHARD_READER_PASSWORD`, без `pg_read_all_data` — research.md R5;
  идемпотентно, как `exporter`); `db-migrator` запускается после `shard-replication-init` (права роли выдаёт V3) — `INF/init/shards/shard-replication-init.sh`, `INF/docker-compose.yaml`, `.env.example` (FR-019)
- [ ] T019 Логическая база `devices_read` на репликах и пользователь только для неё (data-model.md,
  «ShardingSphere-Proxy»); Proxy стартует после готовности обеих реплик (`depends_on: service_healthy`): без реплики он
  не запускается (ADR-0004) — `INF/config/shardingsphere/database-devices_read.yaml`,
  `INF/config/shardingsphere/global.yaml`, `INF/docker-compose.yaml`, `.env.example` (`DEVICES_READ_DB_USER`,
  `DEVICES_READ_DB_PASSWORD`). Проверка: `SELECT` под пользователем чтения проходит, `INSERT` и подключение к базе
  `devices` отклоняются; на основных узлах нет сессий `device_reader` (FR-019)

### Redis через Sentinel

- [ ] T020 (R10, R14) `redis` (основной), `redis-replica`, `redis-sentinel-1..3` (кворум 2, имя `platform-redis`,
  `maxmemory-policy volatile-lru`); у сети `iot-net` подсеть `IOT_NET_SUBNET`, у узлов Redis и Sentinel статические
  адреса, Sentinel следит за основным узлом по адресу, не по имени (research.md R9); узел Redis при старте спрашивает
  роль у Sentinel; healthcheck, порядок старта — `INF/docker-compose.yaml`, `INF/config/redis/redis-entrypoint.sh`
  (100755), `INF/config/redis/sentinel-entrypoint.sh` (100755), `.env.example` (`IOT_NET_SUBNET`, `REDIS_MASTER_IP`,
  `REDIS_REPLICA_IP`, `REDIS_SENTINEL_IPS`, `REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_NODES`, `REDIS_MAXMEMORY`),
  `Makefile` (цель `redis-cli` — к основному узлу).
  Проверка: остановить `redis` — Sentinel назначает основной узел, `redis-cli` через Sentinel отвечает; запустить
  `redis` снова — он становится репликой; `docker restart` нового основного узла — он остаётся основным (FR-020)
- [ ] T021 events-collector-service: подключение через Sentinel в профиле `docker`; тесты и профиль `local` — один узел —
  `ECS/src/main/resources/application-docker.yaml`, `INF/docker-compose.yaml` (блок `events-collector-service`) (FR-020)

### Keycloak

- [ ] T022 Realm `iot-platform`: клиент `device-service` (роли `devices.read`, `devices.write` — в `roles.client`),
  `device-service-consumer` (client credentials, обе роли — через `users[]` с `serviceAccountClientId`, audience mapper
  `device-service`), `device-service-reader` (только `devices.read`); секреты — `${…}` из окружения; срок токена 5 мин.
  Keycloak — в базовом стенде (без профиля), хранилище `dev-file`, `--import-realm`, единый `iss` (`KC_HOSTNAME`,
  `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`), логи в JSON, healthcheck по `/health/ready` — `INF/config/keycloak/iot-platform-realm.json`,
  `INF/docker-compose.yaml`, `.env.example` (`KEYCLOAK_REALM`, `KEYCLOAK_ISSUER_URI`, `KEYCLOAK_JWK_SET_URI`,
  `DEVICE_SERVICE_CONSUMER_SECRET`, `DEVICE_SERVICE_READER_SECRET`). Проверка: токен по quickstart.md с хоста и из сети
  Compose имеет один `iss`, `aud` содержит `device-service`; повторный `make up` — без ошибок (FR-010)

### device-collector-service

- [ ] T023 `avrosVersion = "1.3.0"`, `spring-boot-starter-data-redis`, Redis в тестах —
  `services/device-collector-service/build.gradle.kts` (FR-015)
- [ ] T024 Модель: `CommandType` (`CREATE`, `UPDATE`, `DELETE`); `DeviceChange` + команда; `StoredDevice` + `version`,
  `managedAt`, `deletedBy`; `ProcessingOutcome` + `IGNORED`; `ProcessingResult` + `version`. Перед правкой
  `grep -rn "new StoredDevice(\|new DeviceChange(\|new ProcessingResult(" services/device-collector-service/src` —
  поправить все вызовы — `DCS/domain/model/{CommandType,DeviceChange,StoredDevice,ProcessingOutcome,ProcessingResult}.java`,
  `DCST/data/TestData.java`, `DCST/support/InMemoryDeviceRepository.java` (FR-015)
- [ ] T025 Тесты правил — по тесту на строку таблицы FR-015 (1–11) и на FR-017:
  `givenCreate_whenNoDevice_thenInserted`, `givenCreate_whenDeleted_thenRestored`, `givenUpdate_whenNoDevice_thenInserted`,
  `givenUpdate_whenExists_thenAdminFieldsReplaced`, `givenUpdate_whenDeleted_thenIgnored`,
  `givenDelete_whenExists_thenDeletedByAdmin`, `givenDelete_whenNoDevice_thenTombstone`,
  `givenOlderCommand_whenNewerApplied_thenStale`, `givenRouterEvent_whenDeletedByAdmin_thenIgnored`,
  `givenRouterEvent_whenManaged_thenOnlyDeviceDataUpdated`, `givenRouterEvent_whenNotManaged_thenAppliedAsBefore`,
  `givenSameCommandTwice_thenDuplicate`, `givenRouterClockAhead_whenAdminUpdate_thenApplied` —
  `DCST/service/domain/service/DeviceRegistryServiceTest.java` (FR-015, FR-016, FR-017)
- [ ] T026 [P] Проверка сообщения: `UNKNOWN` и `CREATE`/`UPDATE` без `device` — невалидно; `DELETE` без `device` — валидно;
  тесты — `DCS/domain/service/DeviceInfoValidator.java`, `DCST/service/domain/service/DeviceInfoValidatorTest.java` (FR-015)
- [ ] T027 [P] Перенос `operation` в `DeviceChange`; тесты — `DCS/domain/service/DeviceInfoMapper.java`,
  `DCST/service/domain/service/DeviceInfoMapperTest.java` (FR-015)
- [ ] T028 Правила применения (data-model.md, «Правила device-collector-service»): `outcomeOf`, `toRecord`, `apply`;
  тесты T025 проходят — `DCS/domain/service/DeviceRegistryService.java` (FR-015, FR-016, FR-017)
- [ ] T029 Запросы: чтение `version`, `managed_at`, `deleted_by` под блокировкой; запись полей администратора и данных
  устройства раздельно; `version + 1`; `RETURNING current_database(), version` — `DCS/domain/port/out/DeviceRepository.java`,
  `DCS/adapter/out/postgres/JdbcDeviceRepository.java`, `DCST/support/InMemoryDeviceRepository.java` (FR-015)
- [ ] T030 Сброс кэша: порт, адаптер Redis со скриптом `raise-floor.lua` (data-model.md), подключение через Sentinel в
  профиле `docker`, счётчик `devices_cache_invalidations_total{result}`; тесты «успех» и «Redis недоступен — исключения нет» —
  `DCS/domain/port/out/DeviceCacheInvalidator.java`, `DCS/adapter/out/redis/RedisDeviceCacheInvalidator.java`,
  `DCSR/redis/raise-floor.lua`, `DCSR/application.yaml`, `DCSR/application-docker.yaml`,
  `DCS/domain/port/out/DeviceMetrics.java`, `DCS/support/MetricsFacade.java`,
  `DCST/adapter/out/redis/RedisDeviceCacheInvalidatorTest.java` (FR-018, FR-021)
- [ ] T031 Вызов сброса после `COMMIT` и только при `APPLIED`; ошибка Redis не меняет результат обработки; тесты —
  `DCS/domain/service/DeviceRegistryService.java`, `DCST/service/domain/service/DeviceRegistryServiceTest.java` (FR-018, FR-021)
- [ ] T032 [P] Совместимость 1.2.0 ↔ 1.3.0 — `DCST/service/contract/ContractCompatibilityTest.java` (FR-013)
- [ ] T033 Интеграционные тесты: Redis в базовом классе; `givenCreateUpdateDelete_whenConsumed_thenApplied`,
  `givenAdminDelete_whenRouterEventArrives_thenStillDeleted`, `givenSameCommandTwice_whenConsumed_thenAppliedOnce`,
  `givenUnknownOperation_whenConsumed_thenDeadLettered`, `givenCachedDevice_whenUpdateConsumed_thenCacheEvicted`,
  `givenRedisDown_whenConsumed_thenApplied` — `DCST/it/AbstractItTest.java`, `DCST/it/ItDeviceOperationsTest.java`
  (FR-015, FR-016, FR-018, FR-021)
- [ ] T034 Переменные `REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_NODES`, `REDIS_PASSWORD`, `REDIS_TIMEOUT`, зависимость от
  Sentinel — `INF/docker-compose.yaml` (блок `device-collector-service` целиком) (FR-020)

**Checkpoint**: `./gradlew :services:events-collector-service:check :services:device-collector-service:check` проходит;
на стенде `make up`, затем `make check-local` и `make dcs-quick-check` — код 0.

## Phase 3: US5 — доступ только для сервисов (P1)

- [ ] T035 [US5] База интеграционных тестов: Kafka, Schema Registry (схема `devices-value` регистрируется заранее),
  Redis, PostgreSQL со схемой от db-migrator, Keycloak с realm из `INF/config/keycloak`; помощник токенов (сервисный,
  только чтение, пользовательский, без аудитории, истёкший — пользователь и клиенты для них создаются через Admin API) —
  `DST/it/AbstractItTest.java`, `DST/it/KeycloakTokens.java`, `services/device-service/src/test/resources/application-test.yaml` (FR-026)
- [ ] T036 [US5] Тест: без токена — 401; без аудитории — 401; истёкший — 401; пользовательский с ролью и аудиторией —
  403 `SERVICE_TOKEN_REQUIRED`; сервисный без `devices.write` на PUT — 403 `FORBIDDEN`; сервисный с ролью — не 401/403;
  тело ошибок — Problem Details — `DST/it/ItSecurityTest.java` (FR-009, FR-006, SC-004)
- [ ] T037 [US5] Цепочка безопасности: `STATELESS`, GET — `devices.read`, POST/PUT/DELETE — `devices.write`,
  `/actuator/health/**` открыт; ключи — `KEYCLOAK_JWK_SET_URI`, `iss` сверяется с `KEYCLOAK_ISSUER_URI`; проверки
  `exp`/`nbf`, `aud`, сервисной учётной записи (в токене есть `client_id` и нет `sid`); роли из
  `resource_access.device-service.roles`; тайм-ауты запроса ключей — `DS/adapter/in/web/security/SecurityConfig.java`,
  `DS/adapter/in/web/security/AudienceValidator.java`, `DS/adapter/in/web/security/ServiceAccountValidator.java`,
  `DS/adapter/in/web/security/KeycloakRolesConverter.java` + unit-тесты рядом в `DST/adapter/in/web/security/` (FR-009)
- [ ] T038 [US5] Ошибки: перечисление кодов (значения — `Problem.code` контракта), 401/403 и исключения MVC в Problem
  Details с `code` и `traceId`, без стека — `DS/adapter/in/web/ErrorCode.java`, `DS/adapter/in/web/ProblemDetailsAdvice.java`,
  `DS/adapter/in/web/security/ProblemAuthenticationEntryPoint.java`, `DS/adapter/in/web/security/ProblemAccessDeniedHandler.java`,
  `DST/adapter/in/web/ProblemDetailsAdviceTest.java` (FR-006)

**Checkpoint**: `./gradlew :services:device-service:integrationTest --tests '*ItSecurityTest'` проходит.

## Phase 4: US1 — чтение (P1) 🎯 MVP

- [ ] T039 [P] [US1] Модель и порты — `DS/domain/model/Device.java`, `DS/domain/port/in/GetDeviceUseCase.java`,
  `DS/domain/port/out/DeviceReadRepository.java`, `DS/domain/port/out/DeviceCache.java`,
  `DS/domain/exception/DeviceNotFoundException.java` (FR-003)
- [ ] T040 [US1] Тесты сервиса чтения: попадание; промах — запись в кэш; версия реплики ниже `floor` — не кэшируется;
  ошибка кэша — чтение из базы; нет строки, удалено, нет карточки — `DeviceNotFoundException` —
  `DST/domain/service/DeviceQueryServiceTest.java` (FR-003, FR-018, FR-021)
- [ ] T041 [US1] Сервис чтения и счётчик `device_cache_requests_total{result=hit|miss|stale_replica|error}` —
  `DS/domain/service/DeviceQueryService.java`, `DS/domain/port/out/DeviceServiceMetrics.java`,
  `DS/support/MicrometerDeviceServiceMetrics.java` (FR-003, FR-023)
- [ ] T042 [P] [US1] Чтение из `devices_read` (правило «карточка есть» — data-model.md), `connectTimeout`/`socketTimeout` из
  окружения — `DS/adapter/out/postgres/JdbcDeviceReadRepository.java` (FR-003, FR-019)
- [ ] T043 [P] [US1] Кэш: ключ `device:v1:{deviceId}`, скрипт `put-if-fresh.lua` (data-model.md), TTL `DEVICE_CACHE_TTL`,
  тайм-аут `REDIS_TIMEOUT`, Sentinel в профиле `docker` — `DS/adapter/out/redis/RedisDeviceCache.java`,
  `DSR/redis/put-if-fresh.lua` (FR-003, FR-018, FR-020)
- [ ] T044 [P] [US1] Маппер `Device` → `DeviceResponse` (MapStruct, незаполненное поле — ошибка сборки); тест —
  `DS/adapter/in/web/DeviceMapper.java`, `DST/adapter/in/web/DeviceMapperTest.java` (FR-007)
- [ ] T045 [US1] `getDeviceById`: 200 / 404 `DEVICE_NOT_FOUND` / 503 `READ_MODEL_UNAVAILABLE` —
  `DS/adapter/in/web/DeviceController.java`, `DS/adapter/in/web/ProblemDetailsAdvice.java` (FR-003, FR-006)
- [ ] T046 [US1] Интеграционные тесты: `givenDevice_whenFirstGet_thenCachedWithTtl`, `givenCached_whenGet_thenNoDbQuery`,
  `givenNoDeviceOrDeletedOrNoCard_whenGet_thenNotFound`, `givenFloorAboveReplicaVersion_whenGet_thenNotCached`,
  `givenRedisDown_whenGet_thenOk`, `givenResponse_thenOnlyContractFields` — `DST/it/ItDeviceReadTest.java`
  (FR-003, FR-007, FR-018, FR-021, SC-003)

**Checkpoint**: `./gradlew :services:device-service:check` проходит.

## Phase 5: US2 — изменение (P1)

- [ ] T047 [P] [US2] Порты команд — `DS/domain/port/in/ManageDeviceUseCase.java`,
  `DS/domain/port/out/DeviceCommandPublisher.java`, `DS/domain/model/DeviceCommand.java`,
  `DS/domain/exception/CommandNotAcceptedException.java` (FR-001)
- [ ] T048 [US2] Тесты сервиса команд (UPDATE): устройство есть — команда с новым `eventId`, временем приёма, полями
  запроса; нет или удалено — `DeviceNotFoundException`, публикации нет; сбой публикации — `CommandNotAcceptedException` —
  `DST/domain/service/DeviceCommandServiceTest.java` (FR-001, FR-002, FR-005)
- [ ] T049 [US2] Сервис команд: `update`; счётчики `device_commands_accepted_total{operation}`,
  `device_commands_rejected_total{operation,code}`, таймер `device_command_publish_seconds` —
  `DS/domain/service/DeviceCommandService.java`, `DS/support/MicrometerDeviceServiceMetrics.java` (FR-001, FR-005, FR-023)
- [ ] T050 [P] [US2] Сборка `DeviceInfo` из команды (таблица «Сообщение device-service» в data-model.md); тест —
  `DS/adapter/out/kafka/DeviceInfoFactory.java`, `DST/adapter/out/kafka/DeviceInfoFactoryTest.java` (FR-001)
- [ ] T051 [US2] Публикация: ключ `deviceId`, `acks=all`, идемпотентность, `auto.register.schemas=false`, ожидание
  подтверждения не дольше `DEVICE_COMMAND_SEND_TIMEOUT`, наблюдение (трассировка) включено —
  `DS/adapter/out/kafka/KafkaDeviceCommandPublisher.java`, `DS/config/KafkaConfig.java`, `DSR/application.yaml` (FR-001, FR-002, FR-024)
- [ ] T052 [US2] `updateDevice`: 202, `Location`, тело `DeviceCommandAccepted`; 400 / 404 / 503 `COMMAND_NOT_ACCEPTED` —
  `DS/adapter/in/web/DeviceController.java`, `DS/adapter/in/web/ProblemDetailsAdvice.java` (FR-001, FR-002, FR-006)
- [ ] T053 [US2] Интеграционные тесты: `givenDevice_whenPut_thenUpdateCommandPublished` (ключ, `operation`, `eventType`,
  одно сообщение), `givenInvalidBody_whenPut_thenBadRequestAndNoMessage`, `givenNoDevice_whenPut_thenNotFoundAndNoMessage`,
  `givenKafkaDown_whenPut_thenCommandNotAccepted` — `DST/it/ItDeviceCommandsTest.java` (FR-001, FR-002, FR-005)

## Phase 6: US3 — создание (P2)

- [ ] T054 [US3] Тесты и `create`: устройство есть — `DeviceAlreadyExistsException`; нет или удалено — команда `CREATE` —
  `DST/domain/service/DeviceCommandServiceTest.java`, `DS/domain/service/DeviceCommandService.java`,
  `DS/domain/exception/DeviceAlreadyExistsException.java` (FR-001, FR-004, FR-005)
- [ ] T055 [US3] `createDevice`: 202 с `Location: /api/v1/devices/{deviceId}`; 409 `DEVICE_ALREADY_EXISTS` —
  `DS/adapter/in/web/DeviceController.java`, `DS/adapter/in/web/ProblemDetailsAdvice.java` (FR-001, FR-005)
- [ ] T056 [US3] Интеграционные тесты: `givenNoDevice_whenPost_thenCreateCommandPublished`,
  `givenDevice_whenPost_thenConflictAndNoMessage`, `givenNoDeviceIdOrTooLong_whenPost_thenBadRequest` —
  `DST/it/ItDeviceCommandsTest.java` (FR-001, FR-004, FR-005)

## Phase 7: US4 — удаление (P2)

- [ ] T057 [US4] Тесты и `delete`: команда `DELETE` без `device`; нет или удалено — `DeviceNotFoundException` —
  `DST/domain/service/DeviceCommandServiceTest.java`, `DS/domain/service/DeviceCommandService.java` (FR-001, FR-005)
- [ ] T058 [US4] `deleteDevice`: 202 / 404 — `DS/adapter/in/web/DeviceController.java` (FR-001)
- [ ] T059 [US4] Интеграционный тест `givenDevice_whenDelete_thenDeleteCommandPublished` — `DST/it/ItDeviceCommandsTest.java`
  (FR-001, FR-016)

**Checkpoint**: `./gradlew :services:device-service:check` проходит, покрытие строк ≥ 70 %.

## Phase 8: Polish & Cross-Cutting

- [ ] T060 Сервис в Compose: два экземпляра (`deploy.replicas: 2`, без `container_name`, порты `DEVICE_SERVICE_PORTS`),
  healthcheck по liveness, зависимости (Kafka, Schema Registry, Proxy, Sentinel, Keycloak — `healthy`,
  `schema-register` — завершён), все переменные plan.md — `INF/docker-compose.yaml`, `.env.example`, `Makefile`
  (цели `ds-test`, `ds-it`, `ds-coverage`) (FR-008, FR-025)
- [ ] T061 (R13) Поднять стенд `make up`, записать память контейнеров (`docker stats --no-stream`) в `specs/001-device-service/research.md` (FR-008)
- [ ] T062 [P] (R7) Проверка стенда по quickstart.md одной командой (токен получен с хоста, сервис проверяет его в
  сети Compose): токен → POST → GET → PUT → GET (новые данные ≤ 5 с) →
  DELETE → GET 404 → запрос без токена 401; сессии `device_reader` только на репликах; код 0 при успехе —
  `services/device-service/scripts/quick-check.sh` (100755), `Makefile` (цель `ds-quick-check`) (SC-001, SC-002, FR-019)
- [ ] T063 [P] Клиентская библиотека против запущенного сервиса: `DevicesApi` через `HttpServiceProxyFactory`, токен
  Keycloak — `DST/it/ItClientTest.java` (FR-012)
- [ ] T064 (R12) Полный путь PUT → Kafka → device-collector-service → реплика → GET с новыми данными за 5 с —
  `DST/it/ItEndToEndTest.java` (FR-026, SC-001)
- [ ] T065 [P] Метрики и логи device-service в наблюдаемости: цель Prometheus, метка сервиса в Alloy —
  `INF/config/prometheus/prometheus.yml`, `INF/config/alloy/config.alloy` (FR-023, FR-024)
- [ ] T066 [P] Панель device-service: частота, ошибки, p95/p99, команды по операциям, доля попаданий кэша, время
  публикации, отказы 401/403; исход `ignored` и сброс кэша — на панели device-collector-service —
  `INF/config/grafana/dashboards/device-service.json`, `INF/config/grafana/dashboards/device-collector.json` (FR-023)
- [ ] T067 (R11) На стенде с `PROFILES=observability`: одна трасса PUT содержит спаны device-service, отправки в
  `devices`, обработки в device-collector-service и запроса к базе; логи обоих сервисов находятся в Loki по `traceId`; результат —
  в `specs/001-device-service/research.md` (FR-024)
- [ ] T068 [P] Отказ Redis на стенде: остановить основной узел — GET и обработка `devices` продолжаются без перезапуска
  сервисов; результат — в `specs/001-device-service/research.md` (FR-020, FR-021)
- [ ] T069 README: запуск, получение сервисного токена, примеры запросов, новые переменные и цели `make` —
  `README.md` (FR-010)
- [ ] T070 [P] Привести к коду пункты прежних секций и ссылку на критерии — `docs/definition-of-done.md`;
  диаграммы — `diagrams/*.mmd` (перерендерить изменённые PNG)
- [ ] T071 **ЧЕЛОВЕК** (необратимо) Опубликовать `device-service-client`: `make ds-client-release`; проверить артефакт в
  Nexus (FR-012)
- [ ] T072 **ЧЕЛОВЕК** Снимки панелей Grafana и одной сквозной трассы — `docs/screenshots/`
- [ ] T073 `converge` до `Converged`; результат — раздел Convergence этого файла

## Dependencies & Execution Order

- Phase 1 → Phase 2 → Phase 3 (US5) → Phase 4 (US1) → Phase 5 (US2) → Phase 6 (US3), Phase 7 (US4) → Phase 8.
- T013 (человек) блокирует T014, T023 и всё, что собирается с avros 1.3.0; до него агент работает на одноразовом
  стенде со своим Nexus (AGENTS.md).
- T017 → T024…T033; T018 → T019; T020 → T021, T030, T034, T043; T022 → T035.
- US5 раньше остальных историй: все методы защищены. US1 раньше US2–US4: предварительные проверки читают модель чтения.
- T064 — после T033 и T053; T071 и T072 — перед сдачей.

## Implementation Strategy

MVP — Phase 1–4: защищённое чтение карточки. Затем по одной истории; после каждой фазы — Checkpoint и коммит,
который делает человек после проверки по задачам.

## Дополнительно (после приёма основного объёма, отдельным решением)

Защита кэша от одновременного промаха, несуществующих ключей и массового истечения TTL; `/api/v2` рядом с v1,
v1 — `deprecated: true` в контракте.
