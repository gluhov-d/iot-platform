# Tasks: device-service — управление устройствами

**Input**: [spec.md](spec.md), [plan.md](plan.md), [data-model.md](data-model.md), [research.md](research.md),
[device-service.yaml](../../services/device-service-client/src/main/openapi/device-service.yaml), [quickstart.md](quickstart.md), правила агента —
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

- [X] T001 Подключить подпроекты `services:device-service-client` и `services:device-service` — `settings.gradle.kts` (FR-025)
- [X] T002 [P] Добавить `COPY` файлов `build.gradle.kts` обоих новых подпроектов — `avros/Dockerfile`,
  `ECS/Dockerfile`, `services/device-collector-service/Dockerfile`, `services/db-migrator/Dockerfile` (FR-025)
- [X] T003 Перенести контракт: `git mv specs/001-device-service/contracts/device-service.yaml
  CL/src/main/openapi/device-service.yaml`; поправить ссылки — `specs/001-device-service/{spec,plan,research,tasks}.md` (FR-011)
- [X] T004 Библиотека клиента: `java-library`, OpenAPI Generator 7.17.0 в два прогона (модели: `spring`,
  `useBeanValidation=true`, `useJakartaEe=true`, `openApiNullable=false`; интерфейс: `library=spring-http-interface`
  без моделей), версии зависимостей — `api(platform("org.springframework.boot:spring-boot-dependencies:…"))` без плагина
  dependency-management, пакеты `…deviceservice.api` и `…deviceservice.api.model`, вывод в `build/generated`, версия проекта =
  `info.version` контракта (задача сборки падает при расхождении) — `CL/build.gradle.kts` (FR-011)
- [X] T005 Публикация в Nexus (`maven-publish`, репозиторий и запрет перезаписи версии — как в `avros/build.gradle.kts`),
  цели `ds-client-check`, `ds-client-release` — `CL/build.gradle.kts`, `CL/scripts/publish.sh` (100755), `Makefile` (FR-012)
- [X] T006 Сборка сервиса: зависимости из plan.md, `project(":services:device-service-client")`, JaCoCo 0.70 по строкам,
  задача `integrationTest` (`It*Test`), `testImplementation(project(":services:db-migrator"))` — как у
  device-collector-service — `services/device-service/build.gradle.kts` (FR-026)
- [X] T007 [P] Точка входа и конфигурация только из переменных окружения (plan.md, «Переменные окружения»), JSON-логи с
  `traceId`/`spanId` — `DS/DeviceServiceApplication.java`, `DSR/application.yaml`, `DSR/application-docker.yaml`,
  `DSR/logback-spring.xml` (FR-024, FR-025)
- [X] T008 [P] Dockerfile в два этапа (JDK 25 → JRE 25, слои Spring Boot, непривилегированный пользователь) — по образцу
  `services/device-collector-service/Dockerfile` — `services/device-service/Dockerfile` (FR-025)
- [X] T009 [P] Правила слоёв (порты и адаптеры) — `DST/architecture/ArchitectureTest.java` (FR-026)
- [X] T010 Каркас контроллера `implements DevicesApi` (методы отвечают 501, без `@RequestMapping`; тело — `@Valid` на
  параметре реализации; `deviceId` из пути проверяется явно через `jakarta.validation.Validator` — ограничений на
  `@PathVariable` в реализации нет, research.md R4) и тест маршрутов: четыре метода контракта находят обработчик,
  невалидное тело и `deviceId` длиннее 128 — 400 — `DS/adapter/in/web/DeviceController.java`,
  `DST/adapter/in/web/DeviceControllerRoutingTest.java` (FR-011)

**Checkpoint**: `./gradlew :services:device-service-client:build :services:device-service:test` проходит;
`git status --short` не содержит сгенерированных файлов.

## Phase 2: Foundational

### Контракт сообщений

- [X] T011 Поле `operation` (JSON — дословно из data-model.md) и версия `1.3.0` — `avros/avro/device-info.avsc`,
  `avros/build.gradle.kts` (FR-013)
- [X] T012 Проверить совместимость без публикации: `make avros-check` — код 0 (FR-013)
- [X] T013 **ЧЕЛОВЕК** (необратимо: версия в Nexus не перезаписывается) Опубликовать и зарегистрировать avros 1.3.0:
  `make avros-up`; проверить `curl -s localhost:8081/subjects/devices-value/versions/latest` (FR-013)
- [X] T014 [P] events-collector-service: `avrosVersion = "1.3.0"`; тест «1.2.0 читается схемой 1.3.0 с `operation = null`
  и наоборот» — `ECS/build.gradle.kts`,
  `ECS/src/test/java/com/github/gluhov/iotplatform/eventscollector/service/contract/ContractCompatibilityTest.java` (FR-014)
- [X] T015 [P] Тема `devices` без сжатия (ADR-0011): `cleanup.policy=delete`,
  `retention.ms=${KAFKA_DEVICES_RETENTION_MS}` при создании и `kafka-configs --alter` для существующей темы
  (там же снимается `min.compaction.lag.ms`); переменная в задаче `kafka-init` и `.env.example` (2592000000) —
  `INF/init/kafka/init-topics.sh`, `INF/docker-compose.yaml`, `.env.example` (FR-022).
  Проверка: `make topics` показывает `cleanup.policy=delete` и `retention.ms`; повторный запуск `kafka-init` — код 0
- [X] T016 [P] Восстановление: если копия старше `KAFKA_DEVICES_RETENTION_MS` (время из manifest) — предупреждение
  о возможной потере изменений до остановки сервисов — `INF/config/postgres/shard-restore.sh` (FR-022)

### База данных и чтение из реплик

- [X] T017 Миграция (SQL — дословно из data-model.md) —
  `services/db-migrator/src/main/resources/db/migration/devices/V3__device_management.sql` (FR-015, FR-016)
- [X] T018 [P] Роль `device_reader` (`LOGIN`, пароль `SHARD_READER_PASSWORD`, без `pg_read_all_data` — research.md R5;
  идемпотентно, как `exporter`); `db-migrator` запускается после `shard-replication-init` (права роли выдаёт V3) — `INF/init/shards/shard-replication-init.sh`, `INF/docker-compose.yaml`, `.env.example` (FR-019)
- [X] T019 Логическая база `devices_read` на репликах и пользователь только для неё (data-model.md,
  «ShardingSphere-Proxy»); Proxy стартует после готовности обеих реплик (`depends_on: service_healthy`): без реплики он
  не запускается (ADR-0004) — `INF/config/shardingsphere/database-devices_read.yaml`,
  `INF/config/shardingsphere/global.yaml`, `INF/docker-compose.yaml`, `.env.example` (`DEVICES_READ_DB_USER`,
  `DEVICES_READ_DB_PASSWORD`). Проверка: `SELECT` под пользователем чтения проходит, `INSERT` и подключение к базе
  `devices` отклоняются; на основных узлах нет сессий `device_reader` (FR-019)

### Redis через Sentinel

- [X] T020 (R10, R14) `redis` (основной), `redis-replica`, `redis-sentinel-1..3` (кворум 2, имя `platform-redis`,
  `maxmemory-policy volatile-lru`); у сети `iot-net` подсеть `IOT_NET_SUBNET`, у узлов Redis и Sentinel статические
  адреса, Sentinel следит за основным узлом по адресу, не по имени (research.md R9); узел Redis при старте спрашивает
  роль у Sentinel; healthcheck, порядок старта — `INF/docker-compose.yaml`, `INF/config/redis/redis-entrypoint.sh`
  (100755), `INF/config/redis/sentinel-entrypoint.sh` (100755), `.env.example` (`IOT_NET_SUBNET`, `REDIS_MASTER_IP`,
  `REDIS_REPLICA_IP`, `REDIS_SENTINEL_IPS`, `REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_NODES`, `REDIS_MAXMEMORY`),
  `Makefile` (цель `redis-cli` — к основному узлу).
  Проверка: остановить `redis` — Sentinel назначает основной узел, `redis-cli` через Sentinel отвечает; запустить
  `redis` снова — он становится репликой; `docker restart` нового основного узла — он остаётся основным (FR-020)
- [X] T021 events-collector-service: подключение через Sentinel в профиле `docker`; тесты и профиль `local` — один узел —
  `ECS/src/main/resources/application-docker.yaml`, `INF/docker-compose.yaml` (блок `events-collector-service`) (FR-020)

### Keycloak

- [X] T022 Realm `iot-platform`: клиент `device-service` (роли `devices.read`, `devices.write` — в `roles.client`),
  `device-service-consumer` (client credentials, обе роли — через `users[]` с `serviceAccountClientId`, audience mapper
  `device-service`), `device-service-reader` (только `devices.read`); секреты — `${…}` из окружения; срок токена 5 мин.
  Keycloak — в базовом стенде (без профиля), хранилище `dev-file`, `--import-realm`, единый `iss` (`KC_HOSTNAME`,
  `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`), логи в JSON, healthcheck по `/health/ready` — `INF/config/keycloak/iot-platform-realm.json`,
  `INF/docker-compose.yaml`, `.env.example` (`KEYCLOAK_REALM`, `KEYCLOAK_ISSUER_URI`, `KEYCLOAK_JWK_SET_URI`,
  `DEVICE_SERVICE_CONSUMER_SECRET`, `DEVICE_SERVICE_READER_SECRET`). Проверка: токен по quickstart.md с хоста и из сети
  Compose имеет один `iss`, `aud` содержит `device-service`; повторный `make up` — без ошибок (FR-010)

### device-collector-service

- [X] T023 `avrosVersion = "1.3.0"`, `spring-boot-starter-data-redis`, Redis в тестах —
  `services/device-collector-service/build.gradle.kts` (FR-015)
- [X] T024 Модель: `CommandType` (`CREATE`, `UPDATE`, `DELETE`); `DeviceChange` + команда; `StoredDevice` + `version`,
  `managedAt`, `deletedBy`; `ProcessingOutcome` + `IGNORED`; `ProcessingResult` + `version`. Перед правкой
  `grep -rn "new StoredDevice(\|new DeviceChange(\|new ProcessingResult(" services/device-collector-service/src` —
  поправить все вызовы — `DCS/domain/model/{CommandType,DeviceChange,StoredDevice,ProcessingOutcome,ProcessingResult}.java`,
  `DCST/data/TestData.java`, `DCST/support/InMemoryDeviceRepository.java` (FR-015)
- [X] T025 Тесты правил — по тесту на строку таблицы FR-015 (1–11) и на FR-017:
  `givenCreate_whenNoDevice_thenInserted`, `givenCreate_whenDeleted_thenRestored`, `givenUpdate_whenNoDevice_thenInserted`,
  `givenUpdate_whenExists_thenAdminFieldsReplaced`, `givenUpdate_whenDeleted_thenIgnored`,
  `givenDelete_whenExists_thenDeletedByAdmin`, `givenDelete_whenNoDevice_thenTombstone`,
  `givenOlderCommand_whenNewerApplied_thenStale`, `givenRouterEvent_whenDeletedByAdmin_thenIgnored`,
  `givenRouterEvent_whenManaged_thenOnlyDeviceDataUpdated`, `givenRouterEvent_whenNotManaged_thenAppliedAsBefore`,
  `givenSameCommandTwice_thenDuplicate`, `givenRouterClockAhead_whenAdminUpdate_thenApplied` —
  `DCST/service/domain/service/DeviceRegistryServiceTest.java` (FR-015, FR-016, FR-017)
- [X] T026 [P] Проверка сообщения: `UNKNOWN` и `CREATE`/`UPDATE` без `device` — невалидно; `DELETE` без `device` — валидно;
  тесты — `DCS/domain/service/DeviceInfoValidator.java`, `DCST/service/domain/service/DeviceInfoValidatorTest.java` (FR-015)
- [X] T027 [P] Перенос `operation` в `DeviceChange`; тесты — `DCS/domain/service/DeviceInfoMapper.java`,
  `DCST/service/domain/service/DeviceInfoMapperTest.java` (FR-015)
- [X] T028 Правила применения (data-model.md, «Правила device-collector-service»): `outcomeOf`, `toRecord`, `apply`;
  тесты T025 проходят — `DCS/domain/service/DeviceRegistryService.java` (FR-015, FR-016, FR-017)
- [X] T029 Запросы: чтение `version`, `managed_at`, `deleted_by` под блокировкой; запись полей администратора и данных
  устройства раздельно; `version + 1`; `RETURNING current_database(), version` — `DCS/domain/port/out/DeviceRepository.java`,
  `DCS/adapter/out/postgres/JdbcDeviceRepository.java`, `DCST/support/InMemoryDeviceRepository.java` (FR-015)
- [X] T030 Сброс кэша: порт, адаптер Redis со скриптом `raise-floor.lua` (data-model.md: два ключа — кэш и порог
  `device:v1:floor:{deviceId}`, срок порога `DEVICE_CACHE_FLOOR_TTL`, срок кэша не продлевается), подключение через Sentinel в
  профиле `docker`, счётчик `devices_cache_invalidations_total{result}`; тесты «успех» и «Redis недоступен — исключения нет» —
  `DCS/domain/port/out/DeviceCacheInvalidator.java`, `DCS/adapter/out/redis/RedisDeviceCacheInvalidator.java`,
  `DCSR/redis/raise-floor.lua`, `DCSR/application.yaml`, `DCSR/application-docker.yaml`,
  `DCS/domain/port/out/DeviceMetrics.java`, `DCS/support/MetricsFacade.java`,
  `DCST/adapter/out/redis/RedisDeviceCacheInvalidatorTest.java` (FR-018, FR-021)
- [X] T031 Вызов сброса после `COMMIT` и только при `APPLIED`; ошибка Redis не меняет результат обработки; тесты —
  `DCS/domain/service/DeviceRegistryService.java`, `DCST/service/domain/service/DeviceRegistryServiceTest.java` (FR-018, FR-021)
- [X] T032 [P] Совместимость 1.2.0 ↔ 1.3.0 — `DCST/service/contract/ContractCompatibilityTest.java` (FR-013)
- [X] T033 Интеграционные тесты: Redis в базовом классе; `givenCreateUpdateDelete_whenConsumed_thenApplied`,
  `givenAdminDelete_whenRouterEventArrives_thenStillDeleted`, `givenSameCommandTwice_whenConsumed_thenAppliedOnce`,
  `givenUnknownOperation_whenConsumed_thenDeadLettered`, `givenCachedDevice_whenUpdateConsumed_thenCacheEvicted`,
  `givenRedisDown_whenConsumed_thenApplied` — `DCST/it/AbstractItTest.java`, `DCST/it/ItDeviceOperationsTest.java`
  (FR-015, FR-016, FR-018, FR-021)
- [X] T034 Переменные `REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_NODES`, `REDIS_PASSWORD`, `REDIS_TIMEOUT`, зависимость от
  Sentinel — `INF/docker-compose.yaml` (блок `device-collector-service` целиком) (FR-020)

**Checkpoint**: `./gradlew :services:events-collector-service:check :services:device-collector-service:check` проходит;
на стенде `make up`, затем `make check-local` и `make dcs-quick-check` — код 0.

## Phase 3: US5 — доступ только для сервисов (P1)

- [X] T035 [US5] База интеграционных тестов: Kafka, Schema Registry (схема `devices-value` регистрируется заранее),
  Redis, PostgreSQL со схемой от db-migrator, Keycloak с realm из `INF/config/keycloak`; помощник токенов (сервисный,
  только чтение, пользовательский, без аудитории, истёкший — пользователь и клиенты для них создаются через Admin API) —
  `DST/it/AbstractItTest.java`, `DST/it/KeycloakTokens.java`, `services/device-service/src/test/resources/application-test.yaml` (FR-026)
- [X] T036 [US5] Тест: без токена — 401; без аудитории — 401; истёкший — 401; пользовательский с ролью и аудиторией —
  403 `SERVICE_TOKEN_REQUIRED`; сервисный без `devices.write` на PUT — 403 `FORBIDDEN`; сервисный с ролью — не 401/403;
  тело ошибок — Problem Details — `DST/it/ItSecurityTest.java` (FR-009, FR-006, SC-004)
- [X] T037 [US5] Цепочка безопасности: `STATELESS`, GET — `devices.read`, POST/PUT/DELETE — `devices.write`,
  технические точки `/actuator/**` — на отдельном management-порту без токена (`DEVICE_SERVICE_MANAGEMENT_PORT`, на хост не публикуется), основной порт их не отдаёт; ключи — `KEYCLOAK_JWK_SET_URI`, `iss` сверяется с `KEYCLOAK_ISSUER_URI`; проверки
  `exp`/`nbf`, `aud`, сервисной учётной записи (в токене есть `client_id` и нет `sid`); роли из
  `resource_access.device-service.roles`; тайм-ауты запроса ключей — `DS/adapter/in/web/security/SecurityConfig.java`,
  `DS/adapter/in/web/security/AudienceValidator.java`, `DS/adapter/in/web/security/ServiceAccountValidator.java`,
  `DS/adapter/in/web/security/KeycloakRolesConverter.java` + unit-тесты рядом в `DST/adapter/in/web/security/` (FR-009)
- [X] T038 [US5] Ошибки: перечисление кодов (значения — `Problem.code` контракта), 401/403 и исключения MVC в Problem
  Details с `code` и `traceId`, без стека — `DS/adapter/in/web/ErrorCode.java`, `DS/adapter/in/web/ProblemDetailsAdvice.java`,
  `DS/adapter/in/web/security/ProblemAuthenticationEntryPoint.java`, `DS/adapter/in/web/security/ProblemAccessDeniedHandler.java`,
  `DST/adapter/in/web/ProblemDetailsAdviceTest.java` (FR-006)

**Checkpoint**: `./gradlew :services:device-service:integrationTest --tests '*ItSecurityTest'` проходит.

## Phase 4: US1 — чтение (P1) 🎯 MVP

- [X] T039 [P] [US1] Модель и порты — `DS/domain/model/Device.java`, `DS/domain/port/in/GetDeviceUseCase.java`,
  `DS/domain/port/out/DeviceReadRepository.java`, `DS/domain/port/out/DeviceCache.java`,
  `DS/domain/exception/DeviceNotFoundException.java` (FR-003)
- [X] T040 [US1] Тесты сервиса чтения: попадание; промах — запись в кэш; версия реплики ниже `floor` — не кэшируется;
  ошибка кэша — чтение из базы; нет строки, удалено, нет карточки — `DeviceNotFoundException` —
  `DST/domain/service/DeviceQueryServiceTest.java` (FR-003, FR-018, FR-021)
- [X] T041 [US1] Сервис чтения и счётчик `device_cache_requests_total{result=hit|miss|stale_replica|error}` —
  `DS/domain/service/DeviceQueryService.java`, `DS/domain/port/out/DeviceServiceMetrics.java`,
  `DS/support/MicrometerDeviceServiceMetrics.java` (FR-003, FR-023)
- [X] T042 [P] [US1] Чтение из `devices_read` (правило «карточка есть» — data-model.md), `connectTimeout`/`socketTimeout` из
  окружения — `DS/adapter/out/postgres/JdbcDeviceReadRepository.java` (FR-003, FR-019)
- [X] T043 [P] [US1] Кэш: ключ `device:v1:{deviceId}` (TTL `DEVICE_CACHE_TTL`) и ключ порога
  `device:v1:floor:{deviceId}`, скрипт `put-if-fresh.lua` (data-model.md: читает порог из второго ключа, при успешной
  записи удаляет его), тайм-аут `REDIS_TIMEOUT`, Sentinel в профиле `docker` —
  `DS/adapter/out/redis/RedisDeviceCache.java`, `DSR/redis/put-if-fresh.lua` (FR-003, FR-018, FR-020)
- [X] T044 [P] [US1] Маппер `Device` → `DeviceResponse` (MapStruct, незаполненное поле — ошибка сборки); тест —
  `DS/adapter/in/web/DeviceMapper.java`, `DST/adapter/in/web/DeviceMapperTest.java` (FR-007)
- [X] T045 [US1] `getDeviceById`: 200 / 404 `DEVICE_NOT_FOUND` / 503 `READ_MODEL_UNAVAILABLE` —
  `DS/adapter/in/web/DeviceController.java`, `DS/adapter/in/web/ProblemDetailsAdvice.java` (FR-003, FR-006)
- [X] T046 [US1] Интеграционные тесты: `givenDevice_whenFirstGet_thenCachedWithTtl`, `givenCached_whenGet_thenNoDbQuery`,
  `givenNoDeviceOrDeletedOrNoCard_whenGet_thenNotFound`, `givenFloorAboveReplicaVersion_whenGet_thenNotCached`,
  `givenRedisDown_whenGet_thenOk`, `givenResponse_thenOnlyContractFields` — `DST/it/ItDeviceReadTest.java`
  (FR-003, FR-007, FR-018, FR-021, SC-003)

**Checkpoint**: `./gradlew :services:device-service:check` проходит.

## Phase 5: US2 — изменение (P1)

- [X] T047 [P] [US2] Порты команд — `DS/domain/port/in/ManageDeviceUseCase.java`,
  `DS/domain/port/out/DeviceCommandPublisher.java`, `DS/domain/model/DeviceCommand.java`,
  `DS/domain/exception/CommandNotAcceptedException.java` (FR-001)
- [X] T048 [US2] Тесты сервиса команд (UPDATE): устройство есть — команда с новым `eventId`, временем приёма, полями
  запроса; нет или удалено — `DeviceNotFoundException`, публикации нет; сбой публикации — `CommandNotAcceptedException` —
  `DST/domain/service/DeviceCommandServiceTest.java` (FR-001, FR-002, FR-005)
- [X] T049 [US2] Сервис команд: `update`; счётчики `device_commands_accepted_total{operation}`,
  `device_commands_rejected_total{operation,code}`, таймер `device_command_publish_seconds` —
  `DS/domain/service/DeviceCommandService.java`, `DS/support/MicrometerDeviceServiceMetrics.java` (FR-001, FR-005, FR-023)
- [X] T050 [P] [US2] Сборка `DeviceInfo` из команды (таблица «Сообщение device-service» в data-model.md); тест —
  `DS/adapter/out/kafka/DeviceInfoFactory.java`, `DST/adapter/out/kafka/DeviceInfoFactoryTest.java` (FR-001)
- [X] T051 [US2] Публикация: ключ `deviceId`, `acks=all`, идемпотентность, `auto.register.schemas=false`, ожидание
  подтверждения не дольше `DEVICE_COMMAND_SEND_TIMEOUT`, наблюдение (трассировка) включено —
  `DS/adapter/out/kafka/KafkaDeviceCommandPublisher.java`, `DS/config/KafkaConfig.java`, `DSR/application.yaml` (FR-001, FR-002, FR-024)
- [X] T052 [US2] `updateDevice`: 202, `Location`, тело `DeviceCommandAccepted`; 400 / 404 / 503 `COMMAND_NOT_ACCEPTED` —
  `DS/adapter/in/web/DeviceController.java`, `DS/adapter/in/web/ProblemDetailsAdvice.java` (FR-001, FR-002, FR-006)
- [X] T053 [US2] Интеграционные тесты: `givenDevice_whenPut_thenUpdateCommandPublished` (ключ, `operation`, `eventType`,
  одно сообщение), `givenInvalidBody_whenPut_thenBadRequestAndNoMessage`, `givenNoDevice_whenPut_thenNotFoundAndNoMessage`,
  `givenKafkaDown_whenPut_thenCommandNotAccepted` — `DST/it/ItDeviceCommandsTest.java` (FR-001, FR-002, FR-005)

## Phase 6: US3 — создание (P2)

- [X] T054 [US3] Тесты и `create`: устройство есть — `DeviceAlreadyExistsException`; нет или удалено — команда `CREATE` —
  `DST/domain/service/DeviceCommandServiceTest.java`, `DS/domain/service/DeviceCommandService.java`,
  `DS/domain/exception/DeviceAlreadyExistsException.java` (FR-001, FR-004, FR-005)
- [X] T055 [US3] `createDevice`: 202 с `Location: /api/v1/devices/{deviceId}`; 409 `DEVICE_ALREADY_EXISTS` —
  `DS/adapter/in/web/DeviceController.java`, `DS/adapter/in/web/ProblemDetailsAdvice.java` (FR-001, FR-005)
- [X] T056 [US3] Интеграционные тесты: `givenNoDevice_whenPost_thenCreateCommandPublished`,
  `givenDevice_whenPost_thenConflictAndNoMessage`, `givenNoDeviceIdOrTooLong_whenPost_thenBadRequest` —
  `DST/it/ItDeviceCommandsTest.java` (FR-001, FR-004, FR-005)

## Phase 7: US4 — удаление (P2)

- [X] T057 [US4] Тесты и `delete`: команда `DELETE` без `device`; нет или удалено — `DeviceNotFoundException` —
  `DST/domain/service/DeviceCommandServiceTest.java`, `DS/domain/service/DeviceCommandService.java` (FR-001, FR-005)
- [X] T058 [US4] `deleteDevice`: 202 / 404 — `DS/adapter/in/web/DeviceController.java` (FR-001)
- [X] T059 [US4] Интеграционный тест `givenDevice_whenDelete_thenDeleteCommandPublished` — `DST/it/ItDeviceCommandsTest.java`
  (FR-001, FR-016)

**Checkpoint**: `./gradlew :services:device-service:check` проходит, покрытие строк ≥ 70 %.

## Phase 8: Polish & Cross-Cutting

- [X] T060 Сервис в Compose: два экземпляра (`deploy.replicas: 2`, без `container_name`, порты `DEVICE_SERVICE_PORTS`),
  healthcheck по liveness, зависимости (Kafka, Schema Registry, Proxy, Sentinel, Keycloak — `healthy`,
  `schema-register` — завершён), все переменные plan.md — `INF/docker-compose.yaml`, `.env.example`, `Makefile`
  (цели `ds-test`, `ds-it`, `ds-coverage`) (FR-008, FR-025)
- [X] T061 (R13) Поднять стенд `make up`, записать память контейнеров (`docker stats --no-stream`) в `specs/001-device-service/research.md` (FR-008)
- [X] T062 [P] (R7) Проверка стенда по quickstart.md одной командой (токен получен с хоста, сервис проверяет его в
  сети Compose): токен → POST → GET → PUT → GET (новые данные ≤ 5 с) →
  DELETE → GET 404 → запрос без токена 401; сессии `device_reader` только на репликах; код 0 при успехе —
  `services/device-service/scripts/quick-check.sh` (100755), `Makefile` (цель `ds-quick-check`) (SC-001, SC-002, FR-019)
- [X] T063 [P] Клиентская библиотека против запущенного сервиса: `DevicesApi` через `HttpServiceProxyFactory`, токен
  Keycloak — `DST/it/ItClientTest.java` (FR-012)
- [X] T064 (R12) Полный путь PUT → Kafka → device-collector-service → реплика → GET с новыми данными за 5 с —
  `DST/it/ItEndToEndTest.java` (FR-026, SC-001)
- [X] T065 [P] Метрики и логи device-service в наблюдаемости: цель Prometheus, метка сервиса в Alloy —
  `INF/config/prometheus/prometheus.yml`, `INF/config/alloy/config.alloy` (FR-023, FR-024)
- [X] T066 [P] Панель device-service: частота, ошибки, p95/p99, команды по операциям, доля попаданий кэша, время
  публикации, отказы 401/403; исход `ignored` и сброс кэша — на панели device-collector-service —
  `INF/config/grafana/dashboards/device-service.json`, `INF/config/grafana/dashboards/device-collector.json` (FR-023)
- [X] T067 (R11) На стенде с `PROFILES=observability`: одна трасса PUT содержит спаны device-service, отправки в
  `devices`, обработки в device-collector-service и запроса к базе; логи обоих сервисов находятся в Loki по `traceId`; результат —
  в `specs/001-device-service/research.md` (FR-024)
- [X] T068 [P] Отказ Redis на стенде: остановить основной узел — GET и обработка `devices` продолжаются без перезапуска
  сервисов; результат — в `specs/001-device-service/research.md` (FR-020, FR-021)
- [X] T069 README: запуск, получение сервисного токена, примеры запросов, новые переменные и цели `make` —
  `README.md` (FR-010)
- [X] T070 [P] Привести к коду пункты прежних секций и ссылку на критерии — `docs/definition-of-done.md`;
  диаграммы — `diagrams/*.mmd` (перерендерить изменённые PNG)
- [X] T071 **ЧЕЛОВЕК** (необратимо) Опубликовать `device-service-client`: `make ds-client-release`; проверить артефакт в
  Nexus (FR-012)
- [X] T072 **ЧЕЛОВЕК** Снимки панелей Grafana и одной сквозной трассы — `docs/screenshots/`
- [X] T073 `converge` до `Converged`; результат — раздел Convergence этого файла

## Dependencies & Execution Order

- Phase 1 → Phase 2 → Phase 3 (US5) → Phase 4 (US1) → Phase 5 (US2) → Phase 6 (US3), Phase 7 (US4) → Phase 8.
- T013 (человек) блокирует T014, T023 и всё, что собирается с avros 1.3.0; до него агент работает на одноразовом
  стенде со своим Nexus (AGENTS.md).
- T017 → T024…T033; T018 → T019; T020 → T021, T030, T034, T043; T022 → T035.
- US5 раньше остальных историй: все методы защищены. US1 раньше US2–US4: предварительные проверки читают модель чтения.
- T064 — после T033 и T053; T071 и T072 — перед сдачей.
- Phase 15 и Phase 16 (ревью PR #5) не зависят друг от друга; Phase 16 — отдельный коммит.

## Implementation Strategy

MVP — Phase 1–4: защищённое чтение карточки. Затем по одной истории; после каждой фазы — Checkpoint и коммит,
который делает человек после проверки по задачам.

## Дополнительно (после приёма основного объёма, отдельным решением)

Защита кэша от одновременного промаха, несуществующих ключей и массового истечения TTL; `/api/v2` рядом с v1,
v1 — `deprecated: true` в контракте.

## Phase 9: Convergence

Найдено проходом `converge` (аудит кода против spec.md, plan.md, tasks.md, конституции; три независимых блока: FR-001…012, FR-013…022,
FR-023…026 + конституция). Решения человека: неизвестный путь, 405 и 415 остаются с кодом `VALIDATION_FAILED` (контракт не меняется);
событие роутера `DELETE` без снимка для управляемого устройства игнорируется; порог покрытия events-collector-service поднимается до 70 %.

- [X] T074 Тесты чужой подписи и чужого издателя: токен, подписанный другим ключом, и токен другого realm/`iss` — 401 `UNAUTHORIZED` — `DST/it/ItSecurityTest.java`, `DST/it/KeycloakTokens.java` per FR-009, US5/AC1 (partial)
- [X] T075 Пользовательский токен получает 403 `SERVICE_TOKEN_REQUIRED` только если это единственная ошибка проверки; токен с истёкшим сроком, чужим `iss` или без аудитории — 401 `UNAUTHORIZED` независимо от типа; тесты — `DS/adapter/in/web/security/ProblemAuthenticationEntryPoint.java`, `DST/adapter/in/web/security/ProblemAuthenticationEntryPointTest.java`, `DST/it/ItSecurityTest.java` per FR-009, US5/AC1–AC2 (partial)
- [X] T076 Сброс кэша и при исходе `DUPLICATE` (повторная доставка после сбоя между COMMIT и сбросом): порог `floor` поднимается до версии строки; тест — `DCS/domain/service/DeviceRegistryService.java`, `DCST/service/domain/service/DeviceRegistryServiceTest.java` per FR-018, ADR-0003 (partial)
- [X] T077 Событие роутера `DELETE` без снимка для управляемого устройства — `IGNORED` (удаляет только администратор); тест `givenRouterDeleteWithoutSnapshot_whenManaged_thenIgnored`; строка 12 в таблице FR-015 и пояснение в ADR-0005 (по решению человека) — `DCS/domain/service/DeviceRegistryService.java`, `DCST/service/domain/service/DeviceRegistryServiceTest.java`, `specs/001-device-service/spec.md`, `docs/adr/0005-soft-delete-and-manual-priority.md` per FR-016, FR-015 (partial)
- [X] T078 Измерить покрытие строк events-collector-service и поднять порог JaCoCo до 0.70, если сборка проходит; поправить «65 %» в `Makefile`, `docs/definition-of-done.md` — `ECS/build.gradle.kts` per Constitution VII (contradicts)
- [X] T079 device-service: тайм-аут получения соединения пула и `initialization-fail-timeout` — из переменных окружения (значения в `.env.example`, передача в `docker-compose.yaml`) — `DSR/application.yaml`, `.env.example`, `INF/docker-compose.yaml` per Constitution IV, AGENTS «Конфигурация» (partial)
- [X] T080 Защита от коммита копий `.env`: `.gitignore` — `/.env.*` кроме `.env.example` — `.gitignore` per FR-025, Constitution IV (partial)
- [X] T081 **ЧЕЛОВЕК** Удалить `.env.bak2` из рабочей копии (в нём значения паролей) per FR-025 (partial)
- [X] T082 Переименовать тесты по соглашению `givenX_whenY_thenZ`: `givenResponse_thenOnlyContractFields`, `givenSameCommandTwice_thenDuplicate` — `DST/it/ItDeviceReadTest.java`, `DCST/service/domain/service/DeviceRegistryServiceTest.java` per AGENTS «Соглашения кода» (contradicts); ссылки на имена в tasks.md (T025, T046) не менять
- [X] T083 Тесты `deviceId` с пробелом и `/` в пути и в теле: 400 `VALIDATION_FAILED` — `DST/adapter/in/web/DeviceControllerRoutingTest.java` per FR-004 (partial)
- [X] T084 Диаграмма `ds-read-sequence.mmd` приведена к коду (кэш читает только `body`, `floor` проверяет `put-if-fresh.lua` при записи); перерендерить PNG — `diagrams/ds-read-sequence.mmd`, `diagrams/ds-read-sequence.png` per plan (contradicts)
- [X] T085 README: известное ограничение — 405, 415 и неизвестный путь отдаются с кодом `VALIDATION_FAILED` (решение человека); инвариант `KAFKA_PRODUCER_MAX_BLOCK_MS` ≤ `DEVICE_COMMAND_SEND_TIMEOUT`; раздел 22.4 — исключения «без значений по умолчанию» (`DEVICE_COLLECTOR_CONSUMER_GROUP`, `COMPOSE_PROJECT_NAME`); устаревший комментарий «читает схемой 1.1.0» в `DCST/service/contract/ContractCompatibilityTest.java` — `README.md`, `DCST/service/contract/ContractCompatibilityTest.java` per FR-002, FR-006, FR-025 (partial)

## Phase 10: Convergence

Повторный проход `converge` (после Phase 9): T074–T085 выполнены, найдены следы правок. Решения человека сохраняются
(405/415/неизвестный путь — код `VALIDATION_FAILED`; D11 — снимок старше `last_state_at` для управляемого устройства остаётся `STALE` по шапке FR-015).

- [X] T086 405, 415 и 406 отдаются с собственным статусом и кодом `VALIDATION_FAILED`, а не `500 INTERNAL_ERROR` (обработчики `HttpRequestMethodNotSupportedException`, `HttpMediaTypeNotSupportedException`, `HttpMediaTypeNotAcceptableException` сохраняют статус исключения); тесты на 405 и 415 — `DS/adapter/in/web/ProblemDetailsAdvice.java`, `DST/adapter/in/web/ProblemDetailsAdviceTest.java`, `DST/it/ItSecurityTest.java` или `DST/adapter/in/web/DeviceControllerRoutingTest.java` per FR-006 (contradicts)
- [X] T087 Тест: токен с верной подписью, но чужим `iss`, — 401 `UNAUTHORIZED` (проверка `iss` изолирована от проверки подписи) — `DST/adapter/in/web/security/` (unit-тест декодера или валидаторов) per FR-009, US5/AC1 (partial)
- [X] T088 Диаграммы: `dcs-operations-sequence.mmd` — сброс кэша при `APPLIED` и `DUPLICATE`; `dcs-duplicate-sequence.mmd` — шаг подъёма `floor` при `DUPLICATE`; `ds-read-sequence.mmd` — условие «карточка есть» (`deleted_at IS NULL AND tenant_id IS NOT NULL`) и ответ 404; перерендерить PNG — `diagrams/dcs-operations-sequence.{mmd,png}`, `diagrams/dcs-duplicate-sequence.{mmd,png}`, `diagrams/ds-read-sequence.{mmd,png}` per FR-018, FR-003 (contradicts)
- [X] T089 README и тесты приведены к коду: порог 70 % у всех трёх сервисов (строки 703, 731); переменные `DEVICES_READ_DB_POOL_CONNECTION_TIMEOUT_MS`, `DEVICES_READ_DB_INIT_FAIL_TIMEOUT_MS` в таблице 22.4; список `IGNORED` в 19.1 дополнен событием роутера `DELETE` без снимка для управляемого устройства; `DisplayName`/имя метода «1.1.0» в `ContractCompatibilityTest`; устаревший javadoc «ответ 501» в `DeviceControllerRoutingTest` — `README.md`, `DCST/service/contract/ContractCompatibilityTest.java`, `DST/adapter/in/web/DeviceControllerRoutingTest.java` per T078, T079, FR-015, T085 (partial)

## Phase 11: Правки по ревью дизайна (PR #4)

Семь замечаний преподавателя (17.11.2026). Замечания 1, 2, 5, 6, 7 — про диаграммы: код действующим правилам
соответствует (`managed_at` ставит каждая применённая команда, включая `DELETE` и создающий запись `UPDATE`;
при недоступном Redis чтение отвечает 404, если устройства нет), поэтому в код идут только недостающие тесты.
Замечания 3 и 4 меняют конфигурацию и код; решения человека: [ADR-0011](../../docs/adr/0011-devices-topic-retention.md)
(тема `devices` хранит сообщения вместо сжатия, заменяет ADR-0010) и [ADR-0003](../../docs/adr/0003-cache-invalidation-by-owner.md)
(порог `floor` — отдельный ключ со своим сроком). Тест пишется раньше кода, который он проверяет.

Состояние на 2026-11-17: T090–T100 выполнены. Прогон на Mac: unit — device-service 87, device-collector-service 128,
events-collector-service 132; интеграционные — device-collector-service 18, device-service 42, падений нет.
Открыты T101 (переменные в локальном `.env` — шаг человека) и T102 (`converge`), а также прогон стенда из Checkpoint.

- [X] T090 Диаграммы: `managed_at = eventAt` у всех применённых команд; `dcs-success-sequence` — условие «устройство
  без ручных изменений и не удалено администратором» и сверка с `last_state_at`; `ds-write-sequence` — ветки отказа
  через `break`; `ds-read-sequence` — один путь чтения с исходами 200 / 404 / 503; `component-device-service` и
  `component-device-collector` — узел Redis master (чтение и Lua к нему, Sentinel только за адресом);
  `ds-cache-consistency` — отдельный ключ порога и случай «реплика отстаёт дольше TTL»; `containers`,
  `component-events-collector` — тема `devices` с политикой `delete`; перерендерить PNG —
  `diagrams/*.{mmd,png}` (замечания 1, 2, 5, 6, 7)
- [X] T091 Тема `devices`: `cleanup.policy=delete`, `retention.ms=${KAFKA_DEVICES_RETENTION_MS}` при создании и
  `kafka-configs --alter` для существующей, `--delete-config min.compaction.lag.ms`; переменная вместо
  `KAFKA_DEVICES_MIN_COMPACTION_LAG_MS` (2592000000) — `INF/init/kafka/init-topics.sh`, `INF/docker-compose.yaml`,
  `.env.example` (FR-022). Проверка: `make topics` показывает `cleanup.policy=delete` и `retention.ms`, настройки
  `min.compaction.lag.ms` нет; повторный запуск `kafka-init` — код 0 (замечание 4)
- [X] T092 Восстановление: возраст копии сравнивается с `KAFKA_DEVICES_RETENTION_MS` (текст предупреждения — про срок
  хранения, не про сжатие) — `INF/config/postgres/shard-restore.sh` (FR-022). Проверка: копия старше срока —
  предупреждение до остановки сервисов; переменной нет в `.env` — отказ с кодом 1 (замечание 4)
- [X] T093 Интеграционный тест порядка: после `DELETE` администратора и более позднего события роутера повторное
  чтение темы с начала (новая группа) оставляет устройство удалённым —
  `DCST/it/ItDeviceOperationsTest.java` (FR-022, FR-016, ADR-0011)
- [X] T094 [P] Тест запоздавшей команды: `DELETE` с временем 10:20 применён, `CREATE` с `eventAt` 10:15 — `STALE`,
  устройство остаётся удалённым; `UPDATE`, создавший запись, и `DELETE` без строки ставят `managed_at` —
  `DCST/service/domain/service/DeviceRegistryServiceTest.java` (FR-015, FR-017, замечание 1)
- [X] T095 Тесты порога в отдельном ключе (до кода T096, T097): `put-if-fresh` читает `floor` из
  `device:v1:floor:{deviceId}`, принятая запись удаляет ключ порога, отказ не продлевает TTL ключа кэша; `raise-floor`
  продлевает срок только ключа порога (`DEVICE_CACHE_FLOOR_TTL`) и не трогает TTL кэша —
  `DST/adapter/out/redis/RedisDeviceCacheTest.java`,
  `DCST/adapter/out/redis/RedisDeviceCacheInvalidatorTest.java` (FR-018, замечание 3)
- [X] T096 device-service: `put-if-fresh.lua` на два ключа (`KEYS[1]` кэш, `KEYS[2]` порог) с `DEL` порога при
  успешной записи; срок `DEVICE_CACHE_FLOOR_TTL` из окружения — `DSR/redis/put-if-fresh.lua`,
  `DS/adapter/out/redis/RedisDeviceCache.java`, `DSR/application.yaml`, `INF/docker-compose.yaml`, `.env.example` (FR-018)
- [X] T097 device-collector-service: `raise-floor.lua` на два ключа, `PEXPIRE` только ключа порога на
  `DEVICE_CACHE_FLOOR_TTL` — `DCSR/redis/raise-floor.lua`, `DCS/adapter/out/redis/RedisDeviceCacheInvalidator.java`,
  `DCSR/application.yaml`, `DCSR/application-docker.yaml`, `INF/docker-compose.yaml`, `.env.example` (FR-018)
- [X] T098 Интеграционные тесты порога: после применённой команды ключ порога есть, а срок его жизни больше
  `DEVICE_CACHE_TTL`; истёк ключ кэша, реплика отдаёт версию ниже порога — ответ 200 без записи в кэш
  (`givenFloorOutlivesCacheTtl_whenReplicaStale_thenNotCached`) — `DST/it/ItDeviceReadTest.java`,
  `DCST/it/ItDeviceOperationsTest.java` (FR-018, SC-001)
- [X] T099 [P] Тесты чтения: Redis недоступен и устройства нет (удалено, без карточки) — 404 `DEVICE_NOT_FOUND`;
  модель чтения недоступна при промахе — 503 `READ_MODEL_UNAVAILABLE` —
  `DST/domain/service/DeviceQueryServiceTest.java`, `DST/it/ItDeviceReadTest.java` (FR-003, FR-021, замечание 6)
- [X] T100 README и DoD по факту кода: тема `devices` (`delete`, срок хранения, переход существующей темы), два ключа
  кэша и `DEVICE_CACHE_FLOOR_TTL`, известное ограничение порога, таблицы переменных — `README.md`,
  `docs/definition-of-done.md` (FR-018, FR-022)
- [X] T101 **ЧЕЛОВЕК** Дописать в локальный `.env`: `KAFKA_DEVICES_RETENTION_MS`, `DEVICE_CACHE_FLOOR_TTL`; строку
  `KAFKA_DEVICES_MIN_COMPACTION_LAG_MS` удалить (агент `.env` не читает и не меняет)
- [X] T102 `converge` повторно до `Converged`; результат — в раздел «Результат converge» этого файла

**Checkpoint**: `./gradlew :services:device-service:check :services:device-collector-service:check
:services:events-collector-service:check` проходит; на стенде `make up`, затем `make check-local`,
`make dcs-quick-check`, `make ds-quick-check` — код 0; `make topics` показывает `cleanup.policy=delete` у `devices`.

## Phase 12: Смещения группы без тихого сброса и остатки проверки (18.11.2026)

Проверка реализации показала: у device-collector-service `auto-offset-reset: earliest`, поэтому смещение вне диапазона
темы (обработчик отстал дольше срока хранения) не даёт явной ошибки, как написано в ADR-0011, а молча переводит
обработчик на самое раннее сохранённое сообщение — замечание 4 ревью дизайна воспроизводится, только через 30 суток.
Решение человека: `auto-offset-reset: none` и явная установка смещения для раздела без него. Тест T093 читал тему
отдельным потребителем, а не группой сервиса, — повторное чтение не проверял.

Состояние на 2026-11-18: T103–T107 выполнены. Прогон на Mac: device-collector-service unit 134 / IT 20, device-service
unit 87 / IT 42 (`check` с порогом покрытия), `make up`, `check-local`, `dcs-quick-check`, `ds-quick-check` — код 0,
сквозной `e2eTest` (новая группа на чистом стенде) пройден. T101 выполнен (переменные в локальном `.env` есть, проверено по именам). Открыт T108 (`converge`).

- [X] T103 (R) Интеграционный тест до кода: раздел без смещения группы читается с начала; повторное чтение темы с
  начала после восстановления шарда оставляет `DELETE` администратора (заменяет проверку T093); смещение вне диапазона
  (`deleteRecords`) — обработчик остановлен, событие роутера не применено, health `DOWN`. На коде с `earliest`
  последний сценарий падает (обработчик продолжает работу) — `DCST/it/ItConsumerOffsetsTest.java`,
  `DCST/it/ItDeviceOperationsTest.java`, `DCST/it/AbstractItTest.java` (FR-022, ADR-0011)
- [X] T104 `auto-offset-reset: none`; `DevicesConsumerOffsets` — перед первым `poll` (событие `ConsumerStartingEvent`)
  разделам без смещения группы через Admin выставляется самое раннее смещение, health `DOWN` при смещении вне диапазона; `KafkaConfig.DevicesErrorHandler` останавливает обработчик при
  `OffsetOutOfRangeException`; unit-тесты — `DCSR/application.yaml`, `DCS/config/DevicesConsumerOffsets.java`,
  `DCS/config/KafkaConfig.java`, `DCST/config/DevicesConsumerOffsetsTest.java`, `DCST/config/KafkaConfigTest.java`
  (FR-022, ADR-0011)
- [X] T105 Документы по T104: ADR-0011 (статус «принято», раздел о смещениях группы, неверное «брокер отвергает
  явной ошибкой» исправлено), FR-022, README раздел 5, предупреждение `shard-restore.sh` — `docs/adr/0011-devices-topic-retention.md`,
  `specs/001-device-service/spec.md`, `README.md`, `INF/config/postgres/shard-restore.sh` (FR-022)
- [X] T106 `dcs-success-sequence`: ветка «не этот случай» завершается `break` (после `opt` поток на схеме шёл дальше
  к `APPLIED`), ответ базы — строка или пусто; перерендерить PNG — `diagrams/dcs-success-sequence.{mmd,png}` (замечание 2)
- [X] T107 Остатки проверки: `ItSecurityTest` — конкретный статус и код вместо `isNotIn(401, 403)`; FR-024 — JSON с
  `traceId` у сервисов платформы, сторонние компоненты в своём формате; ссылки на ADR `0001…0011` в spec.md и plan.md;
  README без номеров модулей, снимок `grafana-device-collector-operations.png` — `DST/it/ItSecurityTest.java`,
  `specs/001-device-service/spec.md`, `specs/001-device-service/plan.md`, `README.md`, `docs/screenshots/`
- [X] T108 `converge` до `Converged` (вместе с T102); результат — в раздел «Результат converge»

**Checkpoint**: `./gradlew :services:device-collector-service:check :services:device-service:check` проходит; на стенде
`make up`, `make check-local`, `make dcs-quick-check`, `make ds-quick-check` — код 0.

## Результат converge

Текущий статус: Phase 15 — **Converged** (2026-11-19, проход 7); Phase 16 (метрики через Alloy) открыта, `converge` по ней — T140.
До ревью реализации: **Converged** (2026-11-18, проход 6 после Phase 13 и Phase 14).
Проходы 1–3 (2026-11-15/16) закончились `Converged`; статус снят ревью дизайна (17.11.2026) и проверкой реализации (18.11.2026).
Статус снова снят ревью реализации (PR #5, 19.11.2026): открыты Phase 15 и Phase 16. Phase 15 закрыта проходом 7 (19.11.2026); открыта Phase 16 (метрики через Alloy).

| Проход | Находки | Итог |
|---|---|---|
| 1 | 3 независимых аудита (FR-001…012, FR-013…022, FR-023…026 + конституция): 20 находок, из них в работу взяты 12 (Phase 9, T074–T085) | исправлено |
| 2 | 12 находок (D1–D12), главная: 405/415 отдавались как `500 INTERNAL_ERROR` вместо своего статуса (Phase 10, T086–T089) | исправлено |
| 3 | 4 мелких остатка (N1–N4: комментарии, одна строка README, перепрогон тестов) | исправлено; новых расхождений нет |
| 4 | после Phase 11–12: 3 аудита, находки перепроверены; 6 приняты (Phase 13, T109–T114), из них 2 решения человека (T109, T113), 2 ложных отклонены. Главное: длина значений `settings`/`attributes` не проверялась; первая команда при неотвечающем Schema Registry не получала ответа 20 с и дольше | исправлено |
| 5 | 3 аудита: 4 принятые находки (Phase 14, T116–T119: ADR-0006 о проверке `deviceId` коллектором, тестовый профиль, худший случай 503, формулировки исключения), 10 отклонено с причиной | исправлено |
| 6 | проверка правок Phase 14 по коду и документам, `ItSchemaRegistryDownTest` на новом тестовом профиле | новых расхождений нет |
| 7 | после Phase 15 (T122–T133), по изменённым местам: шесть замечаний ревью (SSL realm, ключи кэша, `null` в картах, раздел без перезапуска, предварительная проверка по кэшу; метрики через Alloy — Phase 16) сверены с кодом, тестами и документами; `grep` старых утверждений (`device:v1` в коде и действующих документах, «только по реплике», «без кэша», `LISTENER_RESTART`) пуст; новая переменная `DEVICES_CONSUMER_METADATA_MAX_AGE_MS` — в `.env.example`, Compose, `application.yaml`, `application-local.yaml`, `application-test.yaml`, README (разделы 19.3, 22.4), `plan.md`; в README и DoD обновлены числа тестов (найдено при проходе); аудиторы не привлекались | расхождений кроме чисел тестов нет; R16 не подтвердилось, принят R18 (решение человека, 19.11.2026) |

Решения человека, зафиксированные при converge: 405/406/415 и неизвестный путь отдаются с кодом `VALIDATION_FAILED` (README 22.8, контракт не менялся);
событие роутера `DELETE` без снимка для управляемого устройства — `IGNORED` (FR-015, строка 12); порог покрытия events-collector-service — 70 %;
тайм-ауты клиента Schema Registry в device-service — из окружения (T111); `deviceId` с пробелом или `/` из событий роутера через device-service
недоступен — известное ограничение (ADR-0006, README 22.8, T116).
Сознательно оставлено: снимок старше `last_state_at` для управляемого устройства даёт `STALE` (шапка FR-015); `infrastructure/.env` — отслеживаемая
символическая ссылка с первого модуля. Значения по умолчанию в `application.yaml` events-collector-service и
device-collector-service (код прошлых модулей, совпадают с `.env.example`, кроме пустого `REDIS_PASSWORD` для запуска без пароля; стенд передаёт переменные явно) и в Compose
у `COMPOSE_PROJECT_NAME`, `DEVICE_COLLECTOR_CONSUMER_GROUP` — исключение из правила AGENTS.md «Конфигурация» (T109, 18.11.2026).

Отчёты сборки на 19.11.2026 (Mac, Docker Desktop, `check --rerun`, после Phase 15): events-collector-service unit 132 / IT 33, device-collector-service 142 / 23,
device-service 100 / 49, падений нет; покрытие строк 98,2 % / 97,4 % / 96,8 %. Проверки стенда `make check-local`, `dcs-quick-check`, `ds-quick-check` — код 0.
Те же интеграционные тесты device-service и device-collector-service на Linux (VPS) проходят. Не проверялись: `ItEndToEndTest` (нужна остановка стенда и согласие с лицензией
Nexus) и Phase 16.

Отчёты сборки на 18.11.2026: events-collector-service unit 132 / IT 33, device-collector-service 134 / 20 (прогон на Mac после Phase 12),
device-service 90 / 44 (`check --rerun-tasks` после T110–T111)
(`check`: порог покрытия строк 70 %, у device-service 96 %), падений нет; сквозной тест `ItEndToEndTest` (метка `e2e`, Testcontainers `ComposeContainer`)
пройден на стенде разработчика: изменение видно на другой реплике за 0,4 с при лимите 5 с. Проверки стенда: `make ds-quick-check`, `make dcs-quick-check`
(код 0); трасса одного PUT через device-service, Kafka и device-collector-service до PostgreSQL — в Tempo, логи по `traceId` — в Loki.

На момент прохода 3 не были закрыты задачи человека T071 (публикация `device-service-client` в Nexus) и T081 (удалить `.env.bak2`); позже обе выполнены — отмечены в списке задач.


## Phase 13: Convergence

Проход 4 (T108, 2026-11-18): три независимых аудита (FR-001…012; FR-013…022 с Phase 12; FR-023…026, конституция, документы),
находки перепроверены по коду. FR-013…FR-022 и Phase 12 (T103–T107) расхождений не дали. Не проверялось (нужен запуск):
поведение Spring Kafka при `OffsetOutOfRangeException`, Schema Registry без ответа, публикация в Nexus.

- [X] T109 **ЧЕЛОВЕК** решает по значениям по умолчанию: (а) в Compose — `COMPOSE_PROJECT_NAME` (`:-iot-platform`, строки 1
  и 1293), `DEVICE_COLLECTOR_CONSUMER_GROUP` (`:-device-collector`, строка 699); (б) в `application.yaml` device-collector-service
  (15) и events-collector-service (20) — тайм-ауты, пулы, попытки, темы, `MANAGEMENT_OTLP_ENDPOINT` с `localhost`. AGENTS.md
  требует переменные без значений, README (строки 1009 и 1257) описывает эти значения как допустимые. Либо убрать значения
  отдельной задачей (тестам и `application-local.yaml` задать их явно), либо записать исключение в «Результат converge» и в
  AGENTS.md (AGENTS.md «Конфигурация»; contradicts). Решение: исключение — AGENTS.md «Соглашения кода», «Результат converge»
- [X] T110 Сервер применяет ограничения контракта `settings`/`attributes` (до 100 ключей, значение до 1024 символов):
  тест до кода — `givenTooManySettings_whenPut_thenValidationFailed`, `givenTooLongAttributeValue_whenPost_thenValidationFailed`
  (сначала падают); проверка без правки генерируемого кода — `DST/adapter/in/web/`, `DST/it/` (FR-011, US2/AC3; partial)
- [X] T111 (R) Проверить, ограничен ли `send()` в `KafkaDeviceCommandPublisher` сроком `DEVICE_COMMAND_SEND_TIMEOUT`, когда Schema
  Registry не отвечает (`docker pause`): тест `It*` — `503 COMMAND_NOT_ACCEPTED` в срок, сообщений нет. Не подтвердилось —
  остановиться, предложить тайм-ауты клиента Schema Registry через переменные окружения (новые имена — в `.env.example`) —
  `DS/adapter/out/kafka/KafkaDeviceCommandPublisher.java`, `DSR/application.yaml` (FR-002, Constitution IV; partial)
- [X] T112 Числа тестов в README (строки 679 и 717: 126 / 84 / 39) и в «Результат converge» (126/17, 84/39) привести к прогону
  18.11.2026 (device-collector-service unit 134 / IT 20, device-service unit 87 / IT 42) — из отчётов сборки (AGENTS.md
  «Соглашения кода»; partial)
- [X] T113 Статусы ADR-0001…0009 «предложено» при реализованных решениях — **ЧЕЛОВЕК** переводит в «принято» с датой ответа
  преподавателя по дизайну (2026-11-17); контекст ADR не меняется — он описывает положение на момент решения —
  `docs/adr/0001…0009` (spec DoD «Этап дизайна»; partial)
- [X] T114 `settings.gradle.kts`: `include("services:device-collector-service")` указан дважды (строки 12 и 15) — убрать
  повтор (unrequested)
- [X] T115 `converge` повторно до `Converged`; результат — в раздел «Результат converge» (T102, T108)

Отклонено при перепроверке: `GET /api/v1/devices/{deviceId}` у device-collector-service — есть с прошлого модуля, вне рамок
фичи; клиент `device-service-reader` в realm — назван в задаче Phase 2 (Keycloak). Значения по умолчанию в `application.yaml`
коллекторов конституцию не нарушают: все переменные есть в `.env.example` и передаются в Compose, значения совпадают; поэтому
не отдельные задачи агента, а решение человека (T109).

## Phase 14: Convergence

Проход 5 (T115, 2026-11-18): три аудита по тем же диапазонам, каждая находка перепроверена по коду и Git. T110–T114
подтверждены: валидатор длины значений и `@Size(max = 100)` на геттерах моделей, тайм-ауты клиента Schema Registry,
числа тестов (отчёты на Mac 132/33, 134/20; device-service 90/44), `settings.gradle.kts` без дубля. Отклонено при
перепроверке: healthcheck events-collector-service и device-collector-service по общему `/actuator/health` (с прошлых
модулей; health `DOWN` при смещении вне диапазона использует ADR-0011); `REDIS_PASSWORD:` и `APP_INSTANCE_ID` в
device-service (не адрес, не тайм-аут, не число попыток); `DEVICES_DB_*` и `HOSTNAME` вне `.env.example` (собирает Compose,
системная переменная); тайм-ауты Schema Registry у коллекторов (не путь запроса клиента, вне FR-002); Complexity
Tracking (исключение T109 — из AGENTS.md, не из конституции); устаревшие отчёты сборки в `build/` на VPS (на Mac — 134/20).

- [X] T116 **ЧЕЛОВЕК** решает по ADR-0006: «device-service проверяет формат так же, как device-collector-service: … без
  пробелов и `/`» неверно — `DeviceInfoValidator` проверяет только непустой `deviceId` и длину до 128 (так с прошлого
  модуля). Устройство `a/b` из события роутера попадает в реестр, но device-service отвечает на него `400`. Либо поправить
  формулировку ADR-0006 и назвать ограничение в README, либо добавить проверку в `DeviceInfoValidator` (такие события
  уйдут в `devices-dlt`) — `docs/adr/0006-device-id-assigned-by-client.md`,
  `DCS/domain/service/DeviceInfoValidator.java` (ADR-0006, FR-004; contradicts)
- [X] T117 Тестовый профиль device-service: сумма тайм-аутов клиента Schema Registry (1000 + 1000 мс) равна `send-timeout`
  (2 с), а инвариант README 22.4 требует «меньше» — `read` 500 мс —
  `services/device-service/src/test/resources/application-test.yaml` (FR-002, T111; partial)
- [X] T118 README 22.4: худший случай — метаданные Kafka ждут почти `KAFKA_PRODUCER_MAX_BLOCK_MS`, затем первый запрос к
  Schema Registry — 503 придёт позже `DEVICE_COMMAND_SEND_TIMEOUT` (до 5 + 3 с). Назвать это ограничение рядом с
  инвариантами — `README.md` (FR-002; partial)
- [X] T119 Формулировки исключения T109: значения по умолчанию коллекторов совпадают с `.env.example`, кроме пустого
  `REDIS_PASSWORD` (запуск без пароля); `plan.md` «Переменные окружения» — «без значений по умолчанию» с отсылкой к
  исключению — `AGENTS.md`, `tasks.md` «Результат converge», `plan.md` (T109; partial)
- [X] T120 `converge` повторно до `Converged`; результат — в раздел «Результат converge» (T102, T108, T115)

## Phase 15: Правки по ревью реализации (PR #5)

Шесть замечаний преподавателя (19.11.2026). Ошибки в артефактах, а не только в коде: ключи кэша и порога в spec.md,
data-model.md и ADR-0003 пересекались при допустимом `deviceId` (FR-004 разрешает `:`); `null` в `settings`/`attributes`
нигде не был определён; FR-022 проверялся только при запуске обработчика; режим SSL realm и окружение Docker Desktop в
research.md не учитывались; диаграммы описывали проверку записи через кэш, а код при недоступной реплике отвечал
`503`, даже если карточка лежит в кэше (решение: реплика — основной источник, кэш — только при её недоступности; T121
приводит диаграммы к прежнему коду, T129–T131 — новое поведение). Документы исправлены до кода (spec.md, сессия
2026-11-19; data-model.md, plan.md, research.md R15–R16, ADR-0001, ADR-0003, ADR-0011). Замечание о метриках через
Alloy — Phase 16, отдельным коммитом. Тест пишется раньше кода, который он проверяет, и сначала падает.

- [X] T121 Диаграммы — фактический путь предварительной проверки: `ds-write-sequence` — запрос к реплике без кэша,
  ветка `503 READ_MODEL_UNAVAILABLE`, заметка о `404` сразу после `POST`; `component-device-service` —
  `DeviceCommandService → JdbcDeviceReadRepository`; перерендерить PNG — `diagrams/ds-write-sequence.{mmd,png}`,
  `diagrams/component-device-service.{mmd,png}` (FR-005, замечание 6)
- [X] T122 (R15) Тест до кода: через Admin API у `iot-platform`, `master` и realm, который создаёт `KeycloakTokens`,
  `sslRequired` = `none` (сначала падает); затем `"sslRequired": "none"` в файле realm `iot-platform`; `master` —
  `execInContainer` с `kcadm.sh update realms/master -s sslRequired=NONE` после старта Keycloak и до первого
  административного токена; realm для проверки «чужого `iss`» создаётся с `sslRequired: none`. Стенд, Compose и порядок
  старта не меняются (`master` стенду не нужен). Не подтвердилось — остановиться, запасной вариант из research.md —
  `INF/config/keycloak/iot-platform-realm.json`, `DST/it/AbstractItTest.java`, `DST/it/KeycloakTokens.java`
  (FR-010, FR-026; замечание 1)
- [X] T123 [P] Тесты ключей до кода: `key("floor:dev-42")` не равен `floorKey("dev-42")` и ни один префикс не является началом
  другого — в обоих сервисах; на Redis (Testcontainers) кэш `floor:dev-42` записан `put-if-fresh`, затем `raise-floor` для
  `dev-42` проходит без `WRONGTYPE`, карточка `dev-42` удалена из кэша, а карточка `floor:dev-42` не тронута —
  `DST/adapter/out/redis/RedisDeviceCacheTest.java`, `DCST/adapter/out/redis/RedisDeviceCacheInvalidatorTest.java`,
  `DCST/it/ItDeviceOperationsTest.java` (FR-018; замечание 2)
- [X] T124 Ключи `device:cache:v1:{deviceId}` и `device:floor:v1:{deviceId}` в обоих сервисах (Lua-скрипты не меняются, ключи
  передаются в `KEYS`); тесты T123 и прежние тесты кэша проходят —
  `DS/adapter/out/redis/RedisDeviceCache.java`, `DCS/adapter/out/redis/RedisDeviceCacheInvalidator.java`, тесты с
  литералами ключей и комментарии с прежними именами — поиск по репозиторию, кроме исторических разделов `specs/`
  (`grep -rn 'device:v1' services/ infrastructure/`) (FR-018)
- [X] T125 [P] Тесты до кода: POST и PUT с `settings: null`, `attributes: null`, `{"k": null}` в `settings` и в `attributes` —
  `400 VALIDATION_FAILED`, поле в `errors[]`; публикации нет (порт публикации не вызван; в интеграционном тесте сообщений
  в теме нет); тело без `settings`/`attributes` — `202`, в сообщении пустые карты —
  `DST/adapter/in/web/DeviceControllerRoutingTest.java`, `DST/it/ItDeviceCommandsTest.java` (FR-011, US2/AC3; замечание 3)
- [X] T126 Проверка тела: `null` у карты и у её значений в `settings` и `attributes` — ошибка поля с кодом `NotNull`;
  тесты T125 проходят — `DS/adapter/in/web/ContractMapValuesValidator.java` (FR-011)
- [X] T127 (R18) Тесты до кода (R16 не подтвердилось: `NoOffsetForPartitionException` приходит из `onPartitionsAssigned`,
  контейнер останавливается; перезапуск с Admin на двух экземплярах не сходится): unit-тест обёртки `Consumer` —
  `NoOffsetForPartitionException` из `position(...)` → `seekToBeginning` этих разделов и повтор, возвращается позиция;
  `OffsetOutOfRangeException` пробрасывается без `seek`; остальные методы делегируются как есть. `ItConsumerOffsetsTest`:
  (а) один экземпляр — на работающем сервисе число разделов `devices` увеличено (Admin `createPartitions`), сообщение с
  ключом, попадающим в новый раздел, применено, контейнер не останавливался (нет `ConsumerStoppedEvent` с причиной
  `NO_OFFSET`), health `UP`; (б) два экземпляра в одной группе (второй — второй контекст Spring) — то же за 60 с, оба
  обработчика работают, health `UP` у обоих, три прогона; (в) регрессия: смещение вне диапазона — обработчик остановлен,
  событие не применено, health `DOWN`. Срок обновления метаданных потребителя в тестовом профиле — короткий. Не
  подтвердилось (Spring или клиент не принимает обёртку, либо (б) не сходится) — остановиться; запасной вариант из
  research.md — ограничение «один экземпляр» с перезапуском контейнера —
  `DCST/config/NoOffsetRecoveringConsumerTest.java`, `DCST/it/ItConsumerOffsetsTest.java`, `DCST/it/AbstractItTest.java`,
  `services/device-collector-service/src/test/resources/application-test.yaml` (FR-022; замечание 4)
- [X] T128 Раздел без смещения у работающей группы читается с начала при любом числе экземпляров (R18): обёртка `Consumer`
  подключена через `DefaultKafkaConsumerFactory.addPostProcessor` (перехват только `position(...)` и `poll(...)`);
  `OffsetOutOfRangeException` по-прежнему останавливает обработчик; health `DOWN`, если обработчик `devices` остановлен по
  любой причине; срок обновления метаданных потребителя — из переменной окружения `DEVICES_CONSUMER_METADATA_MAX_AGE_MS`
  (`.env.example`, `INF/docker-compose.yaml`); `DevicesConsumerOffsets` при старте остаётся, если тесты не покажут, что он
  лишний; unit-тесты — `DCS/config/NoOffsetRecoveringConsumer.java`, `DCS/config/KafkaConfig.java`,
  `DCS/config/DevicesConsumerOffsets.java`, `DCSR/application.yaml`, `DCST/config/KafkaConfigTest.java`,
  `DCST/config/DevicesConsumerOffsetsTest.java` (FR-022, ADR-0011). Удалить `DevicesListenerRestarter`,
  `DevicesListenerRestarterTest` и `DEVICES_LISTENER_RESTART_DELAY` (`application.yaml`, `application-local.yaml`,
  `application-test.yaml`, `.env.example`, `INF/docker-compose.yaml`). Проверка:
  `grep -rn 'LISTENER_RESTART\|ListenerRestarter' .` вне истории `specs/` пуст
- [X] T129 Тесты до кода: Proxy недоступен, карточка в кэше — `update`/`delete` публикуют команду, `create` —
  `DeviceAlreadyExistsException`; Proxy недоступен, карточки в кэше нет или кэш недоступен — `ReadModelUnavailableException`,
  публикации нет; реплика доступна — кэш не читается; интеграционные: Proxy на паузе (`docker pause`), карточка в кэше —
  `PUT` `202` и одно сообщение `UPDATE`, без карточки — `503 READ_MODEL_UNAVAILABLE`, сообщений нет —
  `DST/domain/service/DeviceCommandServiceTest.java`, `DST/it/ItDeviceCommandsTest.java` (FR-005; замечание 6)
- [X] T130 Предварительная проверка: `findById` по реплике; при `ReadModelUnavailableException` — `DeviceCache.find`
  (порт кэша, не `DeviceQueryService`: он при промахе снова пошёл бы в реплику и записал бы в кэш); счётчик
  `device_command_precheck_cache_total{operation,result=hit|miss|error}`; тесты T129 проходят —
  `DS/domain/service/DeviceCommandService.java`, `DS/domain/port/out/DeviceServiceMetrics.java`,
  `DS/support/MicrometerDeviceServiceMetrics.java` (FR-005, FR-023)
- [X] T131 Диаграммы по T130: `ds-write-sequence` — запрос к реплике, при её недоступности — кэш (`HGET body`): карточка
  есть — дальше, нет или Redis недоступен — `503`; `component-device-service` — `DeviceCommandService → JdbcDeviceReadRepository`
  и `DeviceCommandService -. реплика недоступна .-> RedisDeviceCache`; перерендерить PNG —
  `diagrams/ds-write-sequence.{mmd,png}`, `diagrams/component-device-service.{mmd,png}` (FR-005; замечание 6)
- [X] T132 Документы по T122–T131: ADR-0011 — выбранный способ (R16 → «проверено»), research.md R15/R16 — результат
  проверки; README — ключи кэша и переход со старых ключей, `null` в `settings`/`attributes`, раздел без перезапуска,
  `sslRequired: none` (стенд локальный, по HTTP; нелокальному развёртыванию нужны сертификат и `external`),
  предварительная проверка записи (реплика, при её недоступности — кэш) и счётчик
  `device_command_precheck_cache_total`, новые переменные; DoD; диаграммы с именами ключей (`grep -ln 'device:v1' diagrams/*.mmd`) — перерендерить
  PNG; `quickstart.md` — ключ кэша в строке проверки — `docs/adr/0011-devices-topic-retention.md`,
  `specs/001-device-service/research.md`, `specs/001-device-service/quickstart.md`, `README.md`,
  `docs/definition-of-done.md`, `diagrams/*.{mmd,png}` (FR-010, FR-011, FR-018, FR-022)
- [X] T133 **ЧЕЛОВЕК** По желанию — удалить на своём стенде прежние ключи `device:v1:*` (иначе истекают сами, пороги — до
  `DEVICE_CACHE_FLOOR_TTL`). Выполнено человеком 19.11.2026
- [X] T134 `converge` повторно до `Converged`; результат — в раздел «Результат converge»

**Checkpoint**: на Mac (Docker Desktop) `./gradlew :services:device-service:check :services:device-collector-service:check`
проходит, интеграционные тесты device-service в том числе; те же интеграционные тесты — на Linux (VPS); `make up`,
`make check-local`, `make dcs-quick-check`, `make ds-quick-check` — код 0.

## Phase 16: Метрики сервисов через Alloy (отдельный коммит)

Решение — [ADR-0012](../../docs/adr/0012-service-metrics-via-alloy.md), FR-023 (сессия 2026-11-19). Сервисы платформы:
events-collector-service, device-collector-service, device-service (все экземпляры). Экспортёры и сторонние
компоненты Prometheus опрашивает сам, как раньше.

- [ ] T135 (R17) Проверить на стенде с `PROFILES=observability` до переноса: Alloy (`prometheus.scrape` →
  `prometheus.remote_write`) передаёт метрики одного сервиса в Prometheus с `--web.enable-remote-write-receiver`, метки
  `job`/`instance` и ряд `up` совпадают с прежними. Не подтвердилось — остановиться, запасной вариант из research.md
  (FR-023)
- [ ] T136 Alloy: обнаружение экземпляров трёх сервисов, сбор `/actuator/prometheus` (у device-service — порт из
  `DEVICE_SERVICE_MANAGEMENT_PORT` через `sys.env`; у events-collector-service и device-collector-service — порт
  контейнера 8080, как прежде в `prometheus.yml`), метки `job` = имя сервиса, переименование `outbox_candidate_total` →
  `outbox_candidate_created_total`, `prometheus.remote_write` в Prometheus; Prometheus — приём remote write, задания трёх
  сервисов из `prometheus.yml` удалить; Alloy стартует после Prometheus, переменные портов передаются в Alloy —
  `INF/config/alloy/config.alloy`, `INF/config/prometheus/prometheus.yml`, `INF/docker-compose.yaml`, `.env.example` (FR-023)
- [ ] T137 Проверка на стенде с `PROFILES=observability`: `up{job=…}` = 1 для каждого экземпляра трёх сервисов (у
  device-service — два), в `prometheus.yml` их заданий нет, все запросы панелей events-collector, device-collector,
  device-service возвращают данные; результат — research.md R17 (FR-023)
- [ ] T138 Диаграммы: `containers.mmd` (метрики сервисов: Alloy → `/actuator/prometheus`, Alloy → Prometheus remote write),
  компонентные `component-events-collector`, `component-device-collector`, `component-device-service`; перерендерить PNG —
  `diagrams/*.{mmd,png}` (FR-023)
- [ ] T139 README и DoD: путь метрик сервисов, где смотреть состояние сбора (интерфейс Alloy), новые переменные —
  `README.md`, `docs/definition-of-done.md` (FR-023)
- [ ] T140 `converge` повторно до `Converged`; результат — в раздел «Результат converge»

**Checkpoint**: `make check`; стенд `make up PROFILES=observability` — проверки T137; `make check-local`,
`make dcs-quick-check`, `make ds-quick-check` — код 0.
