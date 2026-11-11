# Tasks: device-service — управление устройствами

**Input**: [spec.md](spec.md), [plan.md](plan.md), [data-model.md](data-model.md), [research.md](research.md),
[contracts/device-service.yaml](contracts/device-service.yaml)

Формат: `[ID] [P?] [Story] Описание (требования)`. `[P]` — можно параллельно (разные файлы). Каждая задача
закрывается с проверкой из DoD spec.md; один шаг = один коммит.

## Phase 0: Spike (research.md)

- [ ] T001 [P] Два прогона OpenAPI Generator в Gradle, компиляция на Spring Boot 4 (research #1) (FR-011)
- [ ] T002 Контроллер реализует `DevicesApi`: маршруты MVC и `@Valid` (research #2) (FR-011)
- [ ] T003 [P] Proxy 5.5.3: база `devices_read` на репликах, пользователь только для неё (research #3) (FR-019)
- [ ] T004 [P] Keycloak 26: импорт realm с секретами из env, единый `iss`, признак сервисного токена (research #4–6) (FR-009, FR-010)
- [ ] T005 [P] Redis Sentinel в Compose, Lettuce, failover без перезапуска, `volatile-lru` и аренда (research #7, #11) (FR-020)
- [ ] T006 Полный путь в Testcontainers `ComposeContainer` (research #8)
- [ ] T007 [P] Трасса HTTP → Kafka → listener (research #9) (FR-023)
- [ ] T008 Замер памяти стенда с Keycloak и двумя экземплярами (research #10) (FR-008)
- [ ] T009 Итоги spike — в research.md; при запасном варианте поправить plan.md и ADR

## Phase 1: Setup

- [ ] T010 Подпроекты `services/device-service-client` и `services/device-service` в `settings.gradle.kts`; строки
  `COPY services/device-service*/build.gradle.kts` во все Dockerfile
- [ ] T011 Перенести контракт в `services/device-service-client/src/main/openapi/device-service.yaml` (`git mv`),
  генерация DTO и `DevicesApi`, `maven-publish` в Nexus, `make ds-client-release` (FR-011, FR-012)
- [ ] T012 [P] Каркас device-service: `build.gradle.kts` (зависимости plan.md, JaCoCo 70 %, `integrationTest`),
  `application.yaml` только из env, JSON-логи, двухэтапный Dockerfile (FR-024)

## Phase 2: Foundational

- [ ] T013 avros 1.3.0: `operation` в `device-info.avsc`, `ContractCompatibilityTest`, `make avros-up` (FR-013)
- [ ] T014 [P] events-collector-service на avros 1.3.0, Redis через Sentinel (FR-014, FR-020)
- [ ] T015 [P] `V3__device_management.sql`, роль `device_reader` в `shard-replication-init` (FR-016, FR-019)
- [ ] T016 [P] Proxy: `database-devices_read.yaml`, пользователь чтения в `global.yaml`, `render-config.sh` (FR-019)
- [ ] T017 [P] Redis master + реплика + 3 Sentinel в compose, `sentinel.conf` (FR-020)
- [ ] T018 [P] Keycloak в базовом стенде: realm `iot-platform` (клиенты, роли, audience mapper), `--import-realm` (FR-010)
- [ ] T019 device-collector-service на avros 1.3.0: таблица правил (CREATE/UPDATE/DELETE, `IGNORED`, `managed_at`,
  `deleted_by`, `version`), unit `DeviceRegistryServiceTest` на каждую строку (FR-015, FR-016, FR-017)
- [ ] T020 device-collector-service: сброс кэша после `COMMIT` (Lua, Sentinel), сбой Redis не ломает обработку,
  `devices_cache_invalidations_total`; новая зависимость — `REDIS_SENTINEL_*`, `REDIS_PASSWORD`, `REDIS_TIMEOUT` в compose
  и `.env.example`, Redis в IT (FR-018, FR-021)
- [ ] T021 IT `ItDeviceOperationsTest`, `givenCachedDevice_whenUpdateConsumed_thenCacheEvicted` (FR-015–FR-018)

**Checkpoint**: команды, отправленные в `devices` вручную, применяются; кэш сбрасывается.

## Phase 3: US5 — доступ только для сервисов (P1)

- [ ] T022 [US5] `SecurityConfig` (`STATELESS`, роли), `ServiceAccountValidator`, `KeycloakRolesConverter`, валидатор `aud` (FR-009)
- [ ] T023 [US5] 401/403 в Problem Details, `ProblemDetailsAdvice` с `code` и `traceId`, `ProblemDetailsAdviceTest` (FR-006)
- [ ] T024 [US5] IT `ItSecurityTest` с Keycloak в Testcontainers: пять случаев US5 (SC-004)

## Phase 4: US1 — чтение (P1) 🎯 MVP

- [ ] T025 [US1] `JdbcDeviceReadRepository` (база `devices_read`, тайм-ауты) (FR-003, FR-019)
- [ ] T026 [US1] `RedisDeviceCache` (Lua: `version ≥ floor`, TTL), `RedisDeviceCacheTest` (FR-003, FR-018, FR-020)
- [ ] T027 [US1] `DeviceQueryService`: Cache-Aside, Redis недоступен — реплики, метрики `device_cache_requests_total` (FR-003, FR-021)
- [ ] T028 [US1] `DeviceController.getDeviceById`, маппер DTO (FR-007)
- [ ] T029 [US1] IT `ItDeviceReadTest`: промах, попадание, 404, реплика отстаёт, Redis остановлен (SC-003)

## Phase 5: US2 — изменение (P1)

- [ ] T030 [US2] `KafkaDeviceCommandPublisher` (Avro, ключ, `acks=all`, `send().get(тайм-аут)`, observation) (FR-001, FR-002)
- [ ] T031 [US2] `DeviceCommandService`: проверки, `eventId`, `eventTimeMs`, `202`/`503`, метрики команд (FR-001, FR-002, FR-005)
- [ ] T032 [US2] `DeviceController.updateDevice`; IT `ItDeviceCommandsTest` для UPDATE, Kafka остановлена → 503 (FR-002)

## Phase 6: US3 — создание (P2)

- [ ] T033 [US3] `createDevice`: `deviceId` от клиента, `409`, `Location`; IT для CREATE (FR-001, FR-004, FR-005)

## Phase 7: US4 — удаление (P2)

- [ ] T034 [US4] `deleteDevice`; IT для DELETE; событие роутера после удаления — по-прежнему 404 (FR-001, FR-016)

## Phase 8: Polish & Cross-Cutting

- [ ] T035 Compose: два экземпляра device-service, healthcheck, порядок старта, `.env.example` (FR-008, FR-024)
- [ ] T036 [P] `ItClientTest`: вызов запущенного сервиса через `DevicesApi` из `device-service-client` (FR-012)
- [ ] T037 `ItEndToEndTest`: PUT → Kafka → device-collector-service → реплика → GET (SC-001)
- [ ] T038 [P] Панель Grafana `device-service.json`, цели Prometheus, Alloy; проверка всех запросов панелей (FR-022)
- [ ] T039 [P] Трасса в Tempo от HTTP до `update devices`; логи JSON в Loki (FR-023)
- [ ] T040 [P] Проверка стенда: SC-002 (сессии `device_reader` только на репликах), failover Redis
- [ ] T041 README (запуск, сервисный токен `curl`, примеры запросов), DoD, диаграммы — по факту кода
- [ ] T042 `converge` до `Converged`; снимки Grafana и трассы

## Dependencies & Execution Order

- Phase 0 → всё остальное: результат spike может поменять plan.
- Phase 2: T013 → T014, T019; T015 → T016, T019; T017 → T014, T020; T019 → T020 → T021.
- US5 (Phase 3) → US1–US4: все методы защищены.
- US1 → US2–US4: предварительные проверки записи читают модель чтения.
- T037 — после T021 и T032.

## Дополнительно (после приёма основного, отдельным решением)

Защита кэша от одновременного промаха, несуществующих ключей и массового истечения TTL; `/api/v2` рядом с v1,
v1 — `deprecated: true` в спецификации.
