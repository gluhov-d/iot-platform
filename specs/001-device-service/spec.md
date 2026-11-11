# Feature Specification: device-service — управление устройствами

**Feature**: `001-device-service` | **Created**: 2026-11-11 | **Status**: На проверке дизайна
**Input**: администратор хочет открыть карточку устройства, исправить её, удалить устройство или завести новое
вручную (например, если событие от роутера не дошло). Сервис отдаёт REST API для этого; писатель реестра остаётся
один — device-collector-service.

План: [plan.md](plan.md) · Модель данных: [data-model.md](data-model.md) · Контракт: [contracts/device-service.yaml](contracts/device-service.yaml)
· Решения: [docs/adr](../../docs/adr/) · Диаграммы: `diagrams/containers.mmd`, `component-device-service.mmd`,
`ds-write-sequence.mmd`, `ds-read-sequence.mmd`, `ds-cache-consistency-sequence.mmd`, `dcs-operations-sequence.mmd`.

## Clarifications

### Session 2026-11-11

- Q: Кто назначает `deviceId` при ручном создании? → A: клиент. Это идентификатор физического устройства, тот же,
  что в событиях роутера; сервис проверяет только формат ([ADR-0006](../../docs/adr/0006-device-id-assigned-by-client.md)).
- Q: Кто и когда сбрасывает кэш, если запись идёт через Kafka, а реплика отстаёт? → A: device-collector-service после
  `COMMIT` поднимает в Redis нижнюю границу версии устройства; device-service кладёт в кэш данные реплики, только если
  их версия не ниже границы ([ADR-0003](../../docs/adr/0003-cache-invalidation-by-owner.md)).
- Q: Как device-collector-service удаляет и что будет, если после удаления придёт событие роутера? → A: удаление мягкое
  с отметкой «удалено администратором»; события роутера после него не возвращают устройство, вернуть можно только
  `POST` ([ADR-0005](../../docs/adr/0005-soft-delete-and-manual-priority.md)).
- Q: Как чтение попадает в реплики, а не в основные узлы? → A: отдельная логическая база Proxy `devices_read`, её
  источники — только реплики; у пользователя device-service доступа к базе записи нет
  ([ADR-0004](../../docs/adr/0004-read-from-replicas.md)).
- Q: Что значит `operation = null`? → A: сообщение не команда, а событие роутера; применяется по `eventType`, как
  раньше (для нового устройства это создание). Неизвестная читателю операция — `UNKNOWN`, сообщение в `devices-dlt`
  ([ADR-0002](../../docs/adr/0002-device-info-operation.md)).
- Q: PUT меняет все поля? → A: PUT заменяет целиком поля, которыми управляет администратор (имя, арендатор, владелец,
  тип, серийный номер, производитель, модель, статус, настройки, атрибуты). Прошивку, ОС, сенсоры и время активности
  сообщает устройство — в API они только для чтения.
- Q: Чья правка важнее: администратора или роутера? → A: после ручного изменения поля администратора роутер не
  перезаписывает; данные устройства роутер обновляет как раньше. Между двумя командами администратора — последняя
  по времени приёма ([ADR-0005](../../docs/adr/0005-soft-delete-and-manual-priority.md)).
- Q: Как ответить на пользовательский токен? → A: `403` с кодом `SERVICE_TOKEN_REQUIRED`: токен подлинный, но сервис
  принимает только сервисные учётные записи ([ADR-0007](../../docs/adr/0007-service-tokens-only.md)).

## User Scenarios & Testing

### User Story 1 — Чтение карточки устройства (P1) 🎯 MVP

Сервис-потребитель запрашивает устройство по `deviceId` и получает его текущие данные из модели чтения.

**Independent Test**: `GET /api/v1/devices/{deviceId}` с сервисным токеном для устройства, записанного
device-collector-service, — `200` с данными; повтор — из кэша.

1. **Given** устройство есть в реестре, кэш пуст, **When** GET, **Then** `200`, данные из реплики, ключ появился в Redis.
2. **Given** устройство в кэше, **When** GET, **Then** `200` без обращения к Proxy.
3. **Given** устройства нет или оно удалено, **When** GET, **Then** `404 DEVICE_NOT_FOUND`.
4. **Given** Redis недоступен, **When** GET, **Then** `200` из реплики.

### User Story 2 — Изменение карточки (P1)

**Independent Test**: PUT → `202`; в течение нескольких секунд GET возвращает новые данные.

1. **Given** устройство есть, **When** PUT с валидным телом, **Then** `202`, в `devices` сообщение `UPDATE` с ключом `deviceId`.
2. **Given** устройства нет или оно удалено, **When** PUT, **Then** `404`, сообщений нет.
3. **Given** тело не проходит проверку, **When** PUT, **Then** `400 VALIDATION_FAILED` со списком полей.
4. **Given** Kafka недоступна, **When** PUT, **Then** `503 COMMAND_NOT_ACCEPTED`, клиент может повторить.

### User Story 3 — Ручное создание (P2)

1. **Given** устройства нет, **When** POST с `deviceId`, **Then** `202`, `Location: /api/v1/devices/{deviceId}`, сообщение `CREATE`.
2. **Given** устройство уже есть, **When** POST, **Then** `409 DEVICE_ALREADY_EXISTS`.
3. **Given** устройство удалено администратором, **When** POST, **Then** `202`, устройство снова видно.

### User Story 4 — Удаление (P2)

1. **Given** устройство есть, **When** DELETE, **Then** `202`, затем GET — `404`.
2. **Given** устройство удалено администратором, **When** приходит событие роутера, **Then** GET по-прежнему `404`.

### User Story 5 — Доступ только для сервисов (P1)

1. Нет токена, подпись или издатель чужие, срок истёк, нет аудитории `device-service` — `401`.
2. Пользовательский токен (даже с ролью и аудиторией) — `403 SERVICE_TOKEN_REQUIRED`.
3. Сервисный токен без нужной роли — `403 FORBIDDEN`; с ролью — успех.

### Edge Cases

- Команда на запись прошла предварительную проверку по отстающей реплике, но владелец решает иначе (UPDATE
  удалённого) — `202` уже отдан, команда получает исход `IGNORED`; это видно в журнале и метриках device-collector-service.
- После компакции топика `devices` на ключ остаётся последнее сообщение: `UPDATE` неизвестного устройства вставляет
  его, `DELETE` без строки пишет надгробие.
- Реплика не догнала запись: GET отдаёт её данные, но в кэш их не кладёт.
- Redis недоступен при сбросе кэша: обработка сообщения не падает, устаревание ограничено TTL.
- Повтор POST/PUT/DELETE после сетевого сбоя: новое сообщение с новым `eventId`, результат тот же (PUT и DELETE
  идемпотентны по смыслу, повтор POST — `409` или то же состояние).

## Requirements

### Functional Requirements

- **FR-001**: `POST /api/v1/devices` публикует `DeviceInfo` с `operation = CREATE`, `PUT /api/v1/devices/{deviceId}` —
  `UPDATE`, `DELETE /api/v1/devices/{deviceId}` — `DELETE`; ключ — `deviceId`, `eventId` — новый UUID, `eventTimeMs` —
  время приёма; в базу сервис не пишет.
- **FR-002**: Ответ `202 Accepted` с `Location` — только после подтверждения Kafka (`acks=all`); нет подтверждения за
  `DEVICE_COMMAND_SEND_TIMEOUT` — `503`.
- **FR-003**: `GET /api/v1/devices/{deviceId}` — Cache-Aside: Redis, при промахе реплики шардов через Proxy; `200` или `404`.
- **FR-004**: `deviceId` задаёт клиент: 1–128 символов без пробелов и `/`.
- **FR-005**: Предварительные проверки по модели чтения: POST существующего — `409`, PUT/DELETE отсутствующего или
  удалённого — `404`.
- **FR-006**: Все ошибки — Problem Details с `code` и `traceId`, включая 401/403 Spring Security; без внутренностей.
- **FR-007**: Наружу — только DTO, сгенерированные из OpenAPI; сущностей и Avro-классов в ответах нет.
- **FR-008**: Сервис без состояния, в Compose — два экземпляра.
- **FR-009**: Принимаются только сервисные токены Keycloak: подпись по JWKS, `iss`, `exp`/`nbf`, `aud = device-service`,
  сервисная учётная запись; чтение — роль `devices.read`, запись — `devices.write`.
- **FR-010**: Realm Keycloak лежит в репозитории и импортируется при запуске; у device-service свой клиент, для
  проверки — клиент-потребитель с сервисной учётной записью.
- **FR-011**: OpenAPI в репозитории — источник истины; при сборке генерируются DTO и интерфейс HTTP Service Client,
  контроллер реализует интерфейс.
- **FR-012**: Интерфейс и DTO публикуются в Nexus библиотекой `device-service-client` с версией.
- **FR-013**: В `DeviceInfo` (avros 1.3.0) необязательное поле `operation` (`CREATE`, `UPDATE`, `DELETE`, `UNKNOWN`),
  default `null`; совместимость FULL; версия опубликована в Nexus и зарегистрирована в Schema Registry.
- **FR-014**: events-collector-service переходит на avros 1.3.0 и `operation` не заполняет.
- **FR-015**: device-collector-service применяет `CREATE`, `UPDATE`, `DELETE` по таблице правил (раздел «Критерии
  приёмки»), обработка идемпотентна, более старое сообщение не перезаписывает более новое.
- **FR-016**: Удаление мягкое; событие роутера после удаления администратором устройство не возвращает.
- **FR-017**: После ручного изменения поля администратора не перезаписываются событиями роутера.
- **FR-018**: Кэш сбрасывает device-collector-service после `COMMIT`; в кэш не попадают данные старее последней
  применённой версии.
- **FR-019**: Чтение при промахе идёт только в реплики, не в основные узлы шардов.
- **FR-020**: Redis общий для платформы, все сервисы подключаются через Sentinel; TTL кэша — `DEVICE_CACHE_TTL`.
- **FR-021**: Недоступный Redis не ломает чтение и обработку сообщений.
- **FR-022**: Метрики Micrometer и панели Grafana у events-collector-service, device-collector-service, device-service и PostgreSQL.
- **FR-023**: Логи всех компонентов в JSON с `traceId`, в Loki; один `traceId` от HTTP-запроса в device-service через
  Kafka и device-collector-service до записи в PostgreSQL.
- **FR-024**: Общий Compose; у сервиса двухэтапный Dockerfile (JDK → JRE); образы без `latest`; секретов в репозитории
  нет, есть `.env.example`; у всех исходящих вызовов тайм-ауты; liveness не зависит от внешних систем.

### Key Entities

- **Устройство (модель чтения)** — данные администратора, данные устройства, `version`, признак удаления. Владелец —
  device-collector-service; device-service только читает.
- **Команда** — `DeviceInfo` с `operation`, `eventId`, `eventTimeMs` и полями администратора.
- **Запись кэша** `device:v1:{deviceId}` — готовый ответ, его `version` и `floor`.

## Success Criteria

- **SC-001**: Изменение, принятое `202`, видно через GET не позже 5 с на поднятом стенде (`ItEndToEndTest`, ожидание до 5 с).
- **SC-002**: Ни одного запроса device-service к основным узлам шардов (`pg_stat_activity` пользователя `device_reader` — только на репликах).
- **SC-003**: При остановленном Redis 100 % GET существующих устройств отвечают `200`.
- **SC-004**: Каждый из пяти случаев безопасности (US5) даёт ожидаемый код в IT с токенами настоящего Keycloak.

## Assumptions

- Клиенты — сервисы платформы (API-оркестратор и проверочный клиент); пользователи и веб-интерфейс ходят через оркестратор.
- Объём — тысячи устройств и единицы запросов в секунду на учебном стенде; нагрузочное тестирование вне рамок.
- Read-your-writes не гарантируется: клиент перечитывает ресурс позже или показывает оптимистичное состояние.
- Защита кэша от одновременного промаха, несуществующих ключей и массового истечения TTL, а также API v2 —
  дополнительные задачи; делаются после основного объёма отдельным решением.

## Критерии приёмки (Definition of Done)

`ds` = `./gradlew :services:device-service`, `dc` = `docker compose -f infrastructure/docker-compose.yaml`.

### Дизайн

- [ ] Артефакты Spec Kit: `.specify/memory/constitution.md`, `spec.md`, `plan.md`, `data-model.md`, `research.md`,
  `contracts/device-service.yaml`, `tasks.md`, `analyze-report.md`; ADR `docs/adr/0001…0009`; диаграммы Mermaid
  из шапки рендерятся без ошибок.
- [ ] Дизайн принят до начала кода; после реализации `converge` сообщает `Converged`, результат — в `tasks.md`.

### avros 1.3.0

- [ ] `DeviceInfo.operation`: `["null", OperationType]`, `default: null`; `OperationType` — `CREATE`, `UPDATE`,
  `DELETE`, `UNKNOWN`, default символа `UNKNOWN`. Остальные поля не изменены.
- [ ] `make avros-check` — FULL; `ContractCompatibilityTest`: 1.2.0 читается схемой 1.3.0 (`operation = null`) и наоборот.
- [ ] 1.3.0 в Nexus и в Schema Registry: `curl -s localhost:8081/subjects/devices-value/versions/latest` содержит
  `operation`; повтор `make avros-up` — код 0, ничего не меняет.

### Изменения в приёме событий

- [ ] events-collector-service: `avrosVersion = "1.3.0"`, `operation` не заполняется; Redis через Sentinel
  (`REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_NODES`); `check` и `make check-local` проходят.

### Изменения в реестре устройств

- [ ] device-collector-service на avros 1.3.0 применяет сообщения по таблице (`dcs-operations-sequence.mmd`):

  | Сообщение | Состояние строки | Результат |
  |---|---|---|
  | `CREATE` | нет / есть / удалено | вставка или обновление полей администратора, удаление снимается — `APPLIED` |
  | `UPDATE` | нет | вставка (после компакции `CREATE` мог исчезнуть) — `APPLIED` |
  | `UPDATE` | есть | поля администратора из команды, `managed_at` — `APPLIED` |
  | `UPDATE` | удалено | без изменений — `IGNORED` |
  | `DELETE` | есть / нет | `status = DELETED`, `deleted_at`, `deleted_by = ADMIN` (без строки — надгробие) — `APPLIED` |
  | команда старее `managed_at` | — | `STALE` |
  | событие роутера | удалено администратором | только `seen_count`, `last_event_*` — `IGNORED` |
  | событие роутера | `managed_at` не пуст | только данные устройства (прошивка, ОС, сенсоры, `last_seen_at`) — `APPLIED` |
  | событие роутера старее `last_state_at` | — | `STALE` (как раньше) |
  | повтор `eventId` | — | `DUPLICATE`, строка не меняется |
  | `UNKNOWN`, `CREATE`/`UPDATE` без `device` | — | сразу `devices-dlt`, следующие обрабатываются |

  Unit `DeviceRegistryServiceTest` — тест на строку; IT `ItDeviceOperationsTest` — CREATE/UPDATE/DELETE, событие
  после удаления, повтор команды, старая команда.
- [ ] Время команды сравнивается с `managed_at`, события роутера — с `last_state_at`: часы устройства не делают
  команду администратора устаревшей (unit `givenRouterClockAhead_whenAdminUpdate_thenApplied`).
- [ ] Каждый `APPLIED` увеличивает `devices.version`; после `COMMIT` — Lua в Redis: `floor = max(floor, version)`,
  кэш с меньшей версией удаляется. Сбой Redis — WARN и `devices_cache_invalidations_total{result="error"}`, ack идёт
  (unit на оба случая; IT `givenCachedDevice_whenUpdateConsumed_thenCacheEvicted`).

### База данных

- [ ] `V3__device_management.sql` (`IF NOT EXISTS`): `version BIGINT NOT NULL DEFAULT 0`, `managed_at TIMESTAMPTZ`,
  `deleted_by TEXT`; исход `IGNORED` в CHECK журнала. Повтор на шардах с данными — код 0, старые строки читаются.
- [ ] Роль `device_reader` (`pg_read_all_data`, пароль `SHARD_READER_PASSWORD`) создаёт `shard-replication-init`;
  на репликах она есть через репликацию, запись под ней отклоняется.

### device-service: API и поведение

- [ ] Четыре метода по `contracts/device-service.yaml`; POST/PUT/DELETE — `202` с `Location` и телом
  `DeviceCommandAccepted` (`commandId = eventId`), GET — `200`/`404`. IT `ItDeviceCommandsTest`: для каждой операции
  в `devices` одно сообщение с ключом `deviceId`, нужными `operation` и `eventType`.
- [ ] Kafka остановлена — `503 COMMAND_NOT_ACCEPTED` за `DEVICE_COMMAND_SEND_TIMEOUT` (IT).
- [ ] `409`/`404` предварительных проверок, `400` с полями — IT; ответы ошибок — Problem Details с `code`, `traceId`,
  без стека и SQL (`ProblemDetailsAdviceTest`).
- [ ] В сервисе нет springdoc и самописных DTO (`grep -rn springdoc services/device-service` — пусто);
  `DeviceController implements DeviceApi`.
- [ ] Сессий нет (`STATELESS`, нет `Set-Cookie`); `dc ps device-service` — два экземпляра `healthy`; одинаковые
  запросы к обоим дают одинаковый ответ.

### API First и клиентская библиотека

- [ ] Спецификация лежит в `services/device-service-client/src/main/openapi/device-service.yaml` (переносится из
  `contracts/` при реализации, копий нет); OpenAPI Generator при сборке создаёт DTO и `DeviceApi`; сгенерированное
  не в Git.
- [ ] `device-service-client` версии `info.version` в Nexus (`make ds-client-release`), выпущенная версия не
  перезаписывается; `ItClientTest` вызывает запущенный сервис через `DeviceApi` из библиотеки.

### Безопасность

- [ ] Realm `iot-platform` в `infrastructure/config/keycloak/`, импортируется при старте, секретов нет (подставляются
  из окружения). Клиенты: `device-service` (роли `devices.read`, `devices.write`), `device-service-consumer` (сервисная
  учётная запись, обе роли, audience `device-service`), `device-service-reader` (только `devices.read`).
- [ ] IT `ItSecurityTest` с Keycloak в Testcontainers и тем же realm: без токена — 401; чужая аудитория — 401; истёкший — 401;
  пользовательский токен с ролью и аудиторией — 403 `SERVICE_TOKEN_REQUIRED`; сервисный без роли — 403; с ролью — 2xx.
- [ ] Токен, полученный с хоста по README, принимается сервисом в Compose (тот же `iss`).

### Кэш и чтение

- [ ] Попадание и промах кэша (IT `ItDeviceReadTest`): промах — ключ `device:v1:{id}` появился с TTL ≤ `DEVICE_CACHE_TTL`;
  попадание — Proxy не вызывается (метрика `device_cache_requests_total{result="hit"}`).
- [ ] Реплика с `version < floor` — ответ `200`, в кэш не записано (`RedisDeviceCacheTest`, IT `givenReplicaBehind_whenRead_thenNotCached`).
- [ ] Чтение только из реплик: Proxy `devices_read` описан в `database-devices_read.yaml` с источниками-репликами;
  после серии GET на основных узлах нет сессий `device_reader` (SC-002).
- [ ] Redis остановлен — GET `200` (SC-003); остановка master Redis — Sentinel переключает, device-service и
  events-collector-service работают без перезапуска (проверка стенда в README).

### Запуск

- [ ] `make up` поднимает Keycloak с realm, Redis master + реплика + 3 Sentinel, два экземпляра device-service;
  Dockerfile двухэтапный; все новые переменные в `.env.example`; `git grep -nE "image: .*:latest"` — пусто.
- [ ] Тайм-ауты: Kafka (`DEVICE_COMMAND_SEND_TIMEOUT`, `delivery.timeout.ms`), Redis (`REDIS_TIMEOUT`), JDBC
  (`connectTimeout`, `socketTimeout`), JWKS (тайм-ауты `RestClient` декодера). Liveness — без внешних проверок.

### Наблюдаемость

- [ ] Метрики: `http_server_requests_seconds`, `device_commands_accepted_total{operation}`,
  `device_commands_rejected_total{operation,code}`, `device_command_publish_seconds`, `device_cache_requests_total{result}`;
  в device-collector-service — исход `ignored`, `devices_cache_invalidations_total{result}`. Меток с `deviceId` нет.
- [ ] Панели Grafana: device-service (RED, p95/p99, команды по операциям, доля попаданий кэша, время Kafka, Redis,
  Proxy, отказы 401/403), а также существующие events-collector-service, device-collector-service, PostgreSQL;
  все запросы панелей без ошибок.
- [ ] Логи JSON с `traceId`/`spanId` в Loki; токенов и секретов в логах нет.
- [ ] Одна трасса в Tempo: `PUT /api/v1/devices/{deviceId}` → `devices send` → `devices process` → `update devices`.

### Тесты

- [ ] Unit: логика device-service (проверки, маппинг в `DeviceInfo`, Problem Details, кэш, ошибки Kafka) и изменения
  device-collector-service; покрытие строк ≥ 70 % (`ds check`).
- [ ] IT на Testcontainers: PostgreSQL, Kafka + Schema Registry, Redis, Keycloak; токены настоящие. Полный путь
  `PUT` → Kafka → device-collector-service → реплика → `GET` с новыми данными — `ItEndToEndTest`.

### Что сдаётся после реализации

- [ ] Ссылка на репозиторий, `tasks.md` с `Converged`, снимки панелей Grafana и одной сквозной трассы, README:
  порядок запуска, получение сервисного токена, примеры запросов.
