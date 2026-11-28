# Tasks: router-manager — команды роутерам

**Input**: [spec.md](spec.md), [plan.md](plan.md), [data-model.md](data-model.md), [research.md](research.md),
[router_manager.proto](contracts/router_manager.proto), [quickstart.md](quickstart.md), правила агента — [AGENTS.md](../../AGENTS.md).

Формат: `- [ ] TNNN [P?] [Mac?] действие — файлы (требования)`. `[P]` — можно параллельно: другие файлы, нет зависимости от
незавершённых задач. Тест пишется раньше кода, который он проверяет, и сначала падает. **ЧЕЛОВЕК** — шаг выполняет человек:
исполнитель останавливается и ждёт. `(Rn)` — задача проверяет предположение research.md: не подтвердилось — остановиться и
сообщить. Решения со статусом «Проверено» (R1–R6, R8–R14, R16–R18) записаны в задачах как требования; R7, R15 (частично), R19–R23
проверяют задачи с пометкой `(Rn)`.

**Место выполнения.** Go-сборка и тесты — в контейнере `golang:1.26.11-bookworm` (`docker run`, тома `iot-rm-gomod`,
`iot-rm-gobuild`) на стенде владельца (macOS, arm64): на VPS не хватает диска под модули и кэш сборки Go. `docker build` и
`make up` по ssh на Mac не работают (keychain) — образы собирает человек в своём терминале; задачи с этим помечены `[Mac]`.
Gradle-задачи (`protos`, `db-migrator`) — где обычно.

**Пакеты для исполнителей.** Файлы пакетов не пересекаются; `docker-compose.yaml`, `.env.example`, `Makefile` в каждый момент
правит один владелец, по порядку P1 → P2 → P3 → P7. Пакеты P4, P5, P6 их не трогают: новые переменные называют в отчёте (имена
уже в plan.md). Порядок: P1 → P2 ‖ P3 → P4 ‖ P5 → P6 → P7 → P8 (необязательно) → converge. Отчёт исполнителя: изменённые файлы,
отмеченные `[X]`, RED → GREEN, число тестов из `go test -json` (`"Action":"pass"` по тестам) или `build/test-results`, вывод
команд Checkpoint, «не проверено».

Сокращения путей:

| Сокращение | Путь |
|---|---|
| `PR` | `protos` |
| `RM` | `services/router-manager` |
| `DBM` | `services/db-migrator` |
| `INF` | `infrastructure` |

## Phase 1 (P1): Контракт protos

- [ ] T001 Проект `protos` по образцу avros: `include("protos")` в `settings.gradle.kts`; `PR/build.gradle.kts` (группа
  `com.github.gluhov.iotplatform`, версия `1.0.0`, jar `protos` с `proto/**` и `buf.yaml`, публикация `maven-releases`/`maven-snapshots`
  как в `avros/build.gradle.kts`); `PR/proto/iotplatform/router/v1/router_manager.proto` — дословно
  [contracts/router_manager.proto](contracts/router_manager.proto); `PR/buf.yaml` (lint STANDARD без исключений, breaking WIRE_JSON);
  строка `COPY protos/build.gradle.kts protos/` в Dockerfile всех подпроектов Gradle (AGENTS.md) — `settings.gradle.kts`, `PR/*`,
  `avros/Dockerfile`, `services/*/Dockerfile` (FR-001)
- [ ] T002 Тест совместимости до кода: `PR/scripts/compat-test.sh` — копии контракта с удалённым полем без `reserved` и с
  переименованным значением enum (`PR/src/test/compat/`) дают код ≠ 0, с новым полем — 0; падает, пока нет T003 (FR-001)
- [ ] T003 Задача Gradle `checkProtosCompatibility`: последний релиз из Nexus (`maven-metadata.xml`, как `checkAvrosCompatibility`),
  распаковка в `build/protos-previous`, `buf lint`, `buf breaking --against build/protos-previous` (правила из `buf.yaml`); нет
  релиза — первая публикация; публикация зависит от проверки; `PR/scripts/check.sh` — `PR/build.gradle.kts`, `PR/scripts/check.sh` (FR-001)
- [ ] T004 [Mac] `PR/Dockerfile` (`eclipse-temurin:25.0.4.1_1-jdk-noble` + `COPY --from=bufbuild/buf:1.47.2 /usr/bin/buf`; путь сверить
  в образе) и `PR/scripts/container-publish.sh` (версия есть и jar совпадает — пропуск; отличается — ошибка); сервис Compose
  `protos-publish` (после `nexus-init`), цели `protos-check` (проверка в контейнере, без публикации), `protos-up`; `PROTOS_VERSION`,
  `PROTOS_IMAGE_TAG` в `.env.example` — `PR/Dockerfile`, `PR/scripts/container-publish.sh`, `INF/docker-compose.yaml`, `Makefile`,
  `.env.example` (FR-001)
- [ ] T005 **ЧЕЛОВЕК** (необратимо: версия в Nexus не перезаписывается) Опубликовать protos 1.0.0: `make protos-up`; проверить
  `curl -sf -o /dev/null -w '%{http_code}' $NEXUS/repository/maven-releases/com/github/gluhov/iotplatform/protos/1.0.0/protos-1.0.0.jar` = 200 (FR-001)

**Checkpoint**: `make protos-check` проходит; `PR/scripts/compat-test.sh` — код 0 (все сценарии как ожидалось); после T005 jar в Nexus.

## Phase 2 (P2): База, миграции, Keycloak

- [ ] T006 (R22) `INF/init/postgres/router-manager-db-init.sh` (`/bin/sh`, `chmod +x`, `git add --chmod=+x`): база
  `${ROUTER_MANAGER_DB_NAME}` при отсутствии, роль `${ROUTER_MANAGER_DB_USER}` (`LOGIN`) при отсутствии, `ALTER ROLE … PASSWORD`
  из окружения на каждом запуске, `CONNECT` на базу; сначала проверка на одноразовом PostgreSQL: два запуска подряд — код 0,
  роль одна, новый пароль действует (FR-015)
- [ ] T007 db-migrator, тест до кода: `DBM/src/test/java/…/dbmigrator/MigrationTargetTest.java` — `MIGRATION_SET=router-manager`
  даёт одну цель из `DB_URL`, `DB_USER`, `DB_PASSWORD` и каталог `db/migration/router-manager`; без переменной — прежние шарды и `devices`; затем
  `DBM/src/main/java/…/dbmigrator/{DbMigrator.java,MigrationTarget.java}` (FR-015)
- [ ] T008 [P] Миграция `DBM/src/main/resources/db/migration/router-manager/V1__router_commands.sql` — DDL дословно из
  [data-model.md](data-model.md) (FR-015)
- [ ] T009 Compose: у `postgres` убрать `profiles: [platform]`; сервисы `router-manager-db-init` (образ `postgres:${POSTGRES_VERSION}`,
  после `postgres` healthy) и `db-migrator-router-manager` (образ db-migrator, `MIGRATION_SET=router-manager`, `DB_URL`/`DB_USER`/`DB_PASSWORD` по plan.md, после db-init);
  переменные `ROUTER_MANAGER_DB_*` в `.env.example` — `INF/docker-compose.yaml`, `.env.example` (FR-015, FR-021)
- [ ] T010 Realm: клиент `router-manager` (роли `commands.read`, `commands.write`), `router-manager-consumer` (сервисная учётка,
  обе роли, mapper аудитории), `router-manager-reader` (`commands.read`), секреты `${ROUTER_MANAGER_CONSUMER_SECRET}`,
  `${ROUTER_MANAGER_READER_SECRET}` — как у `device-service-*`; переменные у `keycloak` в Compose и в `.env.example` —
  `INF/config/keycloak/iot-platform-realm.json`, `INF/docker-compose.yaml`, `.env.example` (FR-017)

**Checkpoint**: `./gradlew :services:db-migrator:test` зелёный; `make check`; [Mac] `make up` с пустыми томами — таблица
`router_commands` и индексы есть, у `router_manager` нет `DELETE`; токен `router-manager-consumer` с `aud` `router-manager`.

## Phase 3 (P3): Каркас Go и домен

- [ ] T011 (R20) Go-модуль: `RM/go.mod` (модуль `github.com/gluhov-d/iot-platform/services/router-manager`, версии из plan.md),
  `RM/buf.gen.yaml` (локальные плагины, R13), `RM/scripts/generate.sh` (скачать `protos-${PROTOS_VERSION}.jar` из Nexus
  `maven-public`, распаковать, `buf generate` в `internal/gen`), `RM/.gitignore` (`internal/gen/`), цели `rm-generate`, `rm-test`
  (`go test -race ./...`), `rm-coverage` (≥ 70 %, `RM/scripts/coverage.sh`), `rm-it` (с `/var/run/docker.sock`, тег
  `integration`) — в `Makefile` (контейнер `golang:1.26.11-bookworm`); проверка R20: `go build ./... && go vet ./...` с одним
  сгенерированным пакетом для двух программ (FR-002, FR-025)
- [ ] T012 [P] Тест до кода `RM/internal/domain/model/command_test.go` — переходы статусов (все пары таблицы data-model.md) и
  проверка полей; затем `RM/internal/domain/model/{command.go,errors.go}` (FR-004, FR-007, FR-008)
- [ ] T013 [P] Тест до кода `RM/internal/config/config_test.go` — разбор окружения, отказ при `ROUTER_NEG_CACHE_TTL +
  ROUTER_REQUEST_TIMEOUT ≥ 20s` и пустых секретах; затем `RM/internal/config/config.go` (FR-012, FR-014, FR-016)
- [ ] T014 [P] Тест до кода `RM/internal/adapter/in/grpc/auth/jwt_test.go` (JWKS на `httptest`, свои ключи): сценарии R15 —
  `UNAUTHENTICATED` и `PERMISSION_DENIED`, строгий старт без JWKS; затем `RM/internal/adapter/in/grpc/auth/jwt.go`
  (keyfunc/v3, `iss` из `KEYCLOAK_ISSUER_URI`, ключи по `KEYCLOAK_JWK_SET_URI`, R16) (FR-017, FR-019)
- [ ] T015 [P] (R23) Тест до кода `RM/internal/routerauth/routerauth_test.go` — таблица R17 на строке подписи data-model.md, подмена
  каждого поля тела и nonce, бенчмарк; `RM/internal/adapter/in/grpc/auth/router_test.go` на заглушке порта `ReplayGuard`: повтор →
  `UNAUTHENTICATED`, ошибка хранилища → запрос принят и счётчик +1, неверная подпись в хранилище не пишется; затем
  `RM/internal/routerauth/{sign.go,verify.go}`, перехватчик `RM/internal/adapter/in/grpc/auth/router.go` и `SET NX` в
  `RM/internal/adapter/out/redis/replay.go` (FR-018)
- [ ] T016 Тест до кода `RM/internal/domain/service/command_service_test.go` на заглушках портов: опрос с записью «нет команды» —
  без вызова хранилища; промах — выдача, при пустом ответе — запись с поколением до запроса; создание — хранилище, затем
  поколение; ошибка кэша — путь через хранилище; ответ роутера по таблице data-model.md, поздний ответ — счётчик; затем
  `RM/internal/domain/port/*.go`, `RM/internal/domain/service/command_service.go` (FR-004…FR-008, FR-012, FR-013)
- [ ] T017 Тест до кода `RM/internal/domain/service/stuck_scanner_test.go` — каждая возвращённая команда даёт одну строку
  `command_stuck` и `+1` к счётчику, ошибка хранилища пропускает такт; затем `RM/internal/domain/service/stuck_scanner.go` (FR-010)
- [ ] T018 [P] Тест границ пакетов `RM/internal/archtest/arch_test.go`: `internal/domain/...` не импортирует `internal/adapter/...`,
  `internal/observability`, `internal/gen`, pgx, go-redis, grpc (`go list -deps`) (FR-025)

**Checkpoint**: `make rm-test` — все тесты зелёные с `-race`; `make rm-coverage` по пакетам домена и проверки ≥ 70 %.

## Phase 4 (P4): Адаптеры и сервер

- [ ] T019 Тест до кода `RM/internal/adapter/out/postgres/repository_it_test.go` (тег `integration`, `postgres:16-alpine`, схема —
  SQL из T008): два пула × 16 горутин выдают 500 команд ровно по разу (R1); проход зависших из двух пулов — каждую один раз (R2);
  повторный ответ — 0 строк (R3); создание с тем же `request_id` — одна строка; затем `RM/internal/adapter/out/postgres/repository.go`
  (pgx, `otelpgx`, выражения data-model.md) (FR-004…FR-010, FR-015)
- [ ] T020 Тест `RM/internal/adapter/out/postgres/deadline_it_test.go`: запрос с дедлайном 200 мс прерывается, ошибка
  `DEADLINE_EXCEEDED`-класса, пул исправен (R4) (FR-014)
- [ ] T021 (R7) Сначала `RM/internal/adapter/out/redis/cache_it_test.go`: на приостановленном Redis вызов с контекстом 300 мс
  возвращается не позже 400 мс при `ContextTimeoutEnabled: true` (и не возвращается без флага); затем гонка поколения (R5),
  пауза и восстановление (R6), Sentinel по именам (R8); затем `RM/internal/adapter/out/redis/cache.go` (FailoverClient, Lua из
  data-model.md, `redisotel`) (FR-012…FR-014)
- [ ] T022 [P] Тест до кода `RM/internal/observability/observability_test.go`: строка slog JSON с `traceId`/`spanId` из контекста;
  реестр метрик содержит все имена FR-022 и ни одной метки `router_id`/`command_id`; затем `RM/internal/observability/{log.go,
  trace.go,metrics.go}` (OTLP gRPC в `OTEL_EXPORTER_OTLP_ENDPOINT`) (FR-022, FR-023)
- [ ] T023 Тест до кода `RM/internal/adapter/in/grpc/server_test.go` (bufconn): коды таблицы «Ошибки» plan.md для каждого метода,
  `more_pending`, перехват паники → `INTERNAL`, срок = меньшее из клиента и `ROUTER_REQUEST_TIMEOUT`, health; затем
  `RM/internal/adapter/in/grpc/{admin.go,agent.go,interceptors.go,server.go}` (FR-003…FR-007, FR-014, FR-019)
- [ ] T024 Тест до кода `RM/cmd/router-manager/main_test.go` на коды `healthcheck` (R10, 0/1); затем `RM/cmd/router-manager/main.go`:
  подкоманды `serve` и `healthcheck`, сборка зависимостей, строгий старт JWKS, проход зависших и снимок статусов по таймерам,
  reflection по `ROUTER_GRPC_REFLECTION`, SIGTERM → `NOT_SERVING` → `GracefulStop` (FR-003, FR-010, FR-016)
- [ ] T025 (R21) [Mac] `RM/Dockerfile`: этап сборки — `golang:1.26.11-bookworm`, `buf` из `bufbuild/buf:1.47.2`, плагины `go install`
  версий R13, `scripts/generate.sh` с `ARG NEXUS_URL`, `ARG PROTOS_VERSION`; итог — `gcr.io/distroless/static-debian12:nonroot`,
  `USER nonroot`, `HEALTHCHECK ["/app/router-manager","healthcheck"]`; `RM/.dockerignore`; проверка — человек собирает образ
  (`docker compose … build router-manager`) с пустым `internal/gen` (FR-002, FR-021)

**Checkpoint**: `make rm-test`, `make rm-it` по пакетам адаптеров зелёные; [Mac] образ router-manager собран.

## Phase 5 (P5): Эмулятор роутеров

- [ ] T026 Тест до кода `RM/emulator/emulator_test.go` на заглушке `RouterAgentService` (bufconn): доли отказов и молчания на 1000
  командах в пределах ±5 %, опрос сразу при `more_pending`, молчащий роутер не вызывает `ReportResult`, подпись R17; затем
  `RM/emulator/{fleet.go,router.go,metrics.go}`, `RM/cmd/router-emulator/main.go` (параметры FR-020, клиент `dns:///…` с
  `round_robin`, R11; подкоманда `sign <router-id> <метод> [command_id outcome message]` печатает метаданные подписи (с новым nonce) для `grpcurl -H`),
  `RM/emulator.Dockerfile` (два этапа) (FR-020)

**Checkpoint**: `make rm-test` зелёный, в том числе `./emulator/...`.

## Phase 6 (P6): Интеграционные тесты

- [ ] T027 Каркас `RM/it/harness_test.go` (тег `integration`): testcontainers-go 0.45.0 (R12) — `postgres:16-alpine` со схемой из T008
  и ролью `router_manager`, `redis:8.6.7-alpine`, `quay.io/keycloak/keycloak:26.0.8` с realm из `INF/config/keycloak` (файл —
  с правами 0644); один или два сервера router-manager в процессе на своих портах; эмулятор — пакет `emulator`; приём строк лога
  (обработчик slog в памяти); счётчик SQL (`pgx` QueryTracer) (FR-026)
- [ ] T028 `RM/it/full_path_test.go`: `TestCommandReachesSuccess`, `TestRouterFailureMarksFailed` (FR-005, FR-007, FR-026)
- [ ] T029 `RM/it/stuck_test.go`: `TestStuckCommandFailsWithAlertEvent` — доля молчащих 1.0, срок 2 с, два экземпляра: FAILED
  (`TIMEOUT`), ровно одна строка `command_stuck` на команду с `command_id`; поздний ответ — `applied = false` (FR-010, FR-011, SC-004)
- [ ] T030 `RM/it/two_instances_test.go`: `TestTwoInstancesIssueOnce` — 50 роутеров, 1000 команд, два экземпляра: каждая получена
  один раз, 1000 итоговых статусов; остановка одного экземпляра посреди прогона — остальные команды доходят (FR-009, FR-016, SC-003)
- [ ] T031 `RM/it/negative_cache_test.go`: `TestRepeatedEmptyPollSkipsDatabase` — второй опрос без команды: 0 SQL;
  `TestCommandIssuedByThirdPoll` — уменьшенные сроки с тем же неравенством, что в продуктиве (`срок записи + срок вызова < 2 × интервал`: 0,7 + 0,2 < 2 × 1 с), Redis приостановлен на время создания:
  команда получена не позже третьего опроса (FR-012, SC-001, SC-002)
- [ ] T032 `RM/it/redis_down_test.go`: `TestWorksWithRedisPaused` — создание, выдача, ответ при `docker pause` Redis; после
  `unpause` опрос снова из кэша (FR-013)
- [ ] T033 `RM/it/auth_test.go`: `TestAdminWithoutToken`, `TestAdminForeignAudience` (токен `device-service-consumer`),
  `TestReaderCannotCreate`, `TestUserTokenDenied` (пользовательский токен → `PERMISSION_DENIED`), `TestConsumerCreatesAndReads`,
  `TestRouterWrongKeyRejected`, `TestUnsignedRouterRequest` (без метаданных подписи → `UNAUTHENTICATED`), `TestReplayedRequestRejected`
  (тот же запрос дважды → второй `UNAUTHENTICATED`), `TestTamperedReportRejected` (изменённый `outcome`), `TestReplayGuardSkippedWithRedisPaused`
  (R15, R23) (FR-017, FR-018, SC-005)
- [ ] T034 `RM/it/deadline_trace_test.go`: `TestDeadlineReachesStorage` (FR-014), `TestPollTraceHasRedisAndPostgresSpans` —
  экспортёр в памяти: родитель спанов Redis и pgx — спан сервера, `traceId` строки лога совпадает (FR-023)

**Checkpoint**: `make rm-it` — все тесты T019–T034 зелёные; `make rm-test`; `make rm-coverage` ≥ 70 %.

## Phase 7 (P7): Стенд, наблюдаемость, документация

- [ ] T035 Compose: `router-manager` (`deploy.replicas: 2`, `ROUTER_MANAGER_PORTS`, сборка с `NEXUS_URL`/`PROTOS_VERSION` и
  `extra_hosts` как у device-service, `depends_on` на `db-migrator-router-manager`, `protos-publish`, Redis + Sentinel, `keycloak`,
  healthcheck exec `healthcheck`), `router-emulator` (профиль `emulator`); все переменные plan.md в `.env.example`; цель
  `rm-emulator-up` — `INF/docker-compose.yaml`, `.env.example`, `Makefile` (FR-021)
- [ ] T036 Alloy: `discovery.dns` + `prometheus.scrape` с `job_name = "router-manager"` и `"router-emulator"`, `metrics_path = "/metrics"`;
  `loki.process` для `router-manager` — метка `level`, `trace_id` в structured metadata — `INF/config/alloy/config.alloy` (FR-022, FR-023)
- [ ] T037 (R19) Сначала проверить на одноразовом Grafana 11.2.0: contact point без интеграций принимается провижинингом и
  гасит отправку в `grafana-default-email`; не принимается — остановиться, запасной вариант R19. Затем
  `INF/config/grafana/provisioning/alerting/router-manager.yaml` (правило R18 дословно: интервал 10 с, `uid` источника `loki`,
  contact point, политика по `alertname=RouterManagerCommandStuck`) и монтирование каталога у `grafana` в Compose (FR-011)
- [ ] T038 [P] Панель `INF/config/grafana/dashboards/router-manager.json` (`uid` `router-manager`): частота опросов, доля из кэша,
  команды по статусам (`max by (status)`), p50/p95 создание → выдача и выдача → ответ, число зависших (`increase`), ошибки gRPC и
  Redis, поздние ответы (FR-024)
- [ ] T039 `RM/scripts/quick-check.sh` (+ цель `rm-quick-check`): токен `router-manager-consumer`; `grpcurl` из
  `fullstorydev/grpcurl:v1.9.3` в сети стенда: `CreateCommand` роутеру эмулятора → SUCCESS через `GetCommand`; команда молчащему
  роутеру → FAILED (`TIMEOUT`) и экземпляр оповещения с её `command_id` в `/api/prometheus/grafana/api/v1/alerts`; вызов без
  токена → `Unauthenticated`; порты — через `docker compose port` — `RM/scripts/quick-check.sh`, `Makefile` (FR-011, FR-017, FR-021)
- [ ] T040 [Mac] `make up PROFILES=observability` → `make rm-emulator-up` → `make rm-quick-check`, `make obs-check`; трасса опроса
  в Tempo со спанами Redis и pgx; результат — в research.md «Результаты проверок на стенде» (FR-021…FR-024)
- [ ] T041 README (раздел «router-manager»: порядок запуска, примеры `grpcurl` для четырёх методов, параметры эмулятора, переменные,
  цели make), список диаграмм, `docs/definition-of-done.md` (секция-ссылка — уже в дизайне; пункты прежних секций по списку в ней) —
  `README.md`, `docs/definition-of-done.md` (FR-021)
- [ ] T042 **ЧЕЛОВЕК** Снимки: панель router-manager при работающем эмуляторе, трасса опроса в Tempo, сработавшее оповещение о
  зависшей команде — в `docs/screenshots/` (DoD)

**Checkpoint**: `make check`; [Mac] T040 зелёный; README описывает запуск с нуля.

## Phase 8 (P8, необязательно): Нагрузка и сравнение

- [ ] T043 Режим нагрузки эмулятора (сводка p95/p99, частота, доля из кэша) и `docs/load/router-manager-load.md`: прогон с кэшем
  и без (`ROUTER_NEG_CACHE_ENABLED=false`), нагрузка на базу по `pg_stat_statements` — `RM/emulator/load.go`, `docs/load/router-manager-load.md` (FR-027)
- [ ] T044 [P] pprof на порту метрик по `ROUTER_PPROF_ENABLED`; профиль CPU под нагрузкой и выводы в отчёт — `RM/cmd/router-manager/main.go`,
  `docs/load/router-manager-load.md` (FR-027)
- [ ] T045 [P] Бенчмарк горячего пути опроса до и после `sync.Pool` (`go test -bench -benchmem`), числа в отчёт — `RM/internal/adapter/in/grpc/bench_test.go` (FR-027)
- [ ] T046 Потоковая выдача `WatchCommands`: метод в контракте (protos 1.1.0, `buf breaking` проходит), тест до кода
  `RM/internal/adapter/in/grpc/watch_test.go` (bufconn: команда приходит в поток, выдача ровно один раз), затем `watch.go`;
  сравнение с опросом под той же нагрузкой — `PR/proto/iotplatform/router/v1/router_manager.proto`,
  `RM/internal/adapter/in/grpc/{watch.go,watch_test.go}`, `docs/load/router-manager-load.md` (FR-027)
- [ ] T047 **ЧЕЛОВЕК** (необратимо) Опубликовать protos 1.1.0: `make protos-up` с `PROTOS_VERSION=1.1.0` (FR-027)

**Checkpoint**: `make rm-test`, `make rm-it` зелёные; отчёт `docs/load/router-manager-load.md` с числами обоих прогонов.

## Phase 9: Приёмка и converge

- [ ] T048 [Mac] Checkpoint всех фаз на стенде владельца: `make protos-check`, `make rm-test`, `make rm-coverage`, `make rm-it`,
  `./gradlew :services:db-migrator:test`, `make check`, `make up` с пустыми томами, `make rm-quick-check`, `make obs-check` (SC-001…SC-005)
- [ ] T049 `converge` до `Converged`; результат — раздел «Результат converge» ниже

## Результат converge

Не выполнялся.

## Dependencies & Execution Order

T001–T004 → T005 (ЧЕЛОВЕК) → T011 (генерация берёт protos 1.0.0 из Nexus). P2 (T006–T010) параллельно P3 (T012–T018). P4 после P3
и T008. P5 после T015. P6 после P4, P5 и T010. P7 после P6. Внутри фаз — тест раньше кода. T042 — после T040.

## Implementation Strategy

MVP — P1–P6 (контракт, сервис, эмулятор, IT: User Story 1–4 без стенда), затем P7 (стенд, оповещение, панель). P8 — после
приёмки основной части, по решению владельца.
