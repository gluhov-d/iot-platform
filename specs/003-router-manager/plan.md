# Implementation Plan: router-manager — команды роутерам

**Feature**: `003-router-manager` | **Date**: 2026-11-28 | **Spec**: [spec.md](spec.md)

## Summary

Новый сервис на Go: принимает команды для роутеров по gRPC от администратора (сервисный токен Keycloak), хранит в своей
базе `iot_commands` на общем PostgreSQL, выдаёт роутерам по опросу (подпись HMAC) ровно один раз, ведёт статусы до итога,
переводит зависшие команды в FAILED с оповещением в Grafana. Повторные опросы без команды отвечает негативный кэш в Redis
платформы. Контракт — в новом проекте `protos` (по образцу avros, публикация в Nexus); из него генерируется Go-код сервиса
и эмулятора роутеров. Эмулятор — источник роутеров для интеграционных тестов, демонстрации и нагрузки.

## Technical Context

**Language/Version**: Go 1.26.11 (`golang:1.26.11-bookworm`); проекты `protos` и `db-migrator` — Gradle (Java 25), как сейчас.
**Primary Dependencies** (версии проверены прототипами, research.md): `google.golang.org/grpc` v1.85.0, `google.golang.org/protobuf`
v1.36.12, `github.com/jackc/pgx/v5` v5.13.0, `github.com/exaring/otelpgx` v0.13.0, `github.com/redis/go-redis/v9` и
`extra/redisotel/v9` v9.24.1, `go.opentelemetry.io/otel` v1.47.0 (+ `otlptracegrpc`), `otelgrpc` v0.72.0,
`github.com/MicahParks/keyfunc/v3` v3.9.2, `github.com/golang-jwt/jwt/v5` v5.3.1, `github.com/prometheus/client_golang`,
`github.com/google/uuid` (UUIDv7), `github.com/testcontainers/testcontainers-go` v0.45.0; генерация — buf 1.47.2,
`protoc-gen-go` v1.36.6, `protoc-gen-go-grpc` v1.5.1.
**Storage**: PostgreSQL 16 (`postgres`, база `iot_commands`), Redis 8.6.7 через Sentinel (`platform-redis`).
**Testing**: `go test -race ./...` (unit), `go test -race -tags integration ./...` — `*_it_test.go` адаптеров и сквозные в `it/` (testcontainers-go: PostgreSQL, Redis, Keycloak).
**Target Platform**: Docker Compose платформы, образ `gcr.io/distroless/static-debian12:nonroot`.
**Performance Goals**: проверка подписи < 1 мкс; опрос с попаданием в кэш — 0 запросов в базу; ответ из кэша — две команды Redis (`SET NX` подписи, `GET` записи «нет команды»).
**Constraints**: `ROUTER_NEG_CACHE_TTL + ROUTER_REQUEST_TIMEOUT < 2 × 10 с`; один срок на вызов — от клиента до PostgreSQL и Redis.
**Scale/Scope**: на стенде — 2 экземпляра, эмулятор десятки–тысячи роутеров; дизайн — миллионы роутеров без обращения к базе на проверку.

## Constitution Check

| Принцип | Как выполняется |
|---|---|
| I. Один владелец данных | Команды пишет только router-manager; Redis — производные данные; остальные — только через gRPC |
| II. Контракты первичны | `.proto` только в `protos/`, совместимость `buf breaking` WIRE_JSON, публикация в Nexus с явной версией; схема БД — db-migrator, `IF NOT EXISTS` |
| III. Идемпотентность | `request_id` у создания; выдача одним выражением; повторный ответ — без изменений; ответ «сохранено» только после фиксации |
| IV. Без состояния, конфигурация из окружения | Всё общее — в PostgreSQL и Redis; все сроки и адреса — переменные `.env.example`; у каждого внешнего вызова срок |
| V. Безопасность | Сервисный токен Keycloak (как device-service) и подпись роутера; ответы без внутренностей |
| VI. Наблюдаемость | Метрики (client_golang вместо Micrometer — см. Complexity Tracking), JSON-логи с `traceId`, OTLP-трассы, своя панель |
| VII. Тесты на настоящей инфраструктуре | testcontainers-go с образами стенда, токены настоящего Keycloak; покрытие строк ≥ 70 % |

## Project Structure

```text
protos/                                   # новый проект: все .proto платформы (по образцу avros, ADR-0024)
├── build.gradle.kts                      # jar с proto/**, проверка совместимости, публикация в Nexus
├── buf.yaml                              # lint STANDARD, breaking WIRE_JSON
├── Dockerfile                            # temurin + buf (COPY --from bufbuild/buf): protos-publish и protos-check
├── proto/iotplatform/router/v1/router_manager.proto
└── scripts/{check.sh,container-publish.sh}
services/router-manager/                  # один Go-модуль, две программы (research.md R20)
├── go.mod, go.sum, buf.gen.yaml, Dockerfile, emulator.Dockerfile, .dockerignore
├── cmd/router-manager/main.go            # serve | healthcheck
├── cmd/router-emulator/main.go
├── internal/config/                      # разбор окружения, проверка ограничений
├── internal/domain/{model,port,service}/ # Command, переходы, ошибки; порты хранилища, кэша, метрик, часов; CommandService, StuckScanner
├── internal/adapter/in/grpc/             # RouterAdminService, RouterAgentService, перехватчики recovery и срока вызова
├── internal/adapter/in/grpc/auth/        # jwt.go (сервисный токен), router.go (перехватчик подписи роутера)
├── internal/routerauth/                  # подпись и проверка роутера (общая с эмулятором)
├── internal/archtest/                    # границы пакетов
├── internal/adapter/out/postgres/        # pgx-репозиторий (+ *_it_test.go)
├── internal/adapter/out/redis/           # негативный кэш (Lua) (+ *_it_test.go)
├── internal/observability/               # otel, slog-обработчик с traceId, метрики
├── internal/gen/                         # сгенерированный код (в .gitignore)
├── emulator/                             # библиотека эмулятора (используют cmd и IT)
├── it/                                   # сквозные интеграционные тесты, тег integration
└── scripts/{generate.sh,quick-check.sh,coverage.sh}
services/db-migrator/src/main/resources/db/migration/router-manager/V1__router_commands.sql
infrastructure/init/postgres/router-manager-db-init.sh
infrastructure/config/grafana/dashboards/router-manager.json
infrastructure/config/grafana/provisioning/alerting/router-manager.yaml
```

## Ключевые решения

| Решение | Причина | ADR |
|---|---|---|
| router-manager вместо планового Command Service; связи Camunda и Device Gateway с командами убраны | Задание задаёт модель «роутер забирает сам»; узлы — план | 0020 |
| Унарный опрос; выдача и зависшие — одним выражением `SKIP LOCKED` | Ровно один раз при нескольких экземплярах без блокировок между ними (R1, R2) | 0021 |
| Негативный кэш с поколением и сроком меньше двух интервалов | Закрывает гонку чтения и создания, ограничивает устаревание при сбое Redis (R5) | 0022 |
| Роутеры — HMAC с выведенным ключом; администратор — сервисный токен | Проверка подписи без базы для миллионов; одноразовость — `SET NX` в Redis (R17, R23); токен — как у device-service (R15) | 0023 |
| Проект protos, Go-код генерируется при сборке из артефакта Nexus; один Go-модуль | Одна версия контракта у всех; один сгенерированный пакет в бинарнике (R13, R14, R20, R21) | 0024 |
| Проход зависших в каждом экземпляре, событие лога → правило Grafana по Loki | Оповещение на каждую команду без метки `command_id` у метрик (R2, R18) | 0025 |
| База `iot_commands` на общем PostgreSQL, своя роль, миграции db-migrator | «Своя база» без нового экземпляра; схема — только миграциями (R22) | 0026 |

Обработка вызова: перехватчики по порядку — восстановление после паники → срок (`min(срок клиента, ROUTER_REQUEST_TIMEOUT)`) →
проверка вызывающего (JWT для `RouterAdminService`, HMAC для `RouterAgentService`) → обработчик. Ограничение параллелизма —
`ROUTER_MAX_CONCURRENT_STREAMS` на соединение и пул pgx `ROUTER_DB_MAX_CONNS`; keepalive-политика сервера отвергает частые PING.
Остановка по SIGTERM: health `NOT_SERVING` → `GracefulStop` в пределах `ROUTER_SHUTDOWN_TIMEOUT` → закрытие пулов.

## Ошибки

| Ситуация | Код gRPC | Что видно |
|---|---|---|
| Неверные поля | `INVALID_ARGUMENT` | поле и правило |
| Нет токена, подпись, срок, `iss`, `aud`; нет или неверна подпись роутера, время вне окна, повтор принятой подписи | `UNAUTHENTICATED` | `unauthenticated`; причина — в лог |
| Пользовательский токен, нет роли | `PERMISSION_DENIED` | `permission denied` |
| Команды нет / чужая (`GetCommand`, `ReportResult`) | `NOT_FOUND` | — |
| Ответ на команду NEW | `FAILED_PRECONDITION` | `command not issued` |
| PostgreSQL недоступен | `UNAVAILABLE` | `storage unavailable` |
| Срок вызова истёк | `DEADLINE_EXCEEDED` | — |
| Redis недоступен | нет ошибки | работа через базу, `router_manager_cache_errors_total` |
| Паника обработчика | `INTERNAL` | `internal error`; стек — в лог |

## Переменные окружения

Значения — в `.env.example`; в Compose без значений по умолчанию. Секреты: `ROUTER_MANAGER_DB_PASSWORD`, `ROUTER_AUTH_KEYS`,
`ROUTER_MANAGER_CONSUMER_SECRET`, `ROUTER_MANAGER_READER_SECRET`.

| Переменная | Стенд | Назначение |
|---|---|---|
| `ROUTER_MANAGER_GRPC_PORT` / `ROUTER_MANAGER_METRICS_PORT` | `9090` / `8081` | порты в контейнере |
| `ROUTER_MANAGER_PORTS` | `8120-8121` | порты gRPC на хосте (по экземпляру) |
| `ROUTER_MANAGER_DB_URL` | `postgres://postgres:5432/iot_commands` | адрес базы без учётки |
| `ROUTER_MANAGER_DB_USER` / `ROUTER_MANAGER_DB_PASSWORD` | `router_manager` / секрет | роль сервиса |
| `ROUTER_DB_MAX_CONNS` / `ROUTER_DB_CONNECT_TIMEOUT` | `16` / `2s` | пул pgx |
| `ROUTER_REDIS_TIMEOUT` | `100ms` | dial/read/write go-redis |
| `ROUTER_NEG_CACHE_ENABLED` / `ROUTER_NEG_CACHE_TTL` / `ROUTER_GEN_TTL` | `true` / `15s` / `1h` | негативный кэш |
| `ROUTER_REQUEST_TIMEOUT` | `2s` | верхний срок обработки вызова |
| `ROUTER_COMMAND_SENT_TIMEOUT` | `60s` | срок ожидания ответа после выдачи |
| `ROUTER_STUCK_SCAN_INTERVAL` / `ROUTER_STUCK_SCAN_BATCH` | `10s` / `100` | проход зависших |
| `ROUTER_STATUS_SNAPSHOT_INTERVAL` | `15s` | снимок статусов для метрики |
| `ROUTER_AUTH_KEYS` / `ROUTER_AUTH_CURRENT_KEY_ID` / `ROUTER_AUTH_MAX_SKEW` | `k1:<base64>` / `k1` / `60s` | подпись роутеров |
| `ROUTER_AUTH_REPLAY_TTL` | `120s` | срок записи принятой подписи; сервис не стартует при значении меньше `2 × ROUTER_AUTH_MAX_SKEW` |
| `ROUTER_MANAGER_AUDIENCE` | `router-manager` | аудитория токена |
| `KEYCLOAK_ISSUER_URI`, `KEYCLOAK_JWK_SET_URI`, `KEYCLOAK_JWK_READ_TIMEOUT`, `JWT_CLOCK_SKEW` | как у device-service | проверка токена |
| `ROUTER_MANAGER_CONSUMER_SECRET` / `ROUTER_MANAGER_READER_SECRET` | секреты | клиенты realm |
| `ROUTER_GRPC_REFLECTION` / `ROUTER_MAX_CONCURRENT_STREAMS` / `ROUTER_SHUTDOWN_TIMEOUT` | `true` / `1000` / `10s` | сервер |
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_SERVICE_NAME` / `LOG_LEVEL` | `http://alloy:4319` / `router-manager` / `INFO` | наблюдаемость |
| `ROUTER_PPROF_ENABLED` | `false` | pprof на порту метрик (необязательная часть) |
| `PROTOS_VERSION` / `PROTOS_IMAGE_TAG` | `1.0.0` / `0.1.0` | версия контракта для сборки, образ protos-publish |
| `ROUTER_MANAGER_IMAGE_TAG` | `0.1.0` | теги образов router-manager и эмулятора |
| `EMULATOR_ROUTERS` / `EMULATOR_POLL_INTERVAL` / `EMULATOR_EXEC_DELAY` | `20` / `10s` / `2s` | эмулятор |
| `EMULATOR_FAILURE_RATE` / `EMULATOR_SILENT_RATE` / `EMULATOR_ROUTER_PREFIX` | `0.1` / `0.05` / `emu-router-` | эмулятор |
| `EMULATOR_TARGET` / `EMULATOR_METRICS_PORT` | `dns:///router-manager:9090` / `8082` | эмулятор |
| `ROUTER_MANAGER_DB_NAME` | `iot_commands` | `router-manager-db-init` |
| `DB_URL`, `DB_USER` / `DB_PASSWORD` у `db-migrator-router-manager` | `jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${ROUTER_MANAGER_DB_NAME}`, `${POSTGRES_USER}` / `${POSTGRES_PASSWORD}` | миграции под владельцем базы (`MIGRATION_SET=router-manager`), значения — в Compose из уже существующих переменных |

## Порядок старта

`postgres` → `router-manager-db-init` (база и роль) → `db-migrator-router-manager` → `nexus-init` → `protos-publish` →
`redis` + Sentinel, `keycloak` → `router-manager` ×2 → (`emulator`) `router-emulator`. Сборка образа router-manager берёт
`protos-${PROTOS_VERSION}.jar` из Nexus (как Java-сервисы avros).

## Проверка

`make protos-check`, `make rm-test` (`go test -race ./...`), `make rm-coverage` (≥ 70 %), `make rm-it` (Docker), `make check`,
`make up` → `make rm-quick-check` (опрос, выдача, ответ, зависшая команда и оповещение через Grafana API), `make obs-check`.
Интеграционные тесты с Keycloak идут на стенде владельца (образ Keycloak там amd64 — старт под эмуляцией ~1 мин).

## Complexity Tracking

| Отступление | Почему | Почему не проще |
|---|---|---|
| Второй язык (Go) при «Java 25, Spring Boot 4» в конституции | Требование задания; поправка конституции 1.3.0: Go для узкого сетевого сервиса, gRPC из protos | — |
| Метрики client_golang, а не Micrometer | Micrometer — Java; имена и правила меток те же (без `router_id`, `command_id`) | — |
| Доступ к базе мимо ShardingSphere-Proxy (AGENTS.md: «сервисы работают с БД через Proxy») | Proxy обслуживает реестр устройств; у router-manager своя нешардированная база и pgx по заданию; правило AGENTS.md уточняется | Шардирование команд по роутеру не нужно на этом объёме и усложнило бы `SKIP LOCKED` |
| `postgres` из профиля `platform` в базовый стенд (+~100 МБ) | База сервиса должна быть в базовом стенде | Отдельный экземпляр PostgreSQL — ещё один контейнер и неиспользуемая `iot_commands` |
| Проверка совместимости protos в контейнере (`buf` не Java) | `buf breaking` — стандарт для Protobuf, Java-аналога нет | Своя сверка дескрипторов на Java — дольше и слабее |
