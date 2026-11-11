# Implementation Plan: device-service — управление устройствами

**Feature**: `001-device-service` | **Date**: 2026-11-11 | **Spec**: [spec.md](spec.md)
**Input**: spec.md, конституция `.specify/memory/constitution.md`, код `main`.

## Summary

Новый сервис `device-service` — точка входа CRUD и сервис чтения (гибридный CQRS). Запись: проверка запроса и
токена → `DeviceInfo` с `operation` в тему `devices` → `202` после ack Kafka. Применяет команды единственный писатель
реестра — device-collector-service; после `COMMIT` он поднимает `floor` версии в Redis. Чтение: Cache-Aside, промах —
логическая база Proxy `devices_read` на репликах. API First: OpenAPI → DTO и HTTP Service Client в библиотеке
`device-service-client` (Nexus). Только сервисные токены Keycloak, realm в репозитории.

## Technical Context

**Language/Version**: Java 25, Spring Boot 4.0.3 (как у device-collector-service)
**Primary Dependencies**: spring-boot-starter-webmvc, -security, -oauth2-resource-server, -kafka, -data-redis (Lettuce),
-jdbc, -actuator, -opentelemetry; kafka-avro-serializer 8.2.4; avros 1.3.0; OpenAPI Generator Gradle plugin 7.17.0;
MapStruct 1.6.3
**Storage**: нет своего; чтение — Redis 8.6 (Sentinel), реплики PostgreSQL 18 через ShardingSphere-Proxy 5.5.3
**Testing**: JUnit 5, Testcontainers (Kafka 4.2, Schema Registry 8.2.4, PostgreSQL 18, Redis 8.6, Keycloak 26 через
`testcontainers-keycloak`), ArchUnit; JaCoCo ≥ 70 % строк
**Target Platform**: Docker Compose платформы, два экземпляра
**Project Type**: веб-сервис + библиотека клиента в монорепозитории Gradle
**Performance Goals**: учебный стенд; изменение видно через GET ≤ 5 с (SC-001)
**Constraints**: без состояния; тайм-ауты у всех исходящих вызовов; Mac Димы ~7,75 ГБ на Docker (research #10)

## Constitution Check

| Принцип | Как выполняется | Статус |
|---|---|---|
| I. Один владелец | device-service не пишет в БД; команды через Kafka к device-collector-service ([ADR-0001](../../docs/adr/0001-write-through-kafka.md)) | ✅ |
| II. Контракты | `operation` — необязательное поле, FULL; OpenAPI в репозитории, DTO генерируются; V3 — `IF NOT EXISTS` | ✅ |
| III. Идемпотентность | журнал `eventId` в device-collector-service, `202` после ack, сравнение времени с `managed_at` / `last_state_at` | ✅ |
| IV. Без состояния, env | `STATELESS`, кэш только в Redis; все параметры — `.env.example` | ✅ |
| V. Безопасность | resource server, проверка `aud` и сервисной учётной записи, Problem Details без внутренностей | ✅ |
| VI. Наблюдаемость | метрики без `deviceId`, JSON-логи, трасса HTTP → Kafka → PostgreSQL, панель device-service | ✅ |
| VII. Тесты | IT с настоящим Keycloak и Testcontainers; полный путь — research #8 | ⚠ зависит от spike |

## Project Structure

### Documentation

```text
specs/001-device-service/
├── spec.md · plan.md · research.md · data-model.md · tasks.md · analyze-report.md
└── contracts/device-service.yaml   # переезжает в services/device-service-client/src/main/openapi/ (T011)
docs/adr/0001…0009-*.md
diagrams/containers.mmd, component-device-service.mmd, ds-*-sequence.mmd, dcs-operations-sequence.mmd
```

### Source Code

```text
services/device-service-client/          # java-library + maven-publish
└── src/main/openapi/device-service.yaml # источник истины; build/generated — DevicesApi + DTO
services/device-service/
├── Dockerfile                           # JDK 25 → JRE 25
└── src/main/java/com/github/gluhov/iotplatform/deviceservice/
    ├── adapter/in/web/           DeviceController implements DevicesApi, ProblemDetailsAdvice
    ├── adapter/in/web/security/  SecurityConfig, ServiceAccountValidator, KeycloakRolesConverter, Problem*Handler
    ├── adapter/out/kafka/        KafkaDeviceCommandPublisher
    ├── adapter/out/redis/        RedisDeviceCache (Lua)
    ├── adapter/out/postgres/     JdbcDeviceReadRepository (база devices_read)
    ├── domain/model, domain/port/{in,out}, domain/service/  DeviceCommandService, DeviceQueryService
    └── support/                  DeviceServiceMetrics, DeviceMapper (MapStruct)
infrastructure/config/keycloak/iot-platform-realm.json
infrastructure/config/shardingsphere/database-devices_read.yaml
infrastructure/config/redis/sentinel.conf
infrastructure/config/grafana/dashboards/device-service.json
services/db-migrator/.../V3__device_management.sql
```

**Structure Decision**: пакеты — как у device-collector-service (порты и адаптеры, ArchUnit). Клиент — отдельный
подпроект: его публикуют в Nexus, сервис подключает его как `project(...)`.

## Ключевые решения

| Решение | Почему | ADR |
|---|---|---|
| Запись — команда в `devices`, `202` после ack | один писатель, нет двойной записи | 0001 |
| `operation`: `null` — событие роутера, `UNKNOWN` — в DLT | старые сообщения читаются как есть; новая операция не станет созданием | 0002 |
| Сброс кэша владельцем после `COMMIT` + `floor` версии | реплика отстаёт: простой DEL не спасает от старых данных в кэше | 0003 |
| `devices_read` на репликах | маршрут DCS не меняется; device-service физически не может попасть в основной узел | 0004 |
| Мягкое удаление, приоритет ручных правок | устройство не воскресает от запоздалых событий; часы роутера не решают конфликты | 0005 |
| `deviceId` задаёт клиент | совпадает с событиями роутера, повтор POST не создаёт дубль | 0006 |
| Только сервисные токены; realm в репозитории; Keycloak в базовом стенде (`dev-file`, импорт при старте) | нулевое доверие; воспроизводимый стенд | 0007 |
| Redis через Sentinel для всех сервисов | общий Redis должен переживать отказ master | 0008 |
| OpenAPI в `device-service-client`, два прогона генератора | API First; `spring-http-interface` не даёт Bean Validation (research) | 0009 |

## Ошибки и тайм-ауты

| Ситуация | Ответ / действие |
|---|---|
| Нет токена, подпись, `iss`, `exp`, `aud` | `401 UNAUTHORIZED` |
| Пользовательский токен / нет роли | `403 SERVICE_TOKEN_REQUIRED` / `403 FORBIDDEN` |
| Тело или `deviceId` невалидны | `400 VALIDATION_FAILED` + `errors[]` |
| POST существующего / PUT, DELETE отсутствующего | `409 DEVICE_ALREADY_EXISTS` / `404 DEVICE_NOT_FOUND` |
| Kafka не подтвердила за `DEVICE_COMMAND_SEND_TIMEOUT` (5 с) | `503 COMMAND_NOT_ACCEPTED` |
| Redis недоступен (`REDIS_TIMEOUT` 500 мс) | чтение из реплик, метрика `result=error` |
| Proxy/реплика недоступны при промахе (`connectTimeout` 2 с, `socketTimeout` 3 с) | `503 READ_MODEL_UNAVAILABLE` |
| Непредвиденная ошибка | `500 INTERNAL_ERROR`, стек — только в лог |

## Новые переменные окружения

`DEVICE_SERVICE_IMAGE_TAG`, `DEVICE_SERVICE_PORTS=8105-8106`, `DEVICE_CACHE_TTL=10m`, `DEVICE_COMMAND_SEND_TIMEOUT=5s`,
`DEVICE_SERVICE_AUDIENCE=device-service`, `KEYCLOAK_REALM=iot-platform`, `KEYCLOAK_ISSUER_URI`, `KEYCLOAK_JWK_SET_URI`,
`DEVICE_SERVICE_CONSUMER_SECRET`, `DEVICE_SERVICE_READER_SECRET`, `DEVICES_READ_DB_URL`, `DEVICES_READ_DB_USER`,
`DEVICES_READ_DB_PASSWORD`, `SHARD_READER_PASSWORD`, `REDIS_SENTINEL_MASTER=platform-redis`,
`REDIS_SENTINEL_NODES=redis-sentinel-1:26379,redis-sentinel-2:26379,redis-sentinel-3:26379`.

## Порядок старта

`nexus → avros-publish → avros-register → db-migrator → shard-replication-init → Proxy`; `redis → redis-replica →
sentinel ×3`; `keycloak` (импорт realm, healthy) → `device-collector-service`, `events-collector-service`,
`device-service ×2`.

## Complexity Tracking

| Отступление | Зачем | Почему проще не подходит |
|---|---|---|
| Поле `floor` и Lua вместо `@Cacheable/@CacheEvict` | атомарная проверка версии при записи в кэш | аннотации не умеют условную запись, гонка из теории остаётся |
| Вторая логическая база Proxy | чтение только из реплик | read/write splitting в той же базе меняет маршруты транзакций DCS (`FOR UPDATE`, `PREVIEW`) |
| Keycloak переезжает из профиля `platform` в базовый стенд | сервис без него не работает | профиль пришлось бы включать всегда |
