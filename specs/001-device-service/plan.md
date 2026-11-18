# Implementation Plan: device-service — управление устройствами

**Feature**: `001-device-service` | **Date**: 2026-11-13 | **Spec**: [spec.md](spec.md)
**Input**: spec.md, конституция `.specify/memory/constitution.md`, код ветки `main`.

## Summary

`device-service` — точка входа CRUD и сервис чтения (гибридный CQRS). Запись: проверка токена и запроса →
`DeviceInfo` с `operation` в тему `devices` → `202` после подтверждения Kafka. Команды применяет единственный
писатель реестра, device-collector-service; после `COMMIT` он поднимает в Redis наименьшую версию, которую можно
кэшировать. Чтение: Cache-Aside, промах — логическая база Proxy `devices_read` на репликах. API First: OpenAPI →
DTO и HTTP Service Client в библиотеке `device-service-client`. Только сервисные токены Keycloak, realm в репозитории.

## Technical Context

**Language/Version**: Java 25, Spring Boot 4.0.3 (как у device-collector-service)
**Primary Dependencies**: `spring-boot-starter-webmvc`, `-security`, `-oauth2-resource-server`, `-kafka`, `-data-redis`
(Lettuce), `-jdbc`, `-validation`, `-actuator`, `-opentelemetry`; `micrometer-registry-prometheus`;
`kafka-avro-serializer` 8.2.4; avros 1.3.0; OpenAPI Generator (Gradle) 7.17.0; MapStruct 1.6.3; PostgreSQL JDBC
**Storage**: своего нет; чтение — Redis 8.6 через Sentinel и реплики PostgreSQL 18 через ShardingSphere-Proxy 5.5.3
**Testing**: JUnit 5, Testcontainers (Kafka 4.2, Schema Registry 8.2.4, PostgreSQL 18, Redis 8.6, Keycloak 26),
ArchUnit, JaCoCo ≥ 70 % строк; `*Test` — unit, `It*Test` — интеграционные
**Target Platform**: Docker Compose платформы, два экземпляра
**Project Type**: веб-сервис и библиотека клиента в монорепозитории Gradle
**Performance Goals**: учебный стенд; изменение видно при чтении ≤ 5 с (SC-001)
**Constraints**: без состояния; тайм-ауты у всех исходящих вызовов; предположения research.md проверяются до кода;
Proxy не стартует без реплик (ADR-0004)

## Constitution Check

| Принцип | Как выполняется | Статус |
|---|---|---|
| I. Один владелец | device-service не пишет в базу; команды — через Kafka владельцу ([ADR-0001](../../docs/adr/0001-write-through-kafka.md)) | ✅ |
| II. Контракты | `operation` необязательное, FULL; OpenAPI в репозитории, DTO генерируются; V3 — `IF NOT EXISTS` | ✅ |
| III. Идемпотентность | журнал по `eventId`; `202` после подтверждения; команда сравнивается с `managed_at` | ✅ |
| IV. Без состояния, окружение | `STATELESS`, кэш только в Redis; все параметры — `.env.example` | ✅ |
| V. Безопасность | resource server; `aud`, сервисная учётная запись, роли; Problem Details без внутренностей | ✅ |
| VI. Наблюдаемость | метрики без `deviceId`, JSON-логи, трасса HTTP → Kafka → PostgreSQL, панель сервиса | ✅ |
| VII. Тесты | Testcontainers и настоящий Keycloak; полный путь зависит от R12 | ⚠ до проверки R12 |
| Необратимые действия — человек | публикация avros и клиента, коммиты — задачи с пометкой **ЧЕЛОВЕК** | ✅ |

## Project Structure

```text
specs/001-device-service/   spec · plan · research · data-model · quickstart · tasks · analyze-report
└── (контракт API: services/device-service-client/src/main/openapi/device-service.yaml, перенесён T003)
docs/adr/0001…0011 · diagrams/*.mmd · AGENTS.md

services/device-service-client/       # java-library + maven-publish
└── src/main/openapi/device-service.yaml   # источник истины; build/generated — DevicesApi и DTO
services/device-service/
├── Dockerfile · scripts/quick-check.sh
└── src/main/java/com/github/gluhov/iotplatform/deviceservice/
    ├── adapter/in/web/            DeviceController, DeviceMapper, ProblemDetailsAdvice, ErrorCode
    ├── adapter/in/web/security/   SecurityConfig, AudienceValidator, ServiceAccountValidator,
    │                              KeycloakRolesConverter, Problem*Handler
    ├── adapter/out/kafka/         KafkaDeviceCommandPublisher, DeviceInfoFactory
    ├── adapter/out/redis/         RedisDeviceCache (put-if-fresh.lua)
    ├── adapter/out/postgres/      JdbcDeviceReadRepository
    ├── config/                    KafkaConfig и свойства
    ├── domain/model · domain/port/{in,out} · domain/exception
    ├── domain/service/            DeviceQueryService, DeviceCommandService
    └── support/                   MicrometerDeviceServiceMetrics
infrastructure/config/{keycloak,redis}/ · infrastructure/config/shardingsphere/database-devices_read.yaml
infrastructure/config/grafana/dashboards/device-service.json
services/db-migrator/…/V3__device_management.sql
```

**Structure Decision**: пакеты — как у device-collector-service (порты и адаптеры, проверка ArchUnit). Клиент —
отдельный подпроект: его публикуют в Nexus, сервис подключает его как `project(...)`.

## Ключевые решения

| Решение | Почему | ADR |
|---|---|---|
| Запись — команда в `devices`, `202` после подтверждения | один писатель, нет двойной записи | 0001 |
| `operation`: `null` — поле не заполнено, `UNKNOWN` — в DLT | прежние сообщения читаются как есть; новая операция не станет созданием | 0002 |
| Кэш сбрасывает владелец после `COMMIT`, версия записи; порог — отдельный ключ со своим сроком | реплика отстаёт: простое удаление ключа не спасает от старых данных, а порог в том же ключе истекал вместе с ответом | 0003 |
| `devices_read` на репликах | маршруты device-collector-service не меняются; device-service не может попасть в основной узел | 0004 |
| Мягкое удаление, приоритет ручных правок | устройство не воскресает; часы роутера не решают конфликты | 0005 |
| `deviceId` задаёт клиент | совпадает с событиями роутера; повтор POST не создаёт дубль | 0006 |
| Только сервисные токены; realm в репозитории; Keycloak в базовом стенде | нулевое доверие; воспроизводимый стенд | 0007 |
| Redis через Sentinel для всех сервисов | общий Redis переживает отказ основного узла | 0008 |
| OpenAPI в `device-service-client`, два прогона генератора | API First; один прогон не даёт проверок полей (R2) | 0009 |
| Тема `devices` — `cleanup.policy=delete`, хранение ≥ интервала копирования | сжатие стирало команду, за которой пришёл снимок роутера: повторное чтение возвращало удалённое устройство | 0011 (заменяет 0010) |

## Ошибки

| Ситуация | Ответ |
|---|---|
| Нет токена; подпись, `iss`, срок, `aud` | `401 UNAUTHORIZED` |
| Пользовательский токен / нет роли | `403 SERVICE_TOKEN_REQUIRED` / `403 FORBIDDEN` |
| Тело или `deviceId` невалидны | `400 VALIDATION_FAILED` + `errors[]` |
| POST существующего / PUT, DELETE отсутствующего | `409 DEVICE_ALREADY_EXISTS` / `404 DEVICE_NOT_FOUND` |
| Kafka не подтвердила за `DEVICE_COMMAND_SEND_TIMEOUT` | `503 COMMAND_NOT_ACCEPTED` |
| Redis недоступен | чтение из реплик, `device_cache_requests_total{result="error"}` |
| Proxy или реплика недоступны при промахе | `503 READ_MODEL_UNAVAILABLE` |
| Непредвиденная ошибка | `500 INTERNAL_ERROR`, стек — только в лог |

## Переменные окружения

В Compose — без значений по умолчанию (исключения `COMPOSE_PROJECT_NAME`, `DEVICE_COLLECTOR_CONSUMER_GROUP` — AGENTS.md
«Конфигурация», T109); значения — в `.env.example`.

| Переменная | Пример | Кому |
|---|---|---|
| `DEVICE_SERVICE_IMAGE_TAG`, `DEVICE_SERVICE_PORTS` | `0.1.0`, `8105-8106` | device-service |
| `DEVICE_CACHE_TTL` | `10m` | device-service (ключ `device:v1:{deviceId}`) |
| `DEVICE_CACHE_FLOOR_TTL` | `24h` | device-service, device-collector-service (ключ `device:v1:floor:{deviceId}`; больше худшего ожидаемого отставания реплики) |
| `DEVICE_COMMAND_SEND_TIMEOUT` | `5s` | device-service |
| `SCHEMA_REGISTRY_HTTP_CONNECT_TIMEOUT_MS`, `SCHEMA_REGISTRY_HTTP_READ_TIMEOUT_MS`, `SCHEMA_REGISTRY_MAX_RETRIES` | `1000`, `2000`, `0` | device-service (клиент Schema Registry; сумма меньше `DEVICE_COMMAND_SEND_TIMEOUT`, T111) |
| `DEVICE_SERVICE_MANAGEMENT_PORT` | `8081` | device-service, Prometheus (в `prometheus.yml` — литерал), healthcheck; на хост не публикуется |
| `DEVICES_READ_DB_URL`, `DEVICES_READ_DB_USER`, `DEVICES_READ_DB_PASSWORD` | `jdbc:postgresql://shardingsphere-proxy:3307/devices_read?connectTimeout=2&socketTimeout=3` | device-service, Proxy |
| `SHARD_READER_PASSWORD` | — | `shard-replication-init`, Proxy |
| `KEYCLOAK_REALM`, `KEYCLOAK_ISSUER_URI`, `KEYCLOAK_JWK_SET_URI`, `DEVICE_SERVICE_AUDIENCE` | `iot-platform`, …, `device-service` | device-service |
| `DEVICE_SERVICE_CONSUMER_SECRET`, `DEVICE_SERVICE_READER_SECRET` | — | Keycloak (realm), quickstart |
| `REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_NODES`, `REDIS_MAXMEMORY` | `platform-redis`, `redis-sentinel-1:26379,…`, `256mb` | все сервисы, Redis |
| `IOT_NET_SUBNET`, `IOT_NET_IP_RANGE`, `REDIS_MASTER_IP`, `REDIS_REPLICA_IP`, `REDIS_SENTINEL_IPS` | свободная подсеть `/24`, верхняя половина подсети `/25` для динамических адресов и статические адреса Redis и Sentinel в нижней | сеть Compose, Redis, Sentinel |
| `KAFKA_DEVICES_RETENTION_MS` | `2592000000` | `kafka-init`, `shard-restore.sh` |

Уже есть и используются: `REDIS_PASSWORD`, `REDIS_TIMEOUT`, `KAFKA_DEVICES_TOPIC`, `SCHEMA_REGISTRY_PORT`,
`KEYCLOAK_VERSION`, `KEYCLOAK_PORT`.

## Порядок старта

`nexus → avros-publish → schema-register → db-migrator → shard-replication-init → реплики → Proxy`;
`redis → redis-replica → redis-sentinel ×3`; `keycloak` (realm импортирован, `healthy`) →
`events-collector-service`, `device-collector-service`, `device-service ×2`.

## Проверка

- Сборка и тесты: `./gradlew :services:device-service-client:build`, `./gradlew :services:device-service:check`,
  `./gradlew :services:device-collector-service:check`, `./gradlew :services:events-collector-service:check`.
- Стенд: `make up`, `make check-local`, `make dcs-quick-check`, `make ds-quick-check` ([quickstart.md](quickstart.md)).
- Наблюдаемость: `make up PROFILES=observability`, панели и трасса — T066, T067.

## Complexity Tracking

| Отступление | Зачем | Почему проще не подходит |
|---|---|---|
| Версия записи, `floor` и Lua вместо `@Cacheable`/`@CacheEvict` | атомарная проверка версии при записи в кэш | аннотации не умеют условную запись; гонка со старой репликой остаётся |
| Два ключа на устройство (ответ и порог) со своими сроками | порог должен жить дольше ответа: иначе отставание реплики дольше `DEVICE_CACHE_TTL` возвращает старые данные | один хеш — один TTL; срок по полю (`HEXPIRE`) требует Redis 7.4+ и проверки на Lettuce |
| Вторая логическая база Proxy | чтение только из реплик | read/write splitting в той же базе меняет маршруты транзакций писателя |
| Keycloak переезжает из профиля `platform` в базовый стенд | без него сервис не принимает запросы | профиль пришлось бы включать всегда |
| Четвёртый символ `UNKNOWN` в `operation` | значение новее схемы читателя не становится созданием | default `CREATE` из примера молча создавал бы устройства |
