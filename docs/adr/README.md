# Architecture Decision Records

| ADR | Решение |
|---|---|
| [0001](0001-write-through-kafka.md) | Запись через Kafka, единственный писатель реестра |
| [0002](0002-device-info-operation.md) | Поле operation в DeviceInfo |
| [0003](0003-cache-invalidation-by-owner.md) | Кэш сбрасывает владелец данных после COMMIT |
| [0004](0004-read-from-replicas.md) | Чтение device-service только из реплик |
| [0005](0005-soft-delete-and-manual-priority.md) | Мягкое удаление и приоритет ручных изменений |
| [0006](0006-device-id-assigned-by-client.md) | deviceId назначает клиент |
| [0007](0007-service-tokens-only.md) | Только сервисные токены Keycloak |
| [0008](0008-redis-sentinel.md) | Redis через Sentinel для всей платформы |
| [0009](0009-api-first-client-library.md) | API First и клиентская библиотека |
| [0010](0010-devices-topic-compaction-lag.md) | Сжатие темы devices и команды администратора (заменено ADR-0011) |
| [0011](0011-devices-topic-retention.md) | Тема devices хранит сообщения вместо сжатия |
| [0012](0012-service-metrics-via-alloy.md) | Метрики сервисов платформы собирает Alloy |
| [0013](0013-failed-events-topic-and-contract.md) | Единая тема failed-events и контракт FailedEvent |
| [0014](0014-failed-object-key-and-idempotent-write.md) | Ключ объекта в MinIO и идемпотентная запись |
| [0015](0015-minio-image-buckets-access.md) | Образ MinIO, бакеты и учётки |
| [0016](0016-mimir-and-object-storage-for-telemetry.md) | Mimir вместо Prometheus; Loki и Tempo хранят данные в MinIO |
| [0017](0017-database-backups-to-minio.md) | Копии PostgreSQL и ClickHouse в MinIO контейнером backup-runner |
| [0018](0018-failed-events-processor-failure-modes.md) | Поведение failed-events-processor при сбоях |
| [0019](0019-optional-return-and-object-lock.md) | Необязательная часть: возврат объекта в топик и Object Lock |
| [0020](0020-router-manager-replaces-command-service.md) | router-manager вместо планового Command Service |
| [0021](0021-command-issue-once.md) | Выдача команды опросом ровно один раз |
| [0022](0022-negative-cache-with-generation.md) | Негативный кэш с поколением роутера |
| [0023](0023-router-authentication-hmac.md) | Проверка вызывающих: сервисный токен и подпись роутера |
| [0024](0024-protos-project.md) | Проект protos и генерация Go-кода |
| [0025](0025-stuck-commands-and-alert.md) | Зависшие команды и оповещение на каждую |
| [0026](0026-router-manager-database.md) | База router-manager и миграции |
