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
