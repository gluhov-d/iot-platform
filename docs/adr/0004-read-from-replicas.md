# ADR-0004: Чтение device-service только из реплик

**Статус**: предложено, 2026-11-11 · **Спецификация**: [specs/001-device-service](../../specs/001-device-service/spec.md)

## Контекст

У каждого шарда есть реплика hot standby. Proxy сейчас ходит только в основные узлы (логическая база `devices`).
Read/write splitting в той же базе изменил бы маршруты device-collector-service: `SELECT … FOR UPDATE`, `PREVIEW`,
чтения внутри транзакции.

## Решение

- Вторая логическая база Proxy `devices_read` (`database-devices_read.yaml`): источники `replica_0`, `replica_1` —
  `postgres-shard-N-replica`, то же правило шардирования по `device_id`.
- device-service подключается только к `devices_read` отдельным пользователем Proxy, которому доступна только эта
  база (`DATABASE_PERMITTED`). На репликах — роль `device_reader` (`pg_read_all_data`), созданная на основных узлах
  и пришедшая репликацией.
- Схемой таблицы по-прежнему владеет device-collector-service (db-migrator); изменения схемы обратно совместимы.

## Последствия

- В основной узел device-service попасть не может ни настройкой, ни ошибкой кода; проверка — `pg_stat_activity`.
- Реплика недоступна — промахи кэша по её шарду отвечают `503 READ_MODEL_UNAVAILABLE`; переключения на основной
  узел нет (нагрузка чтения не должна мешать писателю).
- Зависимость от структуры `devices` — осознанный компромисс модели чтения.

## Рассмотренные варианты

Read/write splitting в базе `devices`; прямое подключение к репликам мимо Proxy (сервису пришлось бы знать
топологию и правило шардов); второй экземпляр Proxy (запасной вариант spike).
