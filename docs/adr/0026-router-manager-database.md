# ADR-0026: База router-manager и миграции

**Статус**: предложено, 2026-11-28 (на проверке дизайна). Спецификация — [specs/003-router-manager](../../specs/003-router-manager/spec.md).

## Контекст

Задание: у сервиса своя база, доступ через pgx без ORM, миграции выполняет db-migrator. Шарды реестра устройств за
ShardingSphere-Proxy — чужие данные. Общий `postgres` платформы есть только в профиле `platform`; база `iot_commands` уже
заложена в `POSTGRES_EXTRA_DBS`.

## Решение

- База `iot_commands` на общем `postgres` (PostgreSQL 16); `postgres` переходит в базовый стенд (+~100 МБ).
- Роль `router_manager` с паролем из окружения создаёт и обновляет одноразовая задача `router-manager-db-init` (psql,
  идемпотентно: база и роль при отсутствии, пароль на каждом запуске) — тот же приём, что `shard-replication-init` для `device_reader`.
- Схема — миграции db-migrator в `db/migration/router-manager/` (`IF NOT EXISTS`), отдельный запуск `db-migrator-router-manager`
  (`MIGRATION_SET=router-manager`) под владельцем базы; роль сервиса получает только `SELECT, INSERT, UPDATE`.
- Сервис ходит в базу pgx напрямую, мимо Proxy; правило AGENTS.md «сервисы работают с БД через Proxy» уточняется: это про реестр устройств.

## Рассмотренные варианты

- Отдельный контейнер PostgreSQL для router-manager: полная изоляция ценой ещё одного экземпляра; `iot_commands` осталась бы
  неиспользованной.
- Шарды за Proxy: шардирование команд не нужно, `SKIP LOCKED` через Proxy не проверялось.
- Миграции самим сервисом при старте: два экземпляра гонятся за блокировкой схемы, а правило проекта — db-migrator.

## Последствия

- Init-скрипт образа PostgreSQL выполняется только на пустом томе, поэтому на существующем томе `postgres-data` база и роль
  могут отсутствовать; `router-manager-db-init` создаёт их при любом состоянии тома.
- Копии базы `iot_commands` в backup-runner этой частью не добавляются.
