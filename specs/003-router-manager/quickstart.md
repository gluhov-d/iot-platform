# Quickstart: router-manager

Проверка сценариев spec.md на стенде владельца. Пароли и секреты — только из `.env`.

## Запуск

```bash
make protos-check
make up PROFILES=observability
make rm-emulator-up
```

Ожидаемо: `router-manager` ×2 и `router-emulator` в `docker compose ps` — `healthy`/`running`; в Grafana панель
«IoT Platform — router-manager» показывает опросы.

## Сценарии

| # | Сценарий | Команда | Ожидаемо |
|---|---|---|---|
| 1 | Полный путь (US1) | `make rm-quick-check` | шаг «SUCCESS» пройден |
| 2 | Зависшая команда и оповещение (US3) | `make rm-quick-check` | шаг «alert firing» — экземпляр `RouterManagerCommandStuck` с `command_id` |
| 3 | Без токена (US4) | `make rm-quick-check` | шаг «unauthenticated» |
| 4 | Unit с гонками | `make rm-test` | `ok` по всем пакетам, без `DATA RACE` |
| 5 | Интеграционные | `make rm-it` | все тесты T019–T034 `PASS` |
| 6 | Покрытие | `make rm-coverage` | итог ≥ 70 % |

## Ручной вызов

Порт экземпляра — `PORT=$(docker compose --env-file .env -f infrastructure/docker-compose.yaml port router-manager 9090 | sed 's/.*://')`.
Примеры `grpcurl` с токеном `router-manager-consumer` — в README (раздел router-manager): `CreateCommand`, `GetCommand`;
`PollCommand` и `ReportResult` — от имени роутера эмулятора (подпись считает `router-emulator sign`).

## Выключение

`make down` (тома остаются).
