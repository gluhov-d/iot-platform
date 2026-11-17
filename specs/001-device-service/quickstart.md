# Quickstart: device-service

Сценарий проверки на поднятом стенде после реализации (его автоматизирует `make ds-quick-check`, задача T062).
Основа раздела README. Команды — из корня репозитория, в `.env` заданы переменные из `.env.example`.

## 1. Запуск

```bash
make up
docker compose --env-file .env -f infrastructure/docker-compose.yaml ps device-service
```

Ожидается: два экземпляра `device-service` в состоянии `healthy`; Keycloak, Redis с Sentinel и Proxy — `healthy`.

## 2. Сервисный токен

```bash
set -a; . ./.env; set +a
KC=http://localhost:${KEYCLOAK_PORT}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token
TOKEN=$(curl -s -X POST "$KC" -d grant_type=client_credentials -d client_id=device-service-consumer \
  -d client_secret="${DEVICE_SERVICE_CONSUMER_SECRET}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')
PORT=$(docker compose --env-file .env -f infrastructure/docker-compose.yaml port --index 1 device-service 8080 | sed 's/.*://')
API=http://localhost:${PORT}/api/v1/devices
```

## 3. Запросы

| № | Запрос | Ожидается |
|---|---|---|
| 1 | `curl -s -o /dev/null -w '%{http_code}' "$API/qs-1"` | `401` |
| 2 | `curl -s -i -X POST "$API" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"deviceId":"qs-1","tenantId":"t-1","deviceType":"router","status":"ACTIVE","name":"Роутер склада"}'` | `202`, `Location: /api/v1/devices/qs-1` |
| 3 | `curl -s "$API/qs-1" -H "Authorization: Bearer $TOKEN"` (повторять до 5 с) | `200`, `"name":"Роутер склада"` |
| 4 | тот же POST ещё раз | `409`, `"code":"DEVICE_ALREADY_EXISTS"` |
| 5 | `curl -s -i -X PUT "$API/qs-1" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"tenantId":"t-1","deviceType":"router","status":"MAINTENANCE","name":"Роутер цеха"}'` | `202` |
| 6 | GET как в строке 3 (до 5 с) | `200`, `"name":"Роутер цеха"`, `version` вырос |
| 7 | `curl -s -i -X PUT "$API/qs-1" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"status":"ACTIVE"}'` | `400`, `"code":"VALIDATION_FAILED"`, в `errors` — `tenantId`, `deviceType` |
| 8 | `curl -s -i -X DELETE "$API/qs-1" -H "Authorization: Bearer $TOKEN"` | `202` |
| 9 | GET как в строке 3 (до 5 с) | `404`, `"code":"DEVICE_NOT_FOUND"`, есть `traceId` |

## 4. Проверки платформы

| Что | Команда | Ожидается |
|---|---|---|
| Команды в теме | `make topics` и чтение `devices` (как в `check-local.sh`) | три сообщения с ключом `qs-1`: `CREATE`, `UPDATE`, `DELETE` |
| Чтение только из реплик | `SELECT count(*) FROM pg_stat_activity WHERE usename = 'device_reader'` на каждом основном узле и реплике | основные узлы — 0, реплики — больше 0 |
| Кэш | `make redis-cli`, затем `HGETALL device:v1:qs-1` сразу после строки 6 | `body`, `version`, срок жизни не больше `DEVICE_CACHE_TTL` |
| Отказ Redis | остановить контейнер основного узла Redis, повторить GET существующего устройства | `200`; Sentinel назначил новый основной узел |
| Трасса | `make up PROFILES=observability`, PUT, поиск трассы запроса в Tempo (сервис `device-service`) | спаны device-service, отправки в `devices`, device-collector-service, запроса к базе |

## 5. Выключение

```bash
make down
```
