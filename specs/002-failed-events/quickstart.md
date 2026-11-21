# Quickstart: failed-events

Как запустить стенд и проверить сценарии spec.md. Команды сверяются с реализацией задачами T048, T049, T060, T069–T071: расхождение —
правка здесь, не молчаливая подмена. Порт сервиса из диапазона берётся через `docker compose port <сервис> 8080`, не числом.

## Подготовка

1. `cp .env.example .env`, пароли `change_me_*` заменить своими; `NEXUS_ACCEPT_EULA=true` — только если принимаете лицензию.
2. `make up` — базовый стенд: Kafka и `kafka-init` (тема `failed-events`), MinIO и `minio-init` (бакеты, учётки), сборщики, `failed-events-processor`.
   `make up PROFILES=observability` — Mimir, Loki, Tempo, Alloy, Grafana.
3. Алиас `mc` внутри контейнера MinIO (ключи из окружения контейнера, в историю не попадают):
   `docker compose exec minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"'`.
4. Все контейнеры `healthy`, `minio-init` — `exited (0)`: `docker compose ps`.

## Сценарии

| # | Сценарий | Что сделать | Ожидание (FR) |
|---|---|---|---|
| 1 | Бакеты и права | `make minio-check` | пять бакетов, `403` на чужое, ILM из `.env` (FR-014…016) |
| 2 | Ошибка приёма событий | `make fep-quick-check`: невалидное событие в `events` с `traceparent` | объект `events-collector-service/<дата>/VALIDATION_FAILED/events-<p>-<o>.json`; `traceId` в JSON равен заданному; смещение `events` подтверждено (FR-004, 007, 011) |
| 3 | Ошибка реестра устройств | то же: невалидное сообщение в `devices` | объект `device-collector-service/…/VALIDATION_FAILED/devices-<p>-<o>.json` (FR-004) |
| 4 | Нечитаемые байты | то же: мусор в `events` | объект `DESERIALIZATION_FAILED`, `base64(value)` равно отправленному (FR-005, 007) |
| 5 | Повторная доставка | остановить процессор, `kafka-consumer-groups --reset-offsets --to-earliest --group failed-events-processor --topic failed-events --execute`, запустить | число объектов и версий не изменилось, `failed_events_archived_total{outcome="duplicate"}` выросло (FR-008, SC-002) |
| 6 | Всплеск | `docker compose stop shardingsphere-proxy`, отправить N событий в `events`, подождать `EVENTS_MAX_ATTEMPTS` попыток, запустить Proxy | N записей `STORAGE_UNAVAILABLE`, затем N объектов; замерена скорость процессора; основная тема не стоит (FR-012, SC-001) |
| 7 | MinIO недоступен | `docker pause` MinIO на 45 с во время потока ошибок, затем `unpause` | `kafka-consumer-groups --describe --state`: группа `Stable`, смещение стояло и пошло, объекты появились (FR-009, SC-003) |
| 8 | Нечитаемая запись `failed-events` | положить мусор в `failed-events` консольным продюсером | объект `unreadable/<дата>/failed-events-<p>-<o>.json`, группа дальше читает (FR-010) |
| 9 | Поиск | `mc ls --recursive local/failed-events/<сервис>/<yyyy>/<MM>/<dd>/<код>/`; `mc find local/failed-events --metadata "x-amz-meta-trace-id=<traceId>"`; то же по `device-id` | найден объект тестового сообщения (FR-013) |
| 10 | Трасса | `make up PROFILES=observability`; Grafana → Explore → Tempo → `traceId` из сценария 2 | спаны ECS, публикации в `failed-events`, слушателя процессора, `s3.put-object`; те же логи в Loki (FR-011) |
| 11 | Телеметрия в MinIO | `make obs-check` | объекты в `mimir-blocks`, `loki-chunks`, `tempo-traces`; все запросы панелей — 200 (FR-017…019) |
| 12 | Копии и восстановление | `make backup`, `make backup-list`; затем `make backup-check` (удаляет данные своего проекта compose) | в `backups/postgres/` и `backups/clickhouse/` свежие копии с manifest; после `make shard-restore FROM=minio BACKUP=<метка>` и `make clickhouse-restore BACKUP=<метка>` числа равны manifest (FR-020, 021, SC-004) |

## Восстановление баз из копии (шаги для README)

1. `make backup-list` — метки копий в MinIO.
2. PostgreSQL: `make shard-restore FROM=minio BACKUP=<метка> CONFIRM=yes` — скачивает копии, проверяет контрольные суммы, останавливает
   device-collector-service и Proxy, восстанавливает шарды, возвращает смещения группы к manifest, запускает Proxy.
3. ClickHouse: `make clickhouse-restore BACKUP=<метка>`.
4. Проверка: `make dcs-quick-check` и сравнение чисел устройств/строк с manifest.

Не PITR: восстанавливается состояние на момент копии; события после неё из Kafka перечитываются по возвращённым смещениям.

## Выключение

`make down` (контейнеры всех профилей, тома остаются). Не `make clean` и не `docker compose down -v`: удаляют объекты и копии.
