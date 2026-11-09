# Definition of Done — IoT Platform

Критерии приёмки по частям платформы. Отметка ставится, когда пункт подтверждён указанной проверкой.
Пути — от корня репозитория. `dc` = `docker compose -f infrastructure/docker-compose.yaml`,
`svc` = `./gradlew :services:events-collector-service`.

Каждая новая часть добавляет свою секцию. Если она меняет требования к уже сделанному, пункты прежней секции
правятся на месте (прежние формулировки — в истории Git), а в новой секции есть перечень этих изменений.

- [Приём событий (events-collector-service)](#приём-событий-events-collector-service)
- [Реестр устройств (device-collector-service, avros)](#реестр-устройств-device-collector-service-avros)

---

# Приём событий (events-collector-service)

## Контракты и Kafka

- [ ] Схемы `DeviceEvent` и `DeviceInfo` — в проекте `avros/` (см. «Реестр устройств», раздел avros); поля и типы — как
  в задании, namespace — `com.github.gluhov.iotplatform.events.avro`. Локальных `.avsc` в сервисе нет.
- [ ] Входная тема `events`, выходная `devices` (`.env.example`: `KAFKA_EVENTS_TOPIC`, `KAFKA_DEVICES_TOPIC`).
- [ ] Ключ обеих тем - `deviceId`: `DeviceEvent` отправляется с ключом `deviceId` (`check-local.sh`, `ItEventsCollectorTest`),
        `DeviceInfo` - `KafkaDeviceInfoProducer` (`KafkaDeviceInfoProducerTest.givenDevice_whenPublish_thenSendKeyedByDeviceId`).
- [ ] `events` - `cleanup.policy=delete`, `devices` - `cleanup.policy=compact`, `events-dlt` - `cleanup.policy=delete`: `make topics`.
- [ ] Темы создаются автоматически: контейнер `kafka-init` -> `infrastructure/init/kafka/init-topics.sh`,
        `make health` - у `kafka-init` exited, код 0.

## Прием событий

- [ ] Listener читает по одному сообщению: `max-poll-records: 1`, `listener.type: single`, `batch = "false"`
  (`application.yaml`, `KafkaEventListener`).
- [ ] `enable-auto-commit: false`, `ack-mode: manual_immediate` (`application.yaml`).
- [ ] Offset подтверждается только после записи в обе таблицы: `EventIngestionScenarioTest`
  (`givenValidEvent_whenOnEvent_thenStoreAndAcknowledge` - порядок insert -> insert -> ack;
  `givenEventInsertFailure_whenOnEvent_thenNoAck`, `givenOutboxInsertFailure_whenOnEvent_thenNoAck` - ack не вызван).
- [ ] Невалидные `eventId`/`deviceId`/`timestampMs` - ack не вызван, записи нет (`EventIngestionScenarioTest`,
  `givenBlank*`/`givenInvalidTimestamp*`).
- [ ] Ни одно сообщение не пропускается молча: временный сбой Schema Registry повторяется, недесериализуемые и
   невалидные сообщения откладываются в `events-dlt`; сообщение, не записанное в ClickHouse за `EVENTS_MAX_ATTEMPTS`
   попыток, — тоже в `events-dlt`; offset подтверждается только после записи в DLT
   (`KafkaConfig`, `KafkaErrorClassifier`); committed offset после ошибки и следующего корректного сообщения -
   `ItKafkaErrorHandlingTest`.
- [ ] Redis не участвует в приеме: `EventIngestionService` не зависит от Redis-портов.
- [ ] Успешный прием создает строку в `device_events` и начальную строку `device_outbox`
  (`ItEventsCollectorTest.givenEvent_whenIngest_thenStoredWithInitialOutbox`, `check-local.sh` шаги 3-4).

## Outbox

- [ ] `device_outbox` - `ReplacingMergeTree(state_version)` (`infrastructure/init/clickhouse/01-schema.sh`)
- [ ] Смена статуса - вставка новой версии; `ALTER TABLE ... UPDATE` отсутствует:
    `grep -rni "alter table" services/` - пусто.
- [ ] Выборка через `FINAL` (`ClickHouseOutboxRepository.SELECT_PENDING`).
- [ ] Повторная попытка увеличивает `attempts`
  (`OutboxPublishingServiceTest.givenKafkaFailure_whenPublishBatch_thenFailedVersionWithIncrementedAttempts`,
   `ItEventsCollectorTest.givenPublishFailure_whenRetry_thenSentOrStopsAfterMaxAttempts`).
- [ ] После `OUTBOX_MAX_ATTEMPTS` запись не выбирается (`ItEventsCollectorTest.givenPublishFailure_whenRetry_thenSentOrStopsAfterMaxAttempts`).
- [ ] Повторная доставка не откатывает SENT (`ItEventsCollectorTest.givenRedeliveredEvent_whenIngest_thenDuplicateCollapsesAndSentKept`).

## Redis

- [ ] Аренда `outbox:lease` со случайным токеном, `SET NX PX` (`RedisLeaseService`,
   `ItRedisLeaseServiceTest.givenHeldLease_whenAcquireAgain_thenFails`).
- [ ] Освобождение только при совпадении токена - Lua-скрипт (`RedisLeaseServiceTest.givenOwnerToken_whenRelease_thenKeyDeleted`,
   `givenForeignToken_whenRelease_thenKeyKept`; на настоящем Redis - `ItRedisLeaseServiceTest.givenOwnerToken_whenRelease_thenKeyDeleted`,
   `givenStaleToken_whenRelease_thenForeignLeaseKept`).
- [ ] Учет устройств в `devices:published` и отпечатков опубликованных данных в `devices:fingerprint`
   (`RedisPublishedDeviceStore`); в `devices` уходит сообщение, только если устройство новое или отпечаток изменился.
- [ ] `SADD` и запись отпечатка — только после успешной отправки (`OutboxPublishingServiceTest.givenNewDevice_whenPublishBatch_thenPublishMarkAndSent`) -
   `givenKafkaFailure_whenPublishBatch_thenFailedVersionWithIncrementedAttempts` - `markPublished` не вызван).
- [ ] Без аренды публикации нет (`OutboxPublishingServiceTest.givenLeaseNotAcquired_whenPublishBatch_thenNothingPublished`).
- [ ] Аренда проверяется и продлевается после выборки - перед каждой записью, в том числе первой, и перед каждой
   записью версии outbox (`OutboxPublishingServiceTest.givenSeveralRecords_whenPublishBatch_thenLeaseExtendedBeforeEachRecordAndWrite`).
- [ ] Владение сохраняется на время блокирующих вызовов - фоновое продление каждые TTL/3
   (`LeaseGuardTest.givenHeldLease_whenTimePasses_thenHeartbeatExtends`; на настоящем Redis -
   `ItRedisLeaseServiceTest.givenLeaseGuard_whenLongerThanTtl_thenLeaseKept`).
- [ ] При потере аренды обработка останавливается, новая версия outbox и аудит не пишутся
   (`OutboxPublishingServiceTest.givenLeaseLostDuringSelection_whenPublishBatch_thenNothingPublished`,
   `givenLeaseLostMidBatch_whenPublishBatch_thenBatchStops`, `givenLeaseLostDuringSend_whenPublishBatch_thenNoOutboxVersion`,
   `givenLeaseLostAfterKafkaFailure_whenPublishBatch_thenNoFailedVersion`, `givenRedisFailureOnExtend_whenPublishBatch_thenBatchStops`;
   `LeaseGuardTest.givenForeignLease_whenHeartbeat_thenLostAndNoMoreExtends`;
   на настоящем Redis - `ItRedisLeaseServiceTest.givenForeignLease_whenGuardHeartbeat_thenLost`).
- [ ] TTL истёк во время `findPending`, аренду взял другой экземпляр - ничего не отправлено и не записано, аренда
   нового владельца не тронута (`ItEventsCollectorTest.givenLeaseExpiredDuringSelection_whenPublish_thenNothingPublishedOrWritten`).

## Масштабирование

- [ ] Несколько реплик в одной consumer group: `dc up -d --scale events-collector-service=2 events-collector-service`,
   `kafka-consumer-groups.sh --describe --group events-collector` - разделы распределены между репликами.
- [ ] Публикатор single-active: в каждый момент аренду `outbox:lease` держит один экземпляр (аренда - на одну
   итерацию, раздел 11.1); при двух репликах каждая outbox-запись опубликована один раз: число `PUBLISHED`
   в логах равно числу событий, в `devices` нет дублей.

## Наблюдаемость

- [ ] `/actuator/health` и `/actuator/prometheus` отвечают (`check-local.sh` шаг 8, `curl localhost:<порт>/actuator/prometheus`, порт - `make urls`).
- [ ] Прикладные метрики с требуемыми именами (`MetricsFacadeTest.givenAllMetricsUpdated_whenScrape_thenRequiredPrometheusNames`).
   `outbox_candidate_created_total` - в Prometheus после relabel (в `/actuator/prometheus` - `outbox_candidate_total`,
   ограничение Prometheus Java client 1.x, см. README, раздел 16).
- [ ] Логи - JSON с `traceId`, `eventId`, `deviceId`, `topic`, `partition`, `offset` (`dc logs events-collector-service`).
- [ ] Ошибка обработки outbox-записи пишется внутри MDC и трейса события: в «Outbox record not processed» есть
   `eventId`, `deviceId`, `traceId` приема, `status=ERROR`, `errorClass`, `errorMessage`; аудит `ERROR` - в том же контексте
   (`OutboxPublishingServiceTest.givenRedisFailure_whenPublishBatch_thenErrorLoggedAndAuditedInsideEventScope`;
   с настоящей трассировкой - `ItEventsCollectorTest.givenRedisFailure_whenPublish_thenErrorLogLinkedToEventAndIngestionTrace`).
- [ ] Трассировка успешного пути в Tempo - один сквозной трейс: прием (`events process`) и публикация (`outbox publish`,
   дочерний спан) - `docs/screenshots/tempo-trace-end-to-end.png`; контекст переносится через `trace_id`/`span_id`
   в `device_outbox` (`ItEventsCollectorTest.givenNewDevice_whenPublish_thenDeviceInfoSentAndSecondEventSkipped`).
- [ ] Трассировка пути пропуска публикации - в трейсе своего события (`outbox publish` -> `redis devices:published check`
   -> `redis outbox:lease extend` -> `append SENT version`, без `devices send`) - `docs/screenshots/tempo-trace-skip.png`.
- [ ] Дашборд Grafana "IoT Platform - events-collector-service" - `docs/screenshots/grafana-dashboard.png`.

## Тесты

- [ ] Модульные тесты: `svc:test` (`make test`) - зеленые.
- [ ] Интеграционные тесты на Testcontainers: `svc:integrationTest` (`make integration-test`) - зеленые без запущенного стенда.
- [ ] Покрытие строк >= 65%: `svc:check` (`make coverage`, правило `jacocoTestCoverageVerification`), Avro-классы исключены.

## Репозиторий и документация

- [ ] README со всеми разделами задания и таблицей типовых проблем.
- [ ] Диаграммы приёма событий в Mermaid: `component-events-collector.mmd`, `sequence-ingest.mmd`,
   `sequence-outbox-publish.mmd`.
- [ ] `infrastructure/docker-compose.yaml` без поля `version`; healthcheck у kafka, schema-registry, clickhouse, redis,
   events-collector-service; зависимости через `condition: service_healthy` / `service_completed_successfully`.
- [ ] `init-topics.sh`, скрипт схемы ClickHouse (`01-schema.sh`), `.env.example` на месте.
- [ ] Нет секретов в Git: `git ls-files | grep -x .env` - пусто.
- [ ] Нет образов `latest`: `grep -rn ':latest' infrastructure services .env.example` - пусто.
- [ ] Быстрая проверка `services/events-collector-service/scripts/check-local.sh` -> "All checks passed" - напрямую
   из любого каталога, через `make check-local` и `./gradlew :services:events-collector-service:checkLocal`;
   корневой `.env` передается Compose через `--env-file`, симлинк `infrastructure/.env` не нужен.

---

# Реестр устройств (device-collector-service, avros)

device-collector-service, проект avros и изменения в приёме событий. Дополнительная часть задания
(Patroni, PITR, учения) сюда не входит.

## Дизайн

- [ ] Все диаграммы в Mermaid, в `diagrams/`; файлов PlantUML в репозитории нет (`git ls-files '*.puml'` — пусто).
- [ ] Обновлены: контекстная `context.mmd`, контейнерная `containers.mmd`, диаграммы events-collector-service,
  которые затрагивают изменения: `component-events-collector.mmd`, `sequence-ingest.mmd`, `sequence-outbox-publish.mmd`.
- [ ] Созданы для device-collector-service: компонентная `component-device-collector.mmd` и последовательности
  `dcs-success-sequence.mmd`, `dcs-duplicate-sequence.mmd`, `dcs-error-sequence.mmd`.
- [ ] Дизайн и этот DoD приняты преподавателем до начала работы над кодом.

## Проект avros

- [ ] `avros/` — отдельный проект монорепозитория со своими `build.gradle.kts` и `Dockerfile`.
- [ ] Все Avro-схемы платформы лежат только в `avros/`: `git ls-files '*.avsc'` выводит только пути `avros/…`;
  в events-collector-service локальных `.avsc` нет.
- [ ] Сборка генерирует Java-классы и публикует версионированный артефакт в Nexus (задача compose `avros-publish`,
  `avros/scripts/container-publish.sh`, `make avros-up`). Выпущенная версия не перезаписывается: публикация той же
  версии с другими схемами завершается ошибкой, с теми же схемами - ничего не меняет (код 0).
- [ ] Совместимость проверяется в два этапа. До публикации `avros-publish` сверяет схемы с последней выпущенной
  версией в Nexus в режиме FULL (`checkAvrosCompatibility`, `make avros-check`). При регистрации `schema-register`
  (`avros/scripts/container-register.sh`) субъектам задаётся режим `FULL_TRANSITIVE`, и Schema Registry проверяет схему
  на совместимость со **всеми** зарегистрированными версиями. Несовместимое изменение (например, новое поле без
  значения по умолчанию) останавливает конвейер с кодом ≠ 0 - проверено на временной ветке.
- [ ] После публикации в Nexus конвейер регистрирует схемы в Schema Registry; у субъектов `events-value`,
  `events-dlt-value`, `devices-value`, `devices-dlt-value` режим `FULL_TRANSITIVE`
  (`curl localhost:8081/config/<субъект>`); повторный запуск `make schemas-register` ничего не меняет.
- [ ] Оба сервиса подключают avros из Nexus с явной версией (`avrosVersion = "1.2.0"`): в `build.gradle.kts` нет
  `+`, `latest`, диапазонов.
- [ ] Продюсеры не регистрируют схемы сами: `auto.register.schemas=false`. Отправка с незарегистрированной
  схемой завершается ошибкой, а не новой версией в Schema Registry.

## Изменения в приёме событий

Пункты секции «Приём событий» выше уже исправлены под эти изменения: avros, `events-dlt`, отпечаток.


- [ ] `DeviceEvent` расширен блоком данных устройства: минимум имя, операционная система и её версия, версия
  прошивки. Существующие поля не удалены и не переименованы; новые поля необязательные, со значением по умолчанию.
- [ ] `DeviceInfo` несёт те же данные устройства.
- [ ] Совместимость доказана тестом: сообщение предыдущей версии читается новой схемой и наоборот
  (`ContractCompatibilityTest` в обоих сервисах).
- [ ] В таблицу событий ClickHouse новые колонки добавлены обратно совместимой миграцией
  (`infrastructure/init/clickhouse/03-device-snapshot.sh`, `04-device-profile.sh`: `ADD COLUMN IF NOT EXISTS`,
  значения по умолчанию); задача `clickhouse-init` выполняет их при каждом запуске стенда, поэтому повторный запуск на
  существующем томе проходит без ошибок (`make up` -> `clickhouse-init` `Exited (0)`), старые строки читаются.
- [ ] Дедупликация по набору полей устройства. Отпечаток — SHA-256 данных устройства без полей, меняющихся
  с телеметрией (`lastSeenAtMs`, показания сенсоров); состав полей описан в дизайне. В `devices` уходит сообщение,
  только если устройство новое или отпечаток изменился. Новый отпечаток сохраняется в Redis только после
  подтверждения Kafka. Проверки:
  - unit (`DeviceFingerprintTest`, `OutboxPublishingServiceTest`): те же данные — публикации нет; изменились имя, ОС
    или прошивка — `UPDATED`; новое устройство — `CREATED`; сбой отправки — отпечаток не изменился, следующая попытка
    публикует;
  - стенд: два события с одинаковыми данными устройства дают одно сообщение в `devices` (`check-local.sh`, шаг 10:
    исход аудита `SKIPPED`); отпечатки - Redis-хеш `devices:fingerprint`.
- [ ] Топик `events-dlt` создаётся автоматически (`init-topics.sh`, `KAFKA_EVENTS_DLT_TOPIC`; вместо `events.DLT`).
- [ ] Невалидное сообщение сразу уходит в `events-dlt`. Сообщение, не записанное в ClickHouse за заданное число
  попыток, уходит в `events-dlt`, после чего смещение подтверждается (lag группы — 0). Повторов без ограничения нет
  (кроме недоступного Schema Registry - сбой инфраструктуры, а не сообщения). Тесты: `KafkaConfigTest`,
  `ItKafkaErrorHandlingTest.givenStorageFailure_whenConsume_thenRetriedMaxAttemptsThenMovedToDlt`,
  `givenInvalidEvent_whenConsume_thenMovedToDltAndOffsetCommitted`.
- [ ] Число попыток и паузы задаются переменными окружения `EVENTS_MAX_ATTEMPTS`, `EVENTS_RETRY_INTERVAL` и перечислены
  в `.env.example`.

## device-collector-service

- [ ] Читает `devices` по одному сообщению (`max-poll-records: 1`, `enable-auto-commit: false`, `ack-mode:
  manual_immediate` в `application.yaml`); смещение подтверждается только после `COMMIT` в PostgreSQL
  (`DeviceInfoListenerTest`, `DeviceRegistryServiceTest`).
- [ ] Работает только с ShardingSphere-Proxy: один datasource — логическая база `devices` (`DEVICES_DB_URL` ->
  `shardingsphere-proxy:3307`); адресов шардов в конфигурации сервиса нет (`grep -rn "postgres-shard"
  services/device-collector-service/src/main` — пусто).
- [ ] Идемпотентность: повторное сообщение не меняет запись (`seen_count` не растёт); более старое сообщение
  не перезаписывает более новое. Подтверждается IT: `ItDeviceCollectorTest.givenSameMessageTwice_whenConsumed_thenSeenCountOne`,
  `givenOlderEventAfterNewer_whenConsumed_thenStale`.
- [ ] Невалидные сообщения сразу уходят в `devices-dlt`; сообщения, не записанные за заданное число попыток
  (`DEVICES_MAX_ATTEMPTS`), — в `devices-dlt`; обработка следующих сообщений продолжается
  (`ItDeviceCollectorTest.givenInvalidMessage_whenConsumed_thenDeadLetteredAndNextProcessed`,
  `givenUndeserializableMessage_whenConsumed_thenDeadLetteredAsIs`; `KafkaConfigTest`).
- [ ] Все параметры, включая тайм-ауты (подключение, запрос, блокировка), число попыток, паузы и их рост,
  задаются переменными окружения: в `application*.yaml` нет значений без `${…}`, все переменные есть
  в `.env.example`.
- [ ] Миграций в сервисе нет: в `services/device-collector-service` нет Flyway и DDL
  (`grep -rniE "flyway|create table|alter table" services/device-collector-service/src/main` — пусто).

## База данных

- [ ] Два шарда PostgreSQL 18 (`postgres-shard-0`, `postgres-shard-1`); стратегия по хешу, ключ `device_id`; запрос по
  `device_id` уходит на один шард (`make shard-of ID=<deviceId>` — `PREVIEW` в Proxy; журнал Proxy: `Actual SQL: shard_N`).
- [ ] ShardingSphere-Proxy — отдельный контейнер. Конфигурация лежит в репозитории
  (`infrastructure/config/shardingsphere/global.yaml`, `database-devices.yaml`), пароли подставляются из переменных
  окружения скриптом `render-config.sh`: в репозитории только шаблоны, поиск паролей по `git grep` ничего не находит.
- [ ] У каждого шарда есть реплика только для чтения с потоковой репликацией (`postgres-shard-N-replica`; роль
  `replicator`, слот `replica_N`; `infrastructure/init/shards/`, `config/postgres/pg_hba.conf`). `make replication-status`
  завершается кодом 0 и проверяет:
  - на primary `pg_stat_replication` показывает реплику в состоянии `streaming`, слот `replica_N` активен;
  - на реплике `pg_is_in_recovery()` = `true`, `SHOW transaction_read_only` = `on`, запись отклоняется;
  - строка, записанная через Proxy, появляется на реплике своего шарда (число строк `devices` на шарде и его реплике
    совпадает).
- [ ] Чтение на реплики не направляется: в конфигурации Proxy реплик нет.
- [ ] Для каждого шарда есть резервная копия: `make shard-backup` (`infrastructure/config/postgres/shard-backup.sh`)
  создаёт отдельную копию на шард (`shard_0.dump`, `shard_1.dump`) в `backups/<время>/` с `manifest.txt` (число строк,
  версия схемы, SHA-256, offset группы `device-collector`); копия снимается с реплики, сервисы не останавливаются.
  Восстановление - `make shard-restore CONFIRM=yes` (удаляет данные шардов; без `CONFIRM=yes` отказ); после него
  устройства из manifest читаются через Proxy, реплики снова `streaming`.
- [ ] Миграции выполняет только `db-migrator` на Flyway (`services/db-migrator`, `db/migration/devices`). Миграции
  обратно совместимы и написаны с `IF NOT EXISTS`. Мигратор применяет схему напрямую к primary каждого шарда по очереди
  (не через Proxy) и затем сверяет версии; способ описан в дизайне и в README, раздел 20.2. Повторный запуск применяет 0
  миграций (`applied 0 migration(s)`); расхождение версий даёт код завершения ≠ 0 (`DbMigrator`: `schema versions differ between shards`).

## Запуск

- [ ] Порядок старта в Docker Compose: Nexus → сборка и публикация avros → регистрация схем → `db-migrator` →
  сервисы. Каждый шаг ждёт успешного завершения предыдущего (`service_completed_successfully` или
  `service_healthy`), а не запуска контейнера. Проверки:
  - `make up` на чистом стенде (после `make clean`) завершается успешно; одноразовые задачи — `Exited (0)`;
  - если регистрация схем завершилась ошибкой, `db-migrator` и сервисы не стартуют;
  - цепочка в `docker-compose.yaml`: `nexus` -> `nexus-init` -> `avros-publish` -> `schema-register` -> `db-migrator` ->
    `shardingsphere-proxy` -> `device-collector-service` (`docker compose config` показывает `depends_on`);
    `events-collector-service` ждёт `schema-register`, `kafka-init`, `clickhouse-init`.
- [ ] У всех образов явный тег, не `latest`.
- [ ] Секретов в репозитории нет; `.env.example` содержит все переменные с безопасными значениями-заглушками.

## Наблюдаемость

- [ ] Метрики со всех компонентов: оба сервиса (`/actuator/prometheus`, jobs `events-collector-service`,
  `device-collector-service`), ShardingSphere-Proxy (агент метрик из образа 5.5.3, `PROXY_AGENT_ENABLED=true`, job
  `shardingsphere-proxy`), шарды PostgreSQL и их реплики (`postgres-exporter` v0.20.1 на каждый, job `postgres-shards`;
  отставание реплики - `pg_replication_lag_seconds`, `pg_stat_replication_pg_wal_lsn_diff`), Kafka (job `kafka`).
  Все цели профиля `observability` в Prometheus (`/targets`) — `UP`; `minio`, `keycloak`, `postgres` относятся к профилю
  `platform`.
- [ ] Дашборды в Grafana: «events-collector-service», «device-collector-service», «Proxy и шарды реестра устройств»
  (`device-shards`), «Kafka» — панели получают данные (все запросы дашбордов возвращают ряды при нагрузке).
- [ ] Логи всех компонентов в Loki: по каждому контейнеру стенда находятся строки (`{container="iot-…"}`); в логах
  device-collector-service есть метка `shard`.
- [ ] Один traceId проходит от события в `events` до записи в PostgreSQL через оба сервиса: в Tempo одна трасса
  со спанами events-collector-service (приём, публикация outbox, `devices send`) и device-collector-service
  (`devices process`, `transaction`, `insert devices`) - `docs/screenshots/tempo-trace-events-to-postgres.png`.

## Тесты реестра устройств и avros

- [ ] Unit-тесты логики обоих сервисов (`make test`, `make dcs-test`; events-collector-service — 121 тест,
  device-collector-service — 93, db-migrator — 2); порог покрытия JaCoCo выполняется (65 % и 70 %, `make coverage`,
  `make dcs-coverage`), сгенерированные Avro-классы из отчёта исключены.
- [ ] Интеграционные тесты обоих сервисов на Testcontainers (`make integration-test` — 23, `make dcs-it` — 10).
- [ ] Шардирование: поднимаются два контейнера PostgreSQL и ShardingSphere-Proxy. После сохранения через Proxy
  тест проверяет напрямую в шардах, что запись есть на ожидаемом шарде и её нет на другом
  (`ItDeviceCollectorTest.givenDeviceInfo_whenConsumed_thenStoredOnItsShard`,
  `givenManyDevices_whenConsumed_thenSpreadOverShards`).
- [ ] Тесты отправки в DLT для обоих сервисов: невалидное сообщение и исчерпание попыток (events-collector-service —
  `ItKafkaErrorHandlingTest`, `KafkaConfigTest`; device-collector-service — `ItDeviceCollectorTest`, `KafkaConfigTest`).
- [ ] `./gradlew check` проходит.

## Что сдаётся

- [ ] Ссылка на репозиторий.
- [ ] Диаграммы и этот Definition of Done.
- [ ] Снимки экрана Grafana в `docs/screenshots/`: панели с метриками (оба сервиса, Proxy, шарды, Kafka) —
  `grafana-dashboard.png` (events-collector-service), `grafana-device-collector.png`, `grafana-proxy-shards.png`,
  `grafana-kafka.png` — и одна сквозная трасса `tempo-trace-events-to-postgres.png` (события -> PostgreSQL через оба
  сервиса).
- [ ] README: порядок запуска (раздел 10) и способ проверить, на какой шард попал `deviceId` (раздел 21).
