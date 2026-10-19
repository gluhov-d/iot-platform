# Definition of Done - модуль 2, events-collector-service

Отметка ставится, когда пункт подтвержден указанной проверкой.
Пути - от корня репозитория. `dc` = `docker compose -f infrastructure/docker-compose.yaml`
`svc` = `./gradlew :services:events-collector-service`.

## Контракты и Kafka

- [ ] `contracts/avro/device-event.avsc` и `contracts/avro/device-info.avsc` есть; поля и типы - как в схемах задания
  (namespace - `com.github.gluhov.iotplatform.events.avro`, см. README, раздел 4).
- [ ] Входная тема `events`, выходная `devices` (`.env.example`: `KAFKA_EVENTS_TOPIC`, `KAFKA_DEVICES_TOPIC`).
- [ ] Ключ обеих тем - `deviceId`: `DeviceEvent` отправляется с ключом `deviceId` (`check-local.sh`, `ItEventsCollectorTest`),
        `DeviceInfo` - `KafkaDeviceInfoProducer` (`KafkaDeviceInfoProducerTest.givenDevice_whenPublish_thenSendKeyedByDeviceId`).
- [ ] `events` - `cleanup.policy=delete`, `devices` - `cleanup.policy=compact`, `events.DLT` - `cleanup.policy=delete`: `make topics`.
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
   невалидные сообщения откладываются в `events.DLT`, их offset подтверждается только после записи в DLT
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
- [ ] Учет устройств в `devices:published` (`RedisPublishedDeviceStore`).
- [ ] `SADD` только после успешной отправки (`OutboxPublishingServiceTest.givenNewDevice_whenPublishBatch_thenPublishMarkAndSent`) -
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

- [ ] `/actuator/health` и `/actuator/prometheus` отвечают (`check-local.sh` шаг 7, `curl localhost:<порт>/actuator/prometheus`, порт - `make urls`).
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
- [ ] Три Mermaid-диаграммы в `diagrams/`.
- [ ] `infrastructure/docker-compose.yaml` без поля `version`; healthcheck у kafka, schema-registry, clickhouse, redis,
   events-collector-service; зависимости через `condition: service_healthy` / `service_completed_successfully`.
- [ ] `init-topics.sh`, скрипт схемы ClickHouse (`01-schema.sh`), `.env.example` на месте.
- [ ] Нет секретов в Git: `git ls-files | grep -x .env` - пусто.
- [ ] Нет образов `latest`: `grep -rn ':latest' infrastructure services .env.example` - пусто.
- [ ] Быстрая проверка `services/events-collector-service/scripts/check-local.sh` -> "All checks passed".