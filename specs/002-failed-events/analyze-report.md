# Specification Analysis Report — 002-failed-events

Сверка `constitution.md`, `spec.md`, `plan.md`, `tasks.md`, `data-model.md`, `research.md`, `quickstart.md`, контракта, ADR 0013–0019 и
диаграмм (`/speckit-analyze`), 2026-11-21. Механическая часть (покрытие FR и SC задачами, ссылки, переменные окружения, пути, ссылки `Rn ↔ T###`) —
скриптом согласованности вне репозитория и чтением по шагам analyze: 25 FR и 5 SC, 83 задачи, покрытие 30 из 30; битые ссылки: 2 на `../../AGENTS.md` (файл в корне репозитория, в зеркале ветки его нет), остальные находки скрипта — ложные (коды ошибок принял за переменные окружения). Отчёт — по состоянию после исправлений;
файлы анализ не меняет, исправления внесены отдельно и перечислены в колонке «Статус».

| ID | Category | Severity | Location(s) | Summary | Статус |
|----|----------|----------|-------------|---------|--------|
| C1 | Coverage | HIGH | spec.md, tasks.md | FR-002, SC-001, SC-002, SC-005 не имели задач | Исправлено: T010, T021 (FR-002), T070 (SC-001), T037 (SC-002), T008 (SC-005) |
| I1 | Inconsistency | HIGH | spec.md (FR-023), задание п. 9 | Задание требует, чтобы ошибка каждого из двух сервисов «доходила до объекта» в IT; в прежнем дизайне IT сборщиков останавливались на `failed-events` | Исправлено: FR-023 по букве задания, IT каждого сборщика доходит до объекта (R23, первая IT-задача P3); сквозная проверка на стенде — `make fep-quick-check` (T048, T069); порядок запуска и поиск объекта будут описаны в README (T073) |
| I2 | Inconsistency | MEDIUM | прототип java, contracts | Схема прототипа не содержит `deviceId`; в контракте поле добавлено | Открыто: R9, закрывает `make avros-check` (T003) |
| I3 | Inconsistency | MEDIUM | прежний DoD, README, ADR-0002, spec 001 (FR-015 строка 11) | Прежние тексты называют `devices-dlt`, Prometheus, MinIO в профиле `platform` | Принято: правятся вместе с кодом (T073, T074), в дизайн-ветке — секция-ссылка в DoD со списком изменений |
| I4 | Inconsistency | LOW | decisions (рабочий файл), прототип infra | Срок `mimir-blocks`: прототип 400, решение 380 | Исправлено: 380 везде (spec FR-016, plan, data-model, ADR-0015) |
| U1 | Underspecification | MEDIUM | spec.md | Режим и срок Object Lock не заданы | Исправлено: lock включён при создании бакета (T009), режим GOVERNANCE и `FAILED_EVENTS_LOCK_DAYS` — необязательная фаза (T081), ILM = срок блокировки, 180 суток (R21) |
| U2 | Underspecification | MEDIUM | plan.md | Связь `FAILED_EVENTS_PUBLISH_TIMEOUT` с `FAILED_EVENTS_DELIVERY_TIMEOUT_MS` и `FAILED_EVENTS_S3_API_CALL_TIMEOUT` с `FAILED_EVENTS_MAX_POLL_INTERVAL_MS` не записана | Исправлено: ограничения в plan.md, «Переменные окружения» |
| A1 | Ambiguity | LOW | spec.md | «Новый код ошибки — сначала процессор» без правила выкладки | Исправлено: правило в data-model.md и ADR-0013 |
| C2 | Constitution | MEDIUM | constitution.md, plan.md | В «Технологиях» названы Prometheus и нет MinIO/Mimir | Открыто: T075 (версия 1.2.0 отдельным коммитом) |
| T1 | Terminology | LOW | diagrams | Device Gateway на контейнерной остался без маршрута ошибок после удаления `iot.dlt` | Принято: шлюз не реализован, вне рамок |
| R1 | Risk | MEDIUM | research.md | Предположения не проверены: R9 (`deviceId`), R18 (возврат смещений при restore, `flock`), R20 (скорость процессора), R21, R22 (arm64), R23 (второй контекст процессора в IT) | Открыто: у каждого есть проверяющая задача до зависящего кода и запасной вариант |
| R2 | Risk | MEDIUM | ADR-0018 | При `403` на запись процессор повторяет и блокирует раздел (ошибка настройки учётки) | Принято: видно по `failed_events_archive_errors_total{reason="s3_denied"}`; пользы в пропуске нет — потеря сообщения хуже остановки |
| X1 | Irreversible | HIGH | tasks.md T006, T004 | Удаление тем DLT и публикация avros в основной Nexus необратимы | Исправлено: T004, T078 — **ЧЕЛОВЕК**; T006 проверяется только на своём проекте compose |

**Coverage Summary Table:** все FR-001…FR-025 и SC-001…SC-005 имеют задачи (карта — вывод скрипта; FR-024, FR-025 — только необязательная фаза P9).

**Unmapped tasks:** T074, T075, T077, T078, T083 — документы, снимки и converge, требований FR не закрывают.

**Metrics:** требований 25 FR + 5 SC; задач 83 (из них **ЧЕЛОВЕК** 3: T004, T077, T078); критических находок 0; открытых HIGH 0, открытых MEDIUM 3 (I2, C2, R1).

## Проход после ревью PR #6 (2026-11-22)

Четыре замечания ревью (proselytear) к диаграммам; сверка с кодом ветки реализации показала, что верно первое, остальные три — расхождения диаграмм.

| ID | Category | Severity | Location(s) | Summary | Статус |
|----|----------|----------|-------------|---------|--------|
| P1 | Inconsistency | HIGH | ADR-0014, ADR-0018, spec.md (FR-007, FR-008, FR-010), data-model.md, plan.md, `fep-error-to-minio-sequence.mmd`, `component-failed-events-processor.mmd` | У нечитаемой и разобранной записи разные ключи: сбой между записью `unreadable` и подтверждением смещения после восстановления схемы даёт два объекта | Исправлено: общий ключ `<yyyy>/<MM>/<dd>/failed-events-<partition>-<offset>.json` из координат и времени записи `failed-events`; цена (потеря префикса сервис/код, второй объект при повторной публикации сборщиком) записана в ADR-0014 |
| P2 | Inconsistency | MEDIUM | `fep-error-to-minio-sequence.mmd`, `sequence-ingest.mmd` | Смещение `events` подтверждалось после `device_events` без `device_outbox` | Исправлено: обе записи — один шаг, ack после обеих, ошибка любой ведёт к повторам и `FailedEvent` |
| P3 | Inconsistency | MEDIUM | `sequence-ingest.mmd`, `dcs-error-sequence.mmd` | В `FailedEvent` то `DeviceEvent`/`DeviceInfo` (Avro), то исходные байты | Исправлено: везде исходные байты из заголовков `x-raw-key`, `x-raw-value`, добавлен шаг их сохранения до разбора |
| P4 | Inconsistency | MEDIUM | `dcs-operations-sequence.mmd` | `DeviceRegistryService` сам публикует `FailedEvent`, в других схемах — `DefaultErrorHandler` | Исправлено: сервис бросает исключение, публикует обработчик, ack после ответа Kafka, добавлена ветка неудачной публикации |

## Проход после реализации (2026-11-23, T076)

Сверка `spec.md` (FR-001…FR-023; FR-024 и FR-025 — необязательная фаза P9, не реализованы), `plan.md`, `tasks.md`, ADR 0013–0019,
диаграмм и ветки реализации `fifth-module` с кодом, тестами и стендом. Файлы анализ не меняет; найденное исправлено отдельно
и перечислено в колонке «Статус», документы по расхождениям без исправления — в «Открыто».

**Основания** (повторяемые): `./gradlew :services:{events-collector-service,device-collector-service,failed-events-processor}:check` —
`BUILD SUCCESSFUL`, 0 падений: unit 148 / 157 / 48, интеграционные 38 / 29 / 8, покрытие строк 97,6 % / 96,4 % / 96,6 % (числа из
`build/test-results` и отчёта JaCoCo); стенд `iot-p8` (macOS, arm64, свой проект Compose): `make check`, `check-local`,
`dcs-quick-check`, `ds-quick-check`, `fep-quick-check`, `minio-check` (92 из 92), `obs-check`, `backup`; `git grep` и `grep` по репозиторию.

### Требования

| FR | Результат | Чем подтверждено | Что не подтверждено в этом проходе |
|---|---|---|---|
| FR-001 | выполнено | `failed-event.avsc`: все поля, `ErrorCode` с `default UNKNOWN`; avros 1.4.0 у четырёх сервисов; `FailedEventContractTest` | `make avros-check` не запускался: Nexus на стенде перезапускается (R-риск ниже) |
| FR-002 | выполнено | в `services/*/src/main` нет строковых литералов кодов ошибок (`grep`); соответствие «исключение → код» — `FailedEventRecovererTest` обоих сборщиков | — |
| FR-003 | выполнено с замечанием D1 | `init-topics.sh`: 3 раздела, `delete`, `retention.ms` 14 суток, без `retention.bytes`; переменных `*_DLT_*` нет | `git grep -i dlt` вне `specs/` и `docs/adr/` не пуст (D1) |
| FR-004 | выполнено | `FailedEventRecoverer` публикует синхронно, смещение — после ответа брокера; unit и IT (`ItKafkaErrorHandlingTest`, `ItDeviceCollectorTest`, `ItFailedEventToObjectTest`); диаграмма `sequence-ingest` показывает ветку неудачной публикации | — |
| FR-005 | выполнено | исходные байты из `x-raw-key`, `x-raw-value` (`RawKeepingDeserializer`); `traceId` из спана или `traceparent`; `fep-quick-check`: `base64(value)` равно отправленному | — |
| FR-006 | выполнено | метрика `failed.events.published{error_code,error_type}` и `status=FAILED_EVENT` в обоих сборщиках; в `device-service` нет обращений к `failed-events` | — |
| FR-007 | выполнено | единый ключ `ArchiveKey`; метаданные `error-code`, `source-service`, `source-topic`, `trace-id`, `device-id`; `ItFailedEventsProcessorTest`; `fep-quick-check`: три объекта | — |
| FR-008 | выполнено | `If-None-Match: *`, 412 → `duplicate`; `ItFailedEventsProcessorTest` (повтор той же записи — один объект, одна версия), `ItFailedEventsRestartTest`; ограничение повторной публикации сборщиком — в ADR-0014 и README 23.2 | — |
| FR-009 | выполнено | `ItFailedEventsOutageTest`: MinIO на паузе дольше `max.poll.interval` — группа стабильна, объект появляется после снятия паузы | сценарий 7 quickstart (`docker pause` на стенде) в этом проходе не повторялся |
| FR-010 | выполнено | `ItFailedEventsProcessorTest` (мусор и невалидный UTF-8 в ключе), `ItFailedEventsOutageTest` (Schema Registry на паузе: нечитаемых нет, после снятия — штатная разборка) | — |
| FR-011 | выполнено после исправления N1, N2 | на стенде одна трасса из пяти спанов (`events process`, `validate`, `failed-events send`, `failed-events process`, `s3.put-object`), `traceId` `2727c49060e6eaf27e87d2093a7ecc16`; `ItFailedEventsTraceTest`; снимок `docs/screenshots/tempo-trace-failed-events.png` | — |
| FR-012 | выполнено | измерено в T070 (`research.md`, «Результаты проверок»); на стенде всплески 3 × 200 нечитаемых сообщений обработаны, основная тема не стояла | панель отставания при остановленном Proxy в этом проходе не снималась |
| FR-013 | выполнено | README 23.4: `mc ls`, `mc find --metadata` по коду, сервису, `traceId`, `deviceId` | — |
| FR-014 | выполнено | `minio-check` 92 из 92; бакеты и Object Lock создаёт `minio-init` | — |
| FR-015 | выполнено | `minio-check`: свой бакет — 200, чужой бакет и префикс — 403 по каждой учётке | — |
| FR-016 | выполнено | `minio-check`: ILM по бакетам равен `.env.example` и больше срока Mimir, Loki, Tempo | — |
| FR-017 | выполнено | сервиса, тома и переменных Prometheus нет (`grep`); `obs-check`: запросы дашбордов — 200, `up{job}` в Mimir | — |
| FR-018 | выполнено | на стенде после нагрузки: Mimir 7, Loki 152, Tempo 81 объект в своих бакетах | перезапуск компонентов в этом проходе не проверялся |
| FR-019 | выполнено | дашборды `failed-events-processor` и «MinIO» в `infrastructure/config/grafana/dashboards/`; `obs-check`: все запросы — 200; снимки панелей в `docs/screenshots/` | — |
| FR-020 | выполнено частично | `make backup`: копии PostgreSQL и ClickHouse с manifest в `backups/postgres/`, `backups/clickhouse/` | срабатывание по расписанию `BACKUP_SCHEDULE` не наблюдалось |
| FR-021 | **не подтверждено** | порядок шагов описан (README 23.6, quickstart); ранее прототипом проверены `pg_restore` и `RESTORE DATABASE` по компонентам (R17, R18) | цикл «копия → удаление → восстановление» (`make backup-check CONFIRM=yes`) в этом проходе не выполнялся: удаляет данные; остаётся для T078 |
| FR-022 | выполнено частично | Dockerfile в два этапа (JDK → JRE), без `latest` и `.env` в Git; `make check` проходит | `make up` целиком на чистом стенде не доходил до конца: `avros-up` падал с `HTTP 000`, Nexus перезапускался (N6); сервисы поднимались `up --no-deps` |
| FR-023 | выполнено | `check` трёх сервисов зелёный (см. «Основания»); IT доводят ошибку до объекта (`ItFailedEventToObjectTest`), нечитаемые байты, повторная доставка, недоступный MinIO; `fep-quick-check` на стенде | — |

SC-001…SC-005 — по тем же основаниям, что FR-007, FR-009, FR-011, FR-012, FR-014…FR-016, FR-023.

### Находки прохода

| ID | Category | Severity | Location(s) | Summary | Статус |
|----|----------|----------|-------------|---------|--------|
| N1 | Defect | HIGH | `events-collector-service` `FailedEventRecovererConfig` | Продюсер `failed-events` создан вручную (`new KafkaTemplate`), реестра наблюдений у него нет: спан отправки не создавался, `traceparent` не попадал в запись, трасса процессора шла отдельно от трассы события (FR-011) | Исправлено: `setObservationEnabled(true)`, `setObservationRegistry(...)`, тест `FailedEventRecovererConfigTest` (был красным); на стенде одна трасса из пяти спанов |
| N2 | Defect | MEDIUM | `failed-events-processor.yaml` | Ключ `management.otlp.tracing.endpoint` Spring Boot 4 не читает: трассы процессора уходили на `localhost:4318`; не задана выборка (по умолчанию 10 %) | Исправлено: `management.opentelemetry.tracing.export.otlp.endpoint`, `sampling.probability: 1.0` |
| N3 | Inconsistency | MEDIUM | ADR-0014, spec FR-007 и строка 24, data-model.md, `fep-error-to-minio-sequence.mmd`, `ArchiveService` | Перечень метаданных объекта расходился: `source-topic` — в spec FR-007, data-model и коде, но не в ADR-0014 и spec (строка 24); у нечитаемой записи в диаграмме пропал `x-amz-meta-kind` | Исправлено документами (`a64c332`, `94fe2c0`); код не менялся |
| N4 | Inconsistency | MEDIUM | spec.md, Independent Test | Пример ключа объекта в прежней схеме (`events-collector-service/…/VALIDATION_FAILED/…`) | Исправлено (`a64c332`) |
| N5 | Inconsistency | MEDIUM | `sequence-ingest.mmd` | Отказ публикации `FailedEvent` описан одной общей заметкой, а не веткой у каждой из четырёх публикаций (замечание 4 ревью PR #6 к `dcs-operations-sequence`) | Исправлено (`1ddaaec`) |
| N6 | Risk | MEDIUM | стенд, `iot-nexus` | Nexus перезапускается (счётчик дошёл до 8), `make up` падал на `avros-up` с `HTTP 000`; причина не установлена | Открыто: приёмка T078 — на стенде с памятью Docker не меньше 7 ГБ (README 10); при повторе — `docker logs iot-nexus` |
| N7 | Risk | LOW | стенд Docker Desktop | При 5,8 ГБ памяти Docker и запущенном Nexus проверки стенда (`check-local`, `ds-quick-check`, `backup`) падали по тайм-аутам; после остановки Nexus проходили | Принято: README 10 называет ≥ 7 ГБ с профилями |
| D1 | Documentation | LOW | spec.md FR-003 (*Проверка*) | `git grep -i -E 'dlt|dead.?letter'` вне `specs/` и `docs/adr/` не пуст: `README.md` и `docs/definition-of-done.md` описывают удаление DLT, `infrastructure/init/kafka/init-topics.sh` удаляет прежние темы (требует T006) | Открыто: документ не менялся. Предложение — исключить из проверки `README.md`, `docs/definition-of-done.md` и `init-topics.sh` или сузить её до `services/`, `avros/`, `diagrams/`, `infrastructure/config/` |
| D3 | Documentation | LOW | `docs/adr/0002-device-info-operation.md` (строка 18), `specs/001-device-service/{spec.md (FR-015, строка 11), plan.md, tasks.md}` | Документы предыдущей фичи по-прежнему называют `devices-dlt`; DoD (раздел «Обработка ошибок и архив») оговаривает замену | Открыто: документы не менялись. Предложение — заменить `devices-dlt` на `failed-events` в этих четырёх местах |
| D2 | Documentation | LOW | README 15, `docs/definition-of-done.md` | Числа тестов device-service не пересчитаны (README — 100 unit, 49 IT; DoD — 100 и 44); фича их не меняла | Открыто: вне `specs/002-failed-events`, пересчитать при следующем `check` device-service |

Закрыты находки первого прохода: I2 (`deviceId` есть в схеме и тестах контракта), C2 (T075: конституция 1.2.0, `AGENTS.md`); I3 закрыта частично (T073, T074:
README и DoD без DLT и Prometheus, MinIO описан в базовом стенде), остаток — D3. R1: R20, R22 (arm64), R23 (второй контекст процессора в IT)
подтверждены, R21 — Object Lock включён при создании бакета (`minio-check`), режим GOVERNANCE — фаза P9; R18 (возврат смещений при
восстановлении) — см. FR-021.

**Coverage Summary:** FR-001…FR-023 имеют задачи и реализацию; кодом и тестами подтверждены все 23. Стендом подтверждены FR-005, FR-007, FR-011, FR-012,
FR-014…FR-019 и FR-023 (`fep-quick-check`); не подтверждены полностью — FR-021 (цикл восстановления) и частично FR-001 (`make avros-check`),
FR-020 (расписание), FR-022 (`make up` на чистом стенде).

**Metrics:** критических находок 0; открытых HIGH 0; открытых MEDIUM 1 (N6); LOW 4 (N7, D1, D2, D3).

**Next Actions:** T077 (снимки — приложены в `docs/screenshots/`) и T078 (приёмка на стенде владельца: `make up`, `make backup-check CONFIRM=yes` — закрывает FR-021 и
FR-022) выполняет человек; D1 решает владелец документа; P9 (T079–T082) — после решения преподавателя (ADR-0019).
