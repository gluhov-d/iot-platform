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
