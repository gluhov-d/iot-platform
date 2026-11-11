# Specification Analysis Report — 001-device-service

Проверка согласованности `constitution.md`, `spec.md`, `plan.md`, `tasks.md`, `data-model.md`, `research.md`,
контракта, ADR и диаграмм (`/speckit.analyze`, Spec Kit 1.4.0). Механическая часть — скрипт: покрытие требований
задачами, ссылки между файлами, имена тестов и переменных окружения, устаревшие термины. Отчёт — после исправлений;
в колонке «Статус» — что сделано.

| ID | Category | Severity | Location(s) | Summary | Статус |
|----|----------|----------|-------------|---------|--------|
| A1 | Inconsistency | HIGH | research.md, ADR-0009 | Пример теории: `spring-http-interface` + `useBeanValidation`. Генератор 7.17.0 отключает эту опцию — DTO без ограничений, FR-011 и 400 на невалидное тело не выполнились бы | Исправлено: два прогона генератора (модели `spring`, интерфейс `spring-http-interface`), проверено `openapi-generator-cli` |
| A2 | Inconsistency | HIGH | contracts/device-service.yaml | `pattern: '\S'`: в OpenAPI — поиск в строке, в `@Pattern` — совпадение всей строки; принимались бы только односимвольные `tenantId`, `deviceType` | Исправлено: `'^.*\S.*$'`; контракт валиден |
| A3 | Ambiguity | MEDIUM | data-model.md | `deleted_by` у строк, удалённых роутером до V3, — `NULL`; правило «удалено администратором» для них не определено | Исправлено: `NULL` = `DEVICE` |
| A4 | Underspecification | MEDIUM | tasks.md T020 | device-collector-service получает новую зависимость — Redis; переменные и Redis в его IT не были названы | Исправлено: в T020 |
| A5 | Underspecification | MEDIUM | ADR-0008 | `volatile-lru` может вытеснить аренду `outbox:lease`; безопасность этого — предположение | В spike: research #11 (T005), запасной вариант — без `maxmemory` |
| C1 | Constitution | MEDIUM | plan.md (VII), SC-001, T037 | Полный путь PUT → GET на Testcontainers зависит от research #8 | Открыто до spike; запасной вариант описан |
| C2 | Constitution | LOW | plan.md Complexity Tracking | `floor` + Lua, вторая база Proxy, Keycloak в базовом стенде — отступления от простейшего варианта | Обоснованы в Complexity Tracking и ADR |
| D1 | Duplication | LOW | spec.md (таблица правил), ADR-0005, dcs-operations-sequence.mmd | Правила применения команд в трёх местах | Оставлено: таблица — критерий приёмки, ADR — причина, диаграмма — поток; формулировки сверены |
| U1 | Underspecification | LOW | ADR-0007 | Срок жизни access token (5 мин) не входит в DoD | Принято: настройка realm, проверяется при T018 |
| T1 | Terminology | LOW | диаграммы | `DeviceApi` в черновике против `DevicesApi`, который даёт генератор (тег `devices`) | Исправлено везде |

**Coverage Summary Table:**

| Requirement | Has Task? | Task IDs |
|---|---|---|
| FR-001 | ✅ | T030, T031, T033, T034 |
| FR-002 | ✅ | T030, T031, T032 |
| FR-003 | ✅ | T025, T026, T027 |
| FR-004 | ✅ | T033 |
| FR-005 | ✅ | T031, T033 |
| FR-006 | ✅ | T023 |
| FR-007 | ✅ | T028 |
| FR-008 | ✅ | T008, T035 |
| FR-009 | ✅ | T004, T022 |
| FR-010 | ✅ | T004, T018 |
| FR-011 | ✅ | T001, T002, T011 |
| FR-012 | ✅ | T011, T036 |
| FR-013 | ✅ | T013 |
| FR-014 | ✅ | T014 |
| FR-015 | ✅ | T019, T021 |
| FR-016 | ✅ | T015, T019, T021, T034 |
| FR-017 | ✅ | T019, T021 |
| FR-018 | ✅ | T020, T021, T026 |
| FR-019 | ✅ | T003, T015, T016, T025 |
| FR-020 | ✅ | T005, T014, T017, T026 |
| FR-021 | ✅ | T020, T027 |
| FR-022 | ✅ | T038 |
| FR-023 | ✅ | T007, T039 |
| FR-024 | ✅ | T012, T035 |
| SC-001 | ✅ | T037 |
| SC-002 | ✅ | T040 |
| SC-003 | ✅ | T029 |
| SC-004 | ✅ | T024 |

**Constitution Alignment Issues:** C1 (принцип VII до результата spike).

**Unmapped Tasks:** T009 (итоги spike), T010 (подключение подпроектов), T041 (README, DoD, диаграммы), T042
(`converge`) — служебные, обеспечивают сдачу.

**Metrics:**

- Total Requirements: 28 (24 FR, 4 SC)
- Total Tasks: 42
- Coverage: 100 % (28/28)
- Ambiguity Count: 1 (A3, исправлено)
- Duplication Count: 1 (D1, оставлено намеренно)
- Critical Issues Count: 0

## Next Actions

- CRITICAL нет. Перед `implement` — spike (Phase 0, T001–T009); если сработает запасной вариант, поправить plan.md и ADR
  и повторить `analyze`.
- После реализации — `converge` до `Converged`.
