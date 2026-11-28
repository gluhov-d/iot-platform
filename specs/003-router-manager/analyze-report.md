# Specification Analysis Report — 003-router-manager

Сверка `constitution.md`, `spec.md`, `plan.md`, `tasks.md`, `data-model.md`, `research.md`, `quickstart.md`, контракта
`router_manager.proto`, ADR 0020–0026 и диаграмм (`/speckit-analyze`), 2026-11-28. Механическая часть (покрытие FR и SC задачами,
ссылки, переменные окружения, тесты spec ↔ tasks, ссылки `Rn ↔ T###`) — скриптом согласованности вне репозитория и чтением по
шагам analyze: 27 FR и 5 SC, 49 задач, покрытие 32 из 32. Находки скрипта по переменным (`POSTGRES_EXTRA_DBS`, `ROUTER_FAILED`,
`UPDATE_AGENT`) ложные: первая — уже существующая переменная стенда, две другие — значения enum. Затем два независимых
ревью только на чтение (требования и задачи; ADR, диаграммы и правила), каждая находка подтверждена по файлам. Отчёт — по
состоянию после исправлений; файлы анализ не меняет, исправления внесены отдельно и перечислены в колонке «Статус».

| ID | Category | Severity | Location(s) | Summary | Статус |
|----|----------|----------|-------------|---------|--------|
| S1 | Security | HIGH | spec.md (Clarifications, FR-018), data-model.md, ADR-0023, research.md R17 | Подпись роутера покрывала только `router_id`, время и метод: перехваченные за балансировщиком заголовки в окне ±60 с позволяли подменить итог `ReportResult` и повтором `PollCommand` забрать чужую команду | Исправлено (решение владельца): строка подписи включает метод, роутер, время, nonce и поля тела; принятая подпись одноразовая — `SET NX router:sig:v1:{routerId}:{sig}` на 120 с, при недоступном Redis проверка пропускается со счётчиком (R23, T015, T033) |
| S2 | Inconsistency | MEDIUM | spec.md (Clarifications) | Ответ утверждал, что повтор в окне закрывает TLS, а ADR-0023 признавал повтор возможным | Исправлено вместе с S1 |
| I1 | Inconsistency | HIGH | ADR-0022, spec.md (FR-014), data-model.md | Граница «17 с, третий опрос не раньше 20 с» держится на соблюдении срока вызова go-redis (R7) — это предположение, а документы подавали его как факт | Исправлено: в ADR-0022 и FR-014 — «предположение R7, проверяет T021», запасной вариант и граница при нём записаны в ADR |
| I2 | Inconsistency | MEDIUM | plan.md, tasks.md (T007, T009) | Адрес базы для миграций назывался по-разному (`ROUTER_MANAGER_DB_URL` / `DB_ROUTER_MANAGER_URL`), учётка владельца для `db-migrator-router-manager` не была задана | Исправлено: db-migrator получает `DB_URL`, `DB_USER`, `DB_PASSWORD` (владелец — `POSTGRES_USER`) из существующих переменных; строка в plan.md, T007, T009 |
| I3 | Inconsistency | MEDIUM | spec.md (Input) | «Остальная платформа не меняется» противоречило переносу `postgres`, realm, Alloy, Grafana | Исправлено: перечислены изменения окружения, полный список — в разделе router-manager `docs/definition-of-done.md` |
| I4 | Inconsistency | MEDIUM | containers.mmd, ADR-0020 | Подписи Camunda и Device Gateway описывали удалённый командный маршрут; ADR не называл удалённую связь C1 | Исправлено: подписи узлов «план», в ADR-0020 — «C1–C7 и E5» |
| I5 | Inconsistency | MEDIUM | plan.md, AGENTS.md, tasks.md | Структура проекта и место интеграционных тестов расходились (`routerauth`, `archtest`, `grpc/auth`; IT только в `it/`) | Исправлено: дерево plan.md и правило AGENTS.md — IT рядом с адаптерами и сквозные в `it/` |
| I6 | Inconsistency | MEDIUM | spec.md (FR-002) | Проверка ссылалась на несуществующую цель `make rm-build` | Исправлено: `docker compose … build router-manager` |
| I7 | Inconsistency | MEDIUM | constitution.md | «PostgreSQL за ShardingSphere-Proxy» без исключения для router-manager | Исправлено: исключение со ссылкой на ADR-0026 (поправка 1.3.0) |
| I8 | Inconsistency | LOW | contracts/router_manager.proto | Ссылки на ADR и задачу переноса неверны (ADR-0020/0024, T003) | Исправлено: ADR-0024 (breaking), ADR-0023 (токен), T001 |
| I9 | Inconsistency | LOW | rm-poll-command-sequence.mmd, rm-poll-empty-sequence.mmd, rm-stuck-command-sequence.mmd | Не было ветки отказа подписи у опроса с командой; Alloy читал логи «из router-manager», а не через Docker API; неверное число обращений к Redis | Исправлено, PNG перерисованы |
| C1 | Coverage | MEDIUM | tasks.md (T033) | FR-017 и FR-026 требуют проверить пользовательский токен и неподписанный запрос роутера — тестов не было | Исправлено: `TestUserTokenDenied`, `TestUnsignedRouterRequest`, плюс `TestReplayedRequestRejected`, `TestTamperedReportRejected`, `TestReplayGuardSkippedWithRedisPaused` (S1) |
| C2 | Coverage | MEDIUM | tasks.md (T046) | Публикация protos 1.1.0 без пометки **ЧЕЛОВЕК**, новый метод без теста до кода, у фазы P8 нет Checkpoint | Исправлено: T046 с тестом до кода, T047 **ЧЕЛОВЕК**, Checkpoint P8 |
| C3 | Coverage | LOW | docs/definition-of-done.md | Список изменений прежних секций был неполон | Исправлено: сборка protos, Compose и Makefile, ключи Redis, Alloy и Grafana |
| R1 | Research | MEDIUM | research.md R13, R15 | R13 подавал lint без исключений как проверенный прототипом; R15 — пользовательский токен и роли `commands.*` | Исправлено: R13 — проверено на итоговом контракте (`buf lint` STANDARD, `generate`, `build`); R15 — «Частично», остаток проверяет T033 |
| R2 | Research | LOW | ADR-0024, ADR-0025, ADR-0026 | Предположения R20, R19 и поведение init-скрипта на существующем томе поданы как факты | Исправлено: помечены предположениями с проверяющими задачами; ADR-0026 не утверждает состояние старых томов |
| R3 | Risk | MEDIUM | research.md | Не проверены R7, R15 (частично), R19–R23 | Открыто: у каждого есть проверяющая задача `(Rn)` до зависящего кода и запасной вариант |
| A1 | Ambiguity | LOW | data-model.md | Повтор `CreateCommand` с тем же `request_id`, но другими полями | Исправлено: возвращается существующая команда, поля не сравниваются |
| A2 | Ambiguity | LOW | spec.md (FR-022) | Значения меток `router_manager_polls_total` не заданы, а панель «доля из кэша» на них опирается | Исправлено: `source` = `cache` \| `db`, `result` = `command` \| `empty` |
| T1 | Terminology | LOW | tasks.md, plan.md | Номер учебной части в именах томов и ветки | Исправлено: тома `iot-rm-gomod`, `iot-rm-gobuild`; строка ветки убрана |
| X1 | Irreversible | HIGH | tasks.md T005, T047 | Публикация protos в Nexus необратима | Принято: обе задачи — **ЧЕЛОВЕК** |
| D1 | Deviation | LOW | spec.md (FR-003, Assumptions) | «Наружу только gRPC», а служебный HTTP-порт существует | Принято: порт только внутри сети Compose, на хост не публикуется — записано в Assumptions |

**Coverage Summary Table:** все FR-001…FR-027 и SC-001…SC-005 имеют задачи (FR-027 — только необязательная фаза P8).

**Unmapped tasks:** T042 (снимки, **ЧЕЛОВЕК**), T049 (converge) — требований FR не закрывают.

**Metrics:** требований 27 FR + 5 SC; задач 49 (из них **ЧЕЛОВЕК** 3: T005, T042, T047); критических находок 0; открытых HIGH 0,
открытых MEDIUM 1 (R3).
