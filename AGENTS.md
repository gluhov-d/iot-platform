# AGENTS.md — правила для ИИ-агента в этом репозитории

Реализация идёт по Spec Kit: `/speckit-implement` выполняет `specs/<фича>/tasks.md`, `/speckit-converge` сверяет код
со spec, plan и tasks. Текущая фича — в `.specify/feature.json` (локальный файл; для device-service —
`specs/001-device-service`, для router-manager — `specs/003-router-manager`). Принципы платформы — `.specify/memory/constitution.md`, решения — `docs/adr/`.

## Как работать по задачам

- Одна задача — одно законченное изменение; после неё отметить `[X]` в `tasks.md` и коротко сообщить, что сделано и
  чем проверено. Задачи фазы идут по порядку; `[P]` можно параллельно.
- Тест пишется до кода и сначала падает. В конце фазы выполнить команды её Checkpoint и привести их результат.
- Задача с пометкой `(Rn)` сначала проверяет предположение из `research.md`. Не подтвердилось — остановиться,
  сообщить, предложить запасной вариант из `research.md`; plan и ADR правятся до продолжения.
- Задача с пометкой **ЧЕЛОВЕК** — остановиться и ждать: её выполняет человек.
- Расхождение кода со spec, plan или tasks исправляется в коде. Если неверен документ — остановиться и сообщить,
  молча документ не менять.
- Делать только то, что названо в задаче: без попутных переименований, переформатирования и «улучшений».

## Чего агент не делает

- Не коммитит, не пушит, не создаёт теги и PR — это делает человек после проверки по задачам.
- Не публикует релизы в Nexus основного стенда (`make avros-up`, `make avros-release`, `make ds-client-release`):
  выпущенная версия не перезаписывается. Для проверок — одноразовый стенд со своим Nexus.
- Не меняет применённые миграции `V*.sql` (даже комментарий — меняется контрольная сумма Flyway); новое изменение
  схемы — новая миграция.
- Не читает, не печатает и не меняет `.env`; новые переменные добавляет в `.env.example` и называет в отчёте.
- Не удаляет тома и данные стенда (`make clean`, `docker compose down -v`, `docker volume prune`) и не останавливает
  чужой стенд; свой проверочный стенд поднимает под отдельным `COMPOSE_PROJECT_NAME` и убирает за собой.
- Не добавляет секреты, токены и пароли в код, тесты, логи и документы.

## Сборка и проверка

| Что | Команда |
|---|---|
| Unit-тесты сервиса | `./gradlew :services:<сервис>:test` |
| Интеграционные (Testcontainers, нужен Docker) | `./gradlew :services:<сервис>:integrationTest` |
| Всё с порогом покрытия | `./gradlew :services:<сервис>:check` |
| Корректность Compose | `make check` |
| Стенд: поднять / выключить (тома остаются) | `make up` / `make down` |
| Проверки стенда | `make check-local`, `make dcs-quick-check`, `make ds-quick-check`, `make fep-quick-check` |
| MinIO и наблюдаемость | `make minio-check`, `make obs-check`, `make failed-replay-test` (`make failed-replay KEY=…` публикует в настоящую тему — только на своём стенде) |
| Копии баз | `make backup`, `make backup-list`; `make backup-check CONFIRM=yes` удаляет данные шардов и ClickHouse проекта Compose — только под своим `COMPOSE_PROJECT_NAME` |
| Схемы avros без публикации | `make avros-check` |
| Контракт protos без публикации | `make protos-check` |
| router-manager: unit с `-race` / покрытие / интеграционные | `make rm-test` / `make rm-coverage` / `make rm-it` (Go в контейнере `golang`, нужен Docker) |
| router-manager на стенде | `make rm-emulator-up`, `make rm-quick-check` |

- Go (router-manager): сборка и тесты — только в контейнере `golang:1.26.11-bookworm` через цели make; сгенерированный код
  (`internal/gen`) в Git не попадает; новая версия protos до публикации человеком — генерация из локальной сборки jar.
- Новая версия avros до публикации человеком: сервисы собираются против локальной сборки avros (локальный Maven-репозиторий
  или одноразовый Nexus), в `build.gradle.kts` — сразу новая версия; адреса локальных репозиториев в репозиторий не попадают.
- Проверка, которая ждёт часов или суток (сроки ILM), не держит сессию: запустить, сообщить, когда проверять, задачу проверки
  оставить `[ ]`.

## Соглашения кода

- Go-сервис router-manager: пакеты `internal/domain/{model,port,service}`, `internal/adapter/{in,out}`, `internal/config`,
  `internal/observability`, `internal/routerauth` (подпись роутера, общая с эмулятором), `emulator`, программы в `cmd/`;
  домен не зависит от адаптеров (`internal/archtest`). Unit — `*_test.go` без Docker; интеграционные (тег `integration`) —
  `*_it_test.go` рядом с адаптером и сквозные в пакете `it`; имена `TestXxx` по сценарию.
- Java 25, Spring Boot 4, Gradle Kotlin DSL. Пакеты сервиса: `adapter/in`, `adapter/out`, `domain/{model,port,service,exception}`,
  `config`, `support`; домен не зависит от адаптеров (проверяет `ArchitectureTest`).
- Тесты: `*Test` — unit без Docker, `It*Test` — интеграционные в пакете `it`; имена методов
  `givenX_whenY_thenZ`. Число тестов и покрытие в документах — из отчётов сборки, не по памяти.
- Конфигурация — только из переменных окружения: в `application.yaml` нет зашитых адресов, тайм-аутов и чисел попыток;
  в `docker-compose.yaml` переменные без значений по умолчанию, значения — в `.env.example`. Исключения (решение при
  converge, 18.11.2026): значения по умолчанию в `application.yaml` events-collector-service и device-collector-service
  (код прошлых модулей, совпадают с `.env.example`, кроме пустого `REDIS_PASSWORD` для запуска без пароля);
  в Compose — `COMPOSE_PROJECT_NAME` и `DEVICE_COLLECTOR_CONSUMER_GROUP`
  (стенд со старым `.env` сохраняет имя проекта и группы). Новый код исключений не добавляет.
- Avro-схемы — только в `avros/`; сервисы подключают явную версию; `auto.register.schemas=false`.
- gRPC — только из `protos/`: Go-код генерируется при сборке из артефакта Nexus явной версии (`PROTOS_VERSION`); ошибки — коды gRPC без внутренностей.
- HTTP API — только из OpenAPI: DTO и интерфейсы генерируются при сборке, в Git не попадают; ошибки — Problem Details.
- Схема БД — только миграциями `services/db-migrator` с `IF NOT EXISTS`; сервисы реестра устройств работают с БД через
  ShardingSphere-Proxy, router-manager — со своей базой `iot_commands` напрямую (pgx, ADR-0026).
- Конфигурация компонентов стенда — `infrastructure/config/<компонент>/`, одноразовые скрипты — `infrastructure/init/<компонент>/`.
  Новый скрипт: `chmod +x` и `git add --chmod=+x`. Скрипты работают в `/bin/sh` образов alpine и в bash 3.2 (macOS):
  без `declare -A`, `mapfile`.
- Образы и зависимости — с фиксированными версиями, без `latest`. Порт сервиса с диапазоном портов брать через
  `docker compose port <сервис> 8080`, не числом.
- Новый подпроект Gradle: строка `COPY …/build.gradle.kts` в Dockerfile всех остальных подпроектов.
- Комментарии и документы — по-русски, по существу; части платформы называются именами («приём событий»,
  «реестр устройств», «управление устройствами», «управление роутерами»), без номеров учебных модулей.
- Диаграммы — Mermaid в `diagrams/`; после правки перерендерить PNG там, где он лежит рядом.
