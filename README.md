# IoT Platform

Учебная распределённая микросервисная платформа сбора телеметрии IoT-устройств.
Репозиторий содержит архитектурное описание в нотации **C4 (PlantUML)** и полностью
готовое локальное окружение на **Docker Compose**: хранилища, шину событий,
аутентификацию, BPM-движок и стек наблюдаемости.

Практическое задание по модулю 1 — «Настройка окружения проекта».

---

## Структура репозитория

```
iot-platform/
├── diagrams/                          # C4-диаграммы (PlantUML)
│   ├── context.puml                   #   Level 1 — системный контекст
│   └── containers.puml                #   Level 2 — контейнеры
├── infrastructure/                    # Инфраструктура Docker
│   ├── docker-compose.yaml            #   всё окружение одним файлом
│   ├── .env -> ../.env                #   симлинк на корневой .env
│   ├── config/                        #   конфигурации сервисов
│   │   ├── prometheus/prometheus.yml
│   │   ├── loki/loki-config.yaml
│   │   ├── tempo/tempo.yaml
│   │   ├── alloy/config.alloy
│   │   └── grafana/
│   │       ├── provisioning/          #     datasources + dashboard provider
│   │       └── dashboards/            #     kafka.json, postgresql.json
│   └── init/                          #   скрипты первичной инициализации
│       ├── postgres/                  #     доп. БД + схема devices/commands
│       └── clickhouse/                #     таблицы телеметрии и DLT
├── src/                               # Spring Boot приложение (заготовка)
├── Makefile                           # быстрый запуск инфраструктуры
├── README.md
├── .env.example                       # шаблон переменных окружения
└── .env                               # рабочие переменные (порты, логины, пароли)
```

Каталоги микросервисов (`device-service/`, `analytics-service/` и т. д.) появятся
в следующих модулях — на этапе архитектурного описания их ещё нет.

---

## Быстрый старт

Требуется Docker (≥ 24) с Docker Compose v2 и не менее 6 ГБ памяти, выделенной демону.

```bash
# 1. Подготовить переменные окружения
cp .env.example .env
#    при необходимости отредактируйте пароли и порты

# 2. Поднять всё и дождаться готовности
make up
```

Вариант из задания, без Makefile:

```bash
cd infrastructure
docker compose up -d
docker compose ps        # все сервисы должны быть healthy
```

> В репозитории уже лежит рабочий `.env` со значениями для локальной разработки —
> задание требует сдать его вместе с проектом. В реальном проекте в Git коммитят
> только `.env.example`, а `.env` добавляют в `.gitignore`.

Остановка и очистка:

```bash
make down     # остановить контейнеры, данные в томах сохранятся
make clean    # остановить и удалить тома (все данные будут потеряны)
make reset    # пересоздать окружение с нуля
```

### Команды Makefile

| Команда | Назначение |
|---|---|
| `make up` | Поднять инфраструктуру и дождаться healthy всех сервисов |
| `make down` | Остановить и удалить контейнеры (тома сохраняются) |
| `make stop` / `make start` | Приостановить / возобновить контейнеры |
| `make restart` | Перезапустить окружение |
| `make clean` | Удалить контейнеры вместе с томами |
| `make reset` | `clean` + `up` |
| `make pull` | Скачать/обновить образы |
| `make ps` | Статус контейнеров |
| `make health` | Healthcheck-статус всех сервисов |
| `make logs` / `make logs S=kafka` | Логи всех сервисов или одного |
| `make urls` | Показать адреса всех сервисов |
| `make psql` / `make redis-cli` / `make clickhouse-cli` | Клиенты к БД |
| `make topics` | Список топиков Kafka |
| `make check` | Валидация `docker-compose.yaml` |
| `make diagrams` | Отрендерить `.puml` в PNG (нужен `plantuml`) |

---

## Состав окружения

| Сервис | Назначение | Образ |
|---|---|---|
| `postgres` | Состояние устройств и команд, БД для Keycloak и Camunda | `postgres:16-alpine` |
| `redis` | Кэширование запросов и активных устройств | `redis:7-alpine` |
| `clickhouse` | Хранение телеметрии и событий | `clickhouse/clickhouse-server:24.8-alpine` |
| `kafka` | Шина событий (KRaft, без ZooKeeper) | `confluentinc/cp-kafka:7.6.1` |
| `kafka-init` | Одноразовое создание топиков | `confluentinc/cp-kafka:7.6.1` |
| `schema-registry` | Контракты Avro | `confluentinc/cp-schema-registry:7.6.1` |
| `minio` | Объектное хранилище (DLT + вложения) | `minio/minio` |
| `minio-init` | Одноразовое создание бакетов | `minio/mc` |
| `keycloak` | Аутентификация и авторизация (OAuth2/OIDC) | `quay.io/keycloak/keycloak:26.0` |
| `camunda` | Оркестрация бизнес-процессов | `camunda/camunda-bpm-platform:run-7.21.0` |
| `prometheus` | Сбор метрик | `prom/prometheus` |
| `loki` | Хранение логов | `grafana/loki` |
| `tempo` | Хранение трейсов (OTLP) | `grafana/tempo` |
| `alloy` | Сбор логов контейнеров и приём OTLP | `grafana/alloy` |
| `grafana` | Дашборды поверх Prometheus / Loki / Tempo | `grafana/grafana` |
| `postgres-exporter` | Метрики PostgreSQL для Prometheus | `quay.io/prometheuscommunity/postgres-exporter` |
| `kafka-exporter` | Метрики Kafka для Prometheus | `danielqsj/kafka-exporter` |

У всех долгоживущих сервисов настроены **healthcheck**, именованные **volume** для
данных и **depends_on** с условием `service_healthy`, поэтому порядок старта
детерминирован: `postgres` → `keycloak`/`camunda`, `kafka` → `schema-registry`/
`kafka-init`, `loki`+`tempo` → `alloy`, `prometheus`+`loki`+`tempo` → `grafana`.

---

## Адреса сервисов

| Сервис | URL | Учётные данные |
|---|---|---|
| PostgreSQL | `postgresql://localhost:5432/iot_devices` | `iot` / `iot_secret` |
| Redis | `redis://localhost:6379` | пароль `redis_secret` |
| ClickHouse | http://localhost:8123 (native `9010`) | `iot` / `clickhouse_secret` |
| Kafka | `localhost:29092` (внутри сети — `kafka:9092`) | — |
| Schema Registry | http://localhost:8081 | — |
| MinIO API | http://localhost:9000 | `minioadmin` / `minioadmin123` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Keycloak | http://localhost:8080 | `admin` / `admin` |
| Camunda | http://localhost:8088/camunda | `demo` / `demo` |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | — |
| Loki | http://localhost:3100 | — |
| Tempo | http://localhost:3200 (OTLP `4317`/`4318`) | — |
| Alloy | http://localhost:12345 | — |

> ClickHouse слушает нативный протокол на хостовом порту **9010**, а не 9000, чтобы
> не конфликтовать с MinIO API.
>
> Kafka снаружи доступна на **29092**: слушатель `EXTERNAL` анонсирует
> `localhost:29092`, внутри docker-сети сервисы ходят на `kafka:9092`.

---

## Переменные окружения

Все порты, логины и пароли вынесены в `.env` в корне репозитория; в
`infrastructure/` лежит симлинк на него, поэтому работает и `make up` из корня,
и `cd infrastructure && docker compose up -d`.

Основные группы переменных:

```dotenv
POSTGRES_PORT=5432
POSTGRES_DB=iot_devices
POSTGRES_USER=iot
POSTGRES_PASSWORD=iot_secret
POSTGRES_EXTRA_DBS=iot_commands,keycloak,camunda   # создаются init-скриптом

KAFKA_EXTERNAL_PORT=29092
KAFKA_CLUSTER_ID=5kF0GeTqS3OtiMbQduwsug                     # 16-байтный UUID в base64
KAFKA_TOPICS=iot.events:3:1,iot.device-id:3:1,iot.dlt:1:1   # topic:partitions:rf

MINIO_ROOT_USER=minioadmin
MINIO_BUCKETS=iot-dlt,iot-attachments

KEYCLOAK_PORT=8080
KEYCLOAK_PLATFORM=linux/amd64                               # см. раздел «Диагностика»
GRAFANA_PORT=3000
```

Полный перечень — в самом `.env`, каждая секция прокомментирована.
Значения рассчитаны на локальную разработку; в других контурах их подменяют
через секреты CI/CD.

---

## Что создаётся при первом запуске

* **PostgreSQL** — БД `iot_devices` (основная) плюс `iot_commands`, `keycloak`,
  `camunda`; таблицы `devices` и `device_commands` с индексами.
* **ClickHouse** — база `iot_telemetry`, таблицы `events` (MergeTree, партиции по
  месяцам, TTL 90 дней) и `dead_letters`.
* **Kafka** — топики `iot.events`, `iot.device-id`, `iot.dlt`.
* **MinIO** — бакеты `iot-dlt`, `iot-attachments`.

Инициализация выполняется один раз, при создании пустых томов. Чтобы прогнать её
заново — `make clean && make up`.

---

## Наблюдаемость

Grafana поднимается с автоматическим провижинингом:

* **Источники данных** — Prometheus, Loki, Tempo (`config/grafana/provisioning/datasources`).
* **Дашборды** — папка «IoT Platform» (`config/grafana/dashboards`):
  * **IoT Platform — Kafka**: брокеры онлайн, число топиков и партиций,
    under-replicated партиции, суммарный лаг консьюмеров, скорость записи по
    топикам, лаг по группам, таблица топиков и логи брокера из Loki.
  * **IoT Platform — PostgreSQL**: доступность и uptime, соединения по базам,
    cache hit ratio, commit/rollback rate, статистика по кортежам, размеры баз,
    deadlock-и и логи из Loki.

Метрики собирает Prometheus с `kafka-exporter`, `postgres-exporter`, MinIO,
Keycloak, Alloy, Loki, Tempo и Grafana. Логи всех контейнеров подбирает Alloy
через Docker-сокет и пишет в Loki с метками `service` и `container`. Трейсы
принимаются в Tempo по OTLP (`4317` gRPC, `4318` HTTP).

---

## C4-диаграммы

`diagrams/context.puml` — Level 1: инженер и оператор, IoT-устройства, Keycloak,
Grafana и Camunda вокруг платформы.

`diagrams/containers.puml` — Level 2: микросервисы платформы (API Gateway, API
Orchestrator, Device/Command/Event Service, Events/Device Collector, Failed
Events Processor) и инфраструктурные блоки — Kafka, Schema Registry, Redis,
PostgreSQL, ClickHouse, MinIO, Keycloak, Camunda, стек наблюдаемости.

Обе диаграммы подключают библиотеку через `!includeurl` из
[C4-PlantUML](https://github.com/plantuml-stdlib/C4-PlantUML), поэтому для
рендеринга нужен доступ в интернет:

```bash
make diagrams          # PNG рядом с .puml (требуется brew install plantuml)
```

Либо вставить содержимое файла в http://www.plantuml.com/plantuml.

---

## Диагностика

```bash
make health            # какой сервис не дошёл до healthy
make logs S=keycloak   # логи конкретного сервиса
make check             # синтаксис и подстановка переменных в compose
```

Типовые ситуации:

* **Порт занят** — поменяйте соответствующую `*_PORT` в `.env` и выполните `make up`.

* **Kafka падает с `Cluster ID ... does not appear to be a valid UUID`** — в режиме
  KRaft `KAFKA_CLUSTER_ID` обязан быть 16-байтным UUID в base64 (22 символа),
  произвольная строка не подойдёт. Сгенерировать новый:

  ```bash
  python3 -c "import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))"
  ```

  После смены значения том `kafka-data` нужно пересоздать: `make clean && make up`.

* **Keycloak в цикле перезапусков, в логах `SIGILL` в `System.registerNatives`** —
  на Apple Silicon с включённой опцией *Use Rosetta for x86/amd64 emulation*
  (Docker Desktop 4.37) нативный arm64-образ Keycloak роняет JVM на JDK 21.
  Поэтому в `.env` задан `KEYCLOAK_PLATFORM=linux/amd64` — amd64-вариант работает
  стабильно. На Linux/x86 это значение и так нативное. Альтернатива — выключить
  Rosetta в настройках Docker Desktop.

* **Healthcheck падает с `wget: not found`** — в образах `alloy` и `keycloak` нет
  ни `curl`, ни `wget`, поэтому их проверки сделаны через `/dev/tcp` в `bash`.
  Если меняете эти healthcheck-и, сначала проверьте, какие утилиты есть в образе.

* **Не хватает памяти** — стек целиком просит ~5 ГБ; в Docker Desktop выделите
  не менее 6 ГБ (Settings → Resources).

* **`input/output error` при `docker pull` или `docker prune`** — переполнен диск
  виртуальной машины Docker (не диск хоста). Проверить:

  ```bash
  docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i df -h /var/lib/docker
  ```

  Лечится увеличением *Disk image size* в Docker Desktop либо `docker builder prune -af`
  и удалением неиспользуемых образов. Если после сбоя `docker images` показывает
  пустой список — перезапустите Docker Desktop, метаданные восстановятся.
