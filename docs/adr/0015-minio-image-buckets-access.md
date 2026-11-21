# ADR-0015: Образ MinIO, бакеты и учётки

**Статус**: предложено, 2026-11-21 (на проверке дизайна) · **Спецификация**: [specs/002-failed-events](../../specs/002-failed-events/spec.md)

## Контекст

Стенду нужен MinIO одной нодой с закреплённым тегом и бакеты для необработанных сообщений, метрик, логов, трасс и резервных копий. Официальный `minio/minio` не отдаётся с Docker Hub (`pull access denied`, проверено); `quay.io/minio/minio` анонимно не отдаёт манифест. Прошлые задания версию MinIO не фиксировали.

## Решение

- Образ `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` (форк Pigsty, AGPL-3.0, amd64 и arm64). Проверено на прототипе (см. research.md R10): запуск под root, `minio --version`, в образе есть sh, curl и mc; нет grep, sed и awk. Healthcheck `mc ready local` работает.
- Testcontainers: `MinIOContainer(DockerImageName.parse(...).asCompatibleSubstituteFor("minio/minio"))`, модуль `testcontainers-minio:2.0.3` (см. research.md R19: 14 тестов на MinIO без расхождений с s3mock).
- Проект переименован в `pgsty/silo`: сам образ `pgsty/silo` не проверялся, тег `RELEASE.2026-11-12` не проверялся как образ MinIO; `pgsty/mc RELEASE.2026-11-12` проверен как клиент (backup-runner). Переход на silo — отдельное решение, оформляется своим ADR. Доступность тега 2026-08-04 под старым именем после переименования не проверена.
- Одна нода в базовом стенде, не в профиле `platform`: процессору она нужна всегда.
- Бакеты: `failed-events` (создается с Object Lock, ADR-0019), `mimir-blocks`, `loki-chunks`, `tempo-traces`, `backups`. Бакеты `iot-dlt`, `iot-attachments` и переменная `MINIO_BUCKETS` убираются.
- Init: `infrastructure/init/minio/init.sh` на чистом sh в том же образе (нет grep, sed, awk; используются `case`, `mc --json`, `tr`). Идемпотентен: повторный запуск проходит (см. research.md R10, R11). Политики и ILM — JSON в `infrastructure/config/minio/`. ILM ставится через `mc ilm import`: `mc ilm rule add` при повторе дублирует правила (см. research.md R11).
- Учётки: `failed-events-processor` — PutObject и GetObject на `failed-events/*`, без ListBucket (LIST дает 403, см. research.md R11). `mimir`, `loki`, `tempo` — полный доступ к своему бакету. `backup-postgres` — `backups/postgres/`, `backup-clickhouse` — `backups/clickhouse/`; ListBucket с условием `s3:prefix`. Чужой префикс отвечает 403 (см. research.md R11, R18).
- Сроки хранения (бакет / система): `failed-events` 180 суток; `backups` 30; `mimir-blocks` 380 / Mimir 365; `loki-chunks` 100 / Loki 90; `tempo-traces` 21 / Tempo 14. Бакетный срок больше системного, поэтому удаляет система.
- Метрики MinIO по бакетам — v3: `/minio/metrics/v3/cluster/usage/buckets` (`minio_cluster_usage_buckets_total_bytes{bucket}`, `minio_cluster_usage_buckets_objects_count{bucket}`), проверено curl. Имена `minio_bucket_usage_*` в образе 2026-08 отсутствуют (см. research.md R16).

## Последствия

- Закреплённый тег меняется в compose и в Testcontainers одновременно. Обновление образа — правка двух мест и прогон тестов.
- Нода одна: потеря тома означает потерю данных стенда. Единственная защита — копии (ADR-0017).
- Права по префиксу работают: копии не читают и не пишут чужой префикс (см. research.md R18).
- Init не меняет секрет существующей учётки (см. research.md R11): смена пароля — отдельная операция.
- Дашборды, построенные по `minio_bucket_usage_*` (метрики v2), не работают на этом образе. Панели строятся по v3.
- Размер и число объектов по бакетам получаются из v3, а не сканером; точность при большом объёме не проверена.

## Рассмотренные варианты

- Официальный `minio/minio`: недоступен. Отвергнуто.
- `bitnamilegacy/minio:2024.10.13`: legacy-образ, uid 1001, при тестах нужна явная команда `run.sh` (см. research.md R10). Отвергнуто.
- Один бакет `backups` с общими правами: нарушает требование «учётка только к своему бакету». Отвергнуто.
- Отдельные бакеты для PostgreSQL и ClickHouse: работает, но префиксы проще и проверены (см. research.md R11). Отвергнуто в пользу префиксов.
- Отдельный клиентский образ `mc`: в `pgsty/mc` нет curl и sh-утилит, которых требуют скрипты. Отвергнуто.
