#!/bin/bash
# Копия базы ClickHouse в MinIO (по расписанию из cron backup-runner и по `make backup`):
#
#   backups/clickhouse/<UTC-время>/   - файлы BACKUP DATABASE (метаданные и части таблиц) и manifest.txt
#
# BACKUP DATABASE ... TO S3(backup_s3, ...) выполняет сам ClickHouse (ADR-0017, research.md R17): запрос идёт по HTTP
# (порт 8123), копия уходит напрямую в MinIO. Адрес и ключи MinIO - в named collection backup_s3 из окружения
# контейнера clickhouse (config.d/backup_s3.xml, from_env); в тексте запроса и в этом скрипте ключей ClickHouse нет.
# Пользователю нужен named_collection_control (users.d/backup.xml).
#
# Порядок:
#   1. BACKUP в <метка>.partial с уникальным id в SETTINGS (повтор того же id ClickHouse отвергает), асинхронно:
#      длинный запрос не держит HTTP-соединение, статус читается из system.backups;
#   2. копия читается целиком: RESTORE в служебную базу <база>_backup_verify, числа строк для manifest берутся из неё
#      (в живой базе строки добавляются во время копии, число из неё не совпало бы с копией); служебная база удаляется;
#   3. manifest.txt загружается в <метка>.partial, затем каталог переносится в <метка> (mc mv): незавершённую копию
#      restore-clickhouse.sh не возьмёт.
# manifest: created_at, clickhouse.database, clickhouse.backup_id, clickhouse.version, clickhouse.<таблица>.rows.
#
# Блокировка: flock -n на /tmp/backup-clickhouse.lock - второй одновременный запуск отказывается с кодом 75.
#
# Окружение контейнера backup-runner: CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, MINIO_BUCKET_BACKUPS,
# BACKUP_CLICKHOUSE_S3_ACCESS_KEY, BACKUP_CLICKHOUSE_S3_SECRET_KEY; необязательно CLICKHOUSE_HTTP (clickhouse:8123),
# MINIO_ENDPOINT (minio:9000), BACKUP_CLICKHOUSE_TIMEOUT (секунды ожидания копии, 1800).
set -euo pipefail

LOCK_FILE="${BACKUP_CLICKHOUSE_LOCK_FILE:-/tmp/backup-clickhouse.lock}"
exec 9> "${LOCK_FILE}"
flock -n 9 || { echo "[FAIL] another backup-clickhouse.sh is already running (lock ${LOCK_FILE}): this run is cancelled" >&2; exit 75; }

: "${CLICKHOUSE_USERNAME:?CLICKHOUSE_USERNAME не задан}" "${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD не задан}" \
  "${CLICKHOUSE_DATABASE:?CLICKHOUSE_DATABASE не задан}" "${MINIO_BUCKET_BACKUPS:?MINIO_BUCKET_BACKUPS не задан}" \
  "${BACKUP_CLICKHOUSE_S3_ACCESS_KEY:?BACKUP_CLICKHOUSE_S3_ACCESS_KEY не задан}" \
  "${BACKUP_CLICKHOUSE_S3_SECRET_KEY:?BACKUP_CLICKHOUSE_S3_SECRET_KEY не задан}"

HTTP="${CLICKHOUSE_HTTP:-clickhouse:8123}"
DB="${CLICKHOUSE_DATABASE}"
VERIFY_DB="${DB}_backup_verify"
TIMEOUT="${BACKUP_CLICKHOUSE_TIMEOUT:-1800}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_ID="backup-${STAMP}"
# Ключи MinIO для mc - только в окружении процесса
export MC_HOST_bch="http://${BACKUP_CLICKHOUSE_S3_ACCESS_KEY}:${BACKUP_CLICKHOUSE_S3_SECRET_KEY}@${MINIO_ENDPOINT:-minio:9000}"
DEST="bch/${MINIO_BUCKET_BACKUPS}/clickhouse/${STAMP}"
WORK="$(mktemp -d /tmp/backup-clickhouse.XXXXXX)"

# Запрос к ClickHouse по HTTP (тело запроса - SQL). Учётная запись - в заголовках, не в адресе
ch() {
  wget -q -O - --header "X-ClickHouse-User: ${CLICKHOUSE_USERNAME}" --header "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD}" \
    --post-data "$1" "http://${HTTP}/"
}

on_exit() {
  local rc=$?
  rm -rf "${WORK}"
  # Служебная база и незавершённая копия не остаются ни при успехе, ни при сбое
  ch "DROP DATABASE IF EXISTS \`${VERIFY_DB}\` SYNC" > /dev/null 2>&1 || true
  mc rm --recursive --force "${DEST}.partial" > /dev/null 2>&1 || true
  if [[ ${rc} -ne 0 ]]; then
    echo "[FAIL] backup not completed (restore does not take an incomplete copy)" >&2
  fi
  exit "${rc}"
}
trap on_exit EXIT

# Ожидание конца асинхронной операции по id: BACKUP_CREATED / RESTORED - успех, остальное - ошибка с текстом из system.backups
wait_operation() {
  local id="$1" deadline=$(( $(date +%s) + TIMEOUT )) status="" error=""
  while true; do
    status="$(ch "SELECT status FROM system.backups WHERE id = '${id}' ORDER BY start_time DESC LIMIT 1")" \
      || { echo "[FAIL] ClickHouse does not answer on ${HTTP}" >&2; return 1; }
    case "${status}" in
      BACKUP_CREATED|RESTORED) return 0 ;;
      BACKUP_FAILED|RESTORE_FAILED|BACKUP_CANCELLED|RESTORE_CANCELLED)
        error="$(ch "SELECT error FROM system.backups WHERE id = '${id}' ORDER BY start_time DESC LIMIT 1" || true)"
        echo "[FAIL] ${id}: ${status}: ${error}" >&2
        return 1 ;;
    esac
    [[ $(date +%s) -lt ${deadline} ]] || { echo "[FAIL] ${id}: no result in ${TIMEOUT} s (status: ${status:-unknown})" >&2; return 1; }
    sleep 2
  done
}

echo "==> backup of ClickHouse database ${DB} -> ${DEST}"
ch "BACKUP DATABASE \`${DB}\` TO S3(backup_s3, '${STAMP}.partial') SETTINGS id = '${BACKUP_ID}', async = 1" > /dev/null \
  || { echo "[FAIL] ClickHouse rejected BACKUP (named collection backup_s3, named_collection_control, MinIO key?)" >&2; exit 1; }
wait_operation "${BACKUP_ID}"
echo "    BACKUP_CREATED (${BACKUP_ID})"

# Копия читается целиком: восстановление в служебную базу и числа строк из неё
VERIFY_ID="verify-${STAMP}"
ch "DROP DATABASE IF EXISTS \`${VERIFY_DB}\` SYNC" > /dev/null
ch "RESTORE DATABASE \`${DB}\` AS \`${VERIFY_DB}\` FROM S3(backup_s3, '${STAMP}.partial') SETTINGS id = '${VERIFY_ID}', async = 1" > /dev/null \
  || { echo "[FAIL] ClickHouse rejected the check restore" >&2; exit 1; }
wait_operation "${VERIFY_ID}"

manifest="${WORK}/manifest.txt"
{
  echo "created_at=${STAMP}"
  echo "clickhouse.database=${DB}"
  echo "clickhouse.backup_id=${BACKUP_ID}"
  echo "clickhouse.version=$(ch 'SELECT version()')"
} > "${manifest}"
tables="$(ch "SELECT name FROM system.tables WHERE database = '${VERIFY_DB}' ORDER BY name")"
[[ -n "${tables}" ]] || { echo "[FAIL] the copy has no tables" >&2; exit 1; }
while IFS= read -r table; do
  rows="$(ch "SELECT count() FROM \`${VERIFY_DB}\`.\`${table}\`")"
  [[ "${rows}" =~ ^[0-9]+$ ]] || { echo "[FAIL] ${table}: cannot count rows in the copy" >&2; exit 1; }
  echo "clickhouse.${table}.rows=${rows}" >> "${manifest}"
  echo "    ${table}: ${rows} rows in the copy"
done <<<"${tables}"

mc cp --quiet "${manifest}" "${DEST}.partial/manifest.txt" > /dev/null
mc mv --recursive --quiet "${DEST}.partial/" "${DEST}/" > /dev/null
echo "==> manifest: ${DEST}/manifest.txt"
echo "==> Backup done: ${DEST}"
