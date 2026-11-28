#!/bin/bash
# Восстановление базы ClickHouse из копии в MinIO (make clickhouse-restore BACKUP=<метка>):
#
#   restore-clickhouse.sh <метка>        - метка из make backup-list (каталог backups/clickhouse/<метка>/)
#
# RESTORE DATABASE ... FROM S3(backup_s3, '<метка>') выполняет сам ClickHouse (ключи - в named collection backup_s3,
# как у backup-clickhouse.sh; research.md R17). Порядок:
#   1. копия должна быть завершённой: в MinIO есть её manifest.txt (незавершённая лежит в <метка>.partial и не берётся);
#   2. если база уже существует и в ней есть таблицы, без подтверждения отказ: восстановление заменяет базу копией
#      (RESTORE_CONFIRM=yes, make clickhouse-restore ... CONFIRM=yes - существующая база удаляется целиком);
#      пустой или отсутствующей базы подтверждение не требует;
#   3. RESTORE асинхронно с уникальным id, статус читается из system.backups;
#   4. число строк каждой таблицы сверяется с manifest: расхождение - ошибка (если в базу уже пишут новые события,
#      строк будет больше; останавливайте запись на время восстановления).
# Состояние базы - на момент копии: события, принятые после неё, восстановление не возвращает.
#
# Окружение контейнера backup-runner: CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, MINIO_BUCKET_BACKUPS,
# BACKUP_CLICKHOUSE_S3_ACCESS_KEY, BACKUP_CLICKHOUSE_S3_SECRET_KEY; необязательно CLICKHOUSE_HTTP (clickhouse:8123),
# MINIO_ENDPOINT (minio:9000), BACKUP_CLICKHOUSE_TIMEOUT (секунды ожидания, 1800).
set -euo pipefail

LABEL="${1:-}"
[[ "${LABEL}" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] \
  || { echo "[FAIL] backup label '${LABEL}': expected <UTC-time>, e.g. 20261122T030000Z (make backup-list)" >&2; exit 2; }

: "${CLICKHOUSE_USERNAME:?CLICKHOUSE_USERNAME не задан}" "${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD не задан}" \
  "${CLICKHOUSE_DATABASE:?CLICKHOUSE_DATABASE не задан}" "${MINIO_BUCKET_BACKUPS:?MINIO_BUCKET_BACKUPS не задан}" \
  "${BACKUP_CLICKHOUSE_S3_ACCESS_KEY:?BACKUP_CLICKHOUSE_S3_ACCESS_KEY не задан}" \
  "${BACKUP_CLICKHOUSE_S3_SECRET_KEY:?BACKUP_CLICKHOUSE_S3_SECRET_KEY не задан}"

HTTP="${CLICKHOUSE_HTTP:-clickhouse:8123}"
TIMEOUT="${BACKUP_CLICKHOUSE_TIMEOUT:-1800}"
export MC_HOST_bch="http://${BACKUP_CLICKHOUSE_S3_ACCESS_KEY}:${BACKUP_CLICKHOUSE_S3_SECRET_KEY}@${MINIO_ENDPOINT:-minio:9000}"
SRC="bch/${MINIO_BUCKET_BACKUPS}/clickhouse/${LABEL}"

# Запрос к ClickHouse по HTTP (тело - SQL); учётная запись - в заголовках, не в адресе
ch() {
  wget -q -O - --header "X-ClickHouse-User: ${CLICKHOUSE_USERNAME}" --header "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD}" \
    --post-data "$1" "http://${HTTP}/"
}

manifest="$(mc cat "${SRC}/manifest.txt" 2>/dev/null)" \
  || { echo "[FAIL] backup ${LABEL} not found or incomplete in MinIO (no manifest.txt): make backup-list" >&2; exit 1; }
value() { sed -n "s/^$1=//p" <<<"${manifest}" | head -1; }
DB="$(value clickhouse.database)"
[[ -n "${DB}" ]] || { echo "[FAIL] manifest of ${LABEL} has no clickhouse.database" >&2; exit 1; }
rows_expected="$(grep -E '^clickhouse\.[^=]+\.rows=' <<<"${manifest}" || true)"
[[ -n "${rows_expected}" ]] || { echo "[FAIL] manifest of ${LABEL} has no table row counts" >&2; exit 1; }

echo "==> restore ClickHouse database ${DB} from ${SRC}"
tables_now="$(ch "SELECT count() FROM system.tables WHERE database = '${DB}'")" \
  || { echo "[FAIL] ClickHouse does not answer on ${HTTP}" >&2; exit 1; }
if [[ "${tables_now}" != 0 ]]; then
  [[ "${RESTORE_CONFIRM:-}" == yes ]] \
    || { echo "[FAIL] database ${DB} exists and has ${tables_now} tables: restore would DELETE it (use make clickhouse-restore BACKUP=${LABEL} CONFIRM=yes)" >&2; exit 1; }
  echo "    database ${DB} is dropped (${tables_now} tables)"
fi
ch "DROP DATABASE IF EXISTS \`${DB}\` SYNC" > /dev/null

ID="restore-${LABEL}-$(date -u +%Y%m%dT%H%M%SZ)"
ch "RESTORE DATABASE \`${DB}\` FROM S3(backup_s3, '${LABEL}') SETTINGS id = '${ID}', async = 1" > /dev/null \
  || { echo "[FAIL] ClickHouse rejected RESTORE (named collection backup_s3, named_collection_control, MinIO key?)" >&2; exit 1; }

deadline=$(( $(date +%s) + TIMEOUT ))
while true; do
  status="$(ch "SELECT status FROM system.backups WHERE id = '${ID}' ORDER BY start_time DESC LIMIT 1")" \
    || { echo "[FAIL] ClickHouse does not answer on ${HTTP}" >&2; exit 1; }
  case "${status}" in
    RESTORED) break ;;
    RESTORE_FAILED|RESTORE_CANCELLED)
      echo "[FAIL] ${ID}: ${status}: $(ch "SELECT error FROM system.backups WHERE id = '${ID}' ORDER BY start_time DESC LIMIT 1" || true)" >&2
      exit 1 ;;
  esac
  [[ $(date +%s) -lt ${deadline} ]] || { echo "[FAIL] ${ID}: no result in ${TIMEOUT} s (status: ${status:-unknown})" >&2; exit 1; }
  sleep 2
done
echo "    RESTORED (${ID})"

# Сверка с manifest
bad=0
while IFS= read -r line; do
  table="${line#clickhouse.}"; table="${table%%.rows=*}"
  expected="${line##*=}"
  actual="$(ch "SELECT count() FROM \`${DB}\`.\`${table}\`" || true)"
  if [[ "${actual}" == "${expected}" ]]; then
    echo "    ${table}: ${actual} rows, matches manifest"
  else
    echo "[FAIL] ${table}: ${actual:-no table} rows, manifest ${expected}" >&2
    bad=1
  fi
done <<<"${rows_expected}"
[[ ${bad} -eq 0 ]] || exit 1
echo "==> Restore done from ${LABEL}"
