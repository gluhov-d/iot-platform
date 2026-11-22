#!/usr/bin/env bash
# Копия каждого шарда реестра устройств - с РЕПЛИКИ, без остановки сервисов. Единственная реализация копии шардов:
# по расписанию её запускает cron контейнера backup-runner (копия уходит в MinIO), а `make shard-backup` запускает
# тот же скрипт в этом контейнере с BACKUP_TARGET=dir (копия остаётся в backups/<UTC-время>/ на хосте).
#
#   BACKUP_TARGET=minio (по умолчанию) -> бакет MINIO_BUCKET_BACKUPS, postgres/<UTC-время>/
#   BACKUP_TARGET=dir                   -> BACKUP_DIR (по умолчанию /backups)/<UTC-время>/
#
# В каталоге копии: shard_0.dump, shard_1.dump (pg_dump, формат custom) и manifest.txt - число строк devices и
# processed_device_events, версия схемы Flyway, SHA-256 файлов, до 5 deviceId каждого шарда для проверки после
# восстановления и позиции группы device-collector в теме devices - по каждому разделу.
#
# Порядок (шарды и сервис продолжают работать, нагрузка копии ложится на реплику):
#   1. обе реплики должны быть в режиме streaming;
#   2. записываются позиции группы device-collector по каждому разделу devices: подтверждённый offset, а у раздела
#      без коммита - его начало (restore вернёт группу и туда: иначе сообщения, обработанные после копии, не дочитались бы);
#   3. шаг 2 раньше копий: сервис подтверждает offset ПОСЛЕ записи в шард, поэтому всё, что покрыто этими offset,
#      уже на шарде. Реплика должна применить WAL шарда до текущей позиции - тогда копия содержит всё, что покрыто offset;
#   4. pg_dump на реплике: согласованный снимок на момент старта (запись на шарде его не затрагивает);
#   5. копия читается целиком (повреждённый файл - ошибка сейчас, а не при восстановлении), числа для manifest
#      берутся из самой копии.
# Копия собирается в <время>.partial и переименовывается только после успеха: незаконченную restore не возьмёт
# (в MinIO - `mc mv` каталога после загрузки manifest).
#
# Блокировка: flock -n на /tmp/backup-postgres.lock - второй одновременный запуск (cron и make) отказывается с кодом 75.
# pg_dump - логическая копия на момент запуска: подходит для учебного стенда и выборочного восстановления,
# но это не восстановление на момент времени (PITR: base backup + архив WAL).
#
# Окружение контейнера: SHARD_DB_USER, SHARD_DB_PASSWORD, KAFKA_INTERNAL_PORT, KAFKA_DEVICES_TOPIC,
# DEVICE_COLLECTOR_CONSUMER_GROUP; для minio - MINIO_BUCKET_BACKUPS, BACKUP_POSTGRES_S3_ACCESS_KEY/SECRET_KEY.
set -euo pipefail

LOCK_FILE="${BACKUP_LOCK_FILE:-/tmp/backup-postgres.lock}"
exec 9> "${LOCK_FILE}"
flock -n 9 || { echo "[FAIL] another backup-postgres.sh is already running (lock ${LOCK_FILE}): this run is cancelled" >&2; exit 75; }

export SHARD_LIB_MODE=container
# shellcheck source=../postgres/shard-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../postgres/shard-lib.sh"

TARGET="${BACKUP_TARGET:-minio}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOCAL_ROOT="${BACKUP_DIR:-/backups}"
case "${TARGET}" in
  minio)
    : "${MINIO_BUCKET_BACKUPS:?MINIO_BUCKET_BACKUPS не задан}" "${BACKUP_POSTGRES_S3_ACCESS_KEY:?BACKUP_POSTGRES_S3_ACCESS_KEY не задан}" \
      "${BACKUP_POSTGRES_S3_SECRET_KEY:?BACKUP_POSTGRES_S3_SECRET_KEY не задан}"
    # Адрес MinIO - как в сети compose; ключи только в окружении процесса mc
    export MC_HOST_bpg="http://${BACKUP_POSTGRES_S3_ACCESS_KEY}:${BACKUP_POSTGRES_S3_SECRET_KEY}@${MINIO_ENDPOINT:-minio:9000}"
    WORK_ROOT="$(mktemp -d /tmp/backup-postgres.XXXXXX)"
    DEST="bpg/${MINIO_BUCKET_BACKUPS}/postgres/${STAMP}"
    ;;
  dir)
    WORK_ROOT="${LOCAL_ROOT}"
    DEST="${LOCAL_ROOT}/${STAMP}"
    ;;
  *) echo "[FAIL] BACKUP_TARGET=${TARGET}: допустимо minio или dir" >&2; exit 2 ;;
esac
WORK="${WORK_ROOT}/${STAMP}.partial"

on_exit() {
  local rc=$?
  if [[ "${TARGET}" == minio ]]; then
    # Каталог во временной области контейнера нужен только до загрузки; в MinIO остаётся только завершённая копия
    rm -rf "${WORK_ROOT}"
    mc rm --recursive --force "${DEST}.partial" > /dev/null 2>&1 || true
  fi
  if [[ ${rc} -ne 0 ]]; then
    echo "[FAIL] backup not completed (restore does not take an incomplete copy)" >&2
  fi
  exit "${rc}"
}
trap on_exit EXIT

pg_dump_shard() {
  PGPASSWORD="${SHARD_DB_PASSWORD}" pg_dump -h "postgres-shard-$1-replica" -U "${SHARD_DB_USER}" -d "shard_$1" \
    --format=custom --no-owner --no-privileges
}

echo "==> backup of all shards from replicas -> ${TARGET} ${DEST}"
for i in "${SHARDS[@]}"; do
  require_replica_streaming "${i}"
done

# Позиции ДО копий (см. пункт 3 в шапке). Группа или раздел без коммитов - не ошибка (позиция - начало раздела);
# недоступная Kafka - ошибка: копия без позиций при восстановлении не дочитала бы сообщения, обработанные после неё
offsets="$(group_positions "${CONSUMER_GROUP}" "${DEVICES_TOPIC}")" || exit 1
echo "    kafka: group ${CONSUMER_GROUP}, ${DEVICES_TOPIC} positions ${offsets} (partition without a commit - its start)"

mkdir -p "${WORK}"
manifest="${WORK}/manifest.txt"
{
  echo "created_at=${STAMP}"
  echo "shards=$(printf 'shard_%s,' "${SHARDS[@]}" | sed 's/,$//')"
  echo "kafka.group=${CONSUMER_GROUP}"
  echo "kafka.topic=${DEVICES_TOPIC}"
  echo "kafka.offsets=${offsets}"
} > "${manifest}"

for i in "${SHARDS[@]}"; do
  shard="shard_${i}"
  replica="postgres-shard-${i}-replica"
  dump="${WORK}/${shard}.dump"

  lsn="$(wait_replica_caught_up "${i}")"
  echo "==> ${shard}: pg_dump from ${replica} (WAL replayed up to ${lsn})"
  pg_dump_shard "${i}" > "${dump}"
  # Файл читается целиком, а не только оглавление
  pg_restore --file=/dev/null < "${dump}"

  devices="$(dump_rows "${dump}" devices)"
  events="$(dump_rows "${dump}" processed_device_events)"
  version="$(dump_schema_version "${dump}")"
  samples="$(dump_sample_ids "${dump}")"
  [[ -n "${version}" ]] || { echo "[FAIL] ${shard}: no Flyway migrations in the copy" >&2; exit 1; }
  {
    echo "${shard}.source=${replica}"
    echo "${shard}.wal_lsn=${lsn}"
    echo "${shard}.devices=${devices}"
    echo "${shard}.processed_device_events=${events}"
    echo "${shard}.schema_version=${version}"
    echo "${shard}.sha256=$(sha256 "${dump}")"
    echo "${shard}.sample_device_ids=${samples}"
  } >> "${manifest}"
  echo "    devices=${devices} processed_device_events=${events} schema=V${version} size=$(wc -c < "${dump}" | tr -d ' ') bytes"
done

if [[ "${TARGET}" == minio ]]; then
  # В MinIO нет переименования каталога: загрузка в <метка>.partial, затем перенос; незавершённая загрузка (сбой
  # посередине) остаётся под .partial и удаляется в on_exit. Перенос идёт после загрузки manifest
  mc cp --recursive --quiet "${WORK}/" "${DEST}.partial/" > /dev/null
  # Загруженное сверяется с manifest до переноса: копия, которую restore не примет, не должна выглядеть завершённой
  for i in "${SHARDS[@]}"; do
    uploaded="$(mc cat "${DEST}.partial/shard_${i}.dump" | { sha256sum || true; } | cut -d' ' -f1)"
    [[ "${uploaded}" == "$(sed -n "s/^shard_${i}\.sha256=//p" "${manifest}")" ]] \
      || { echo "[FAIL] shard_${i}.dump: SHA-256 in MinIO differs from the copy" >&2; exit 1; }
  done
  mc mv --recursive --quiet "${DEST}.partial/" "${DEST}/" > /dev/null
  echo "==> manifest: ${DEST}/manifest.txt"
else
  mv "${WORK}" "${DEST}"
  echo "==> manifest: ${DEST}/manifest.txt"
fi
echo "==> Backup done: ${DEST}"
