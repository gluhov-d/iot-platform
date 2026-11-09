#!/usr/bin/env bash
# Резервная копия каждого шарда реестра устройств - с РЕПЛИКИ, без остановки сервисов (make shard-backup):
#
#   infrastructure/config/postgres/shard-backup.sh        -> backups/<UTC-время>/
#
# В каталоге копии: shard_0.dump, shard_1.dump (pg_dump, формат custom) и manifest.txt - число строк devices и
# processed_device_events, версия схемы Flyway, SHA-256 файлов, до 5 deviceId каждого шарда для проверки после
# восстановления и подтверждённые offset группы device-collector в теме devices.
#
# Порядок (шарды и сервис продолжают работать, нагрузка копии ложится на реплику):
#   1. обе реплики должны быть в режиме streaming;
#   2. записываются подтверждённые offset группы device-collector;
#   3. шаг 2 раньше копий: сервис подтверждает offset ПОСЛЕ записи в шард, поэтому всё, что покрыто этими offset,
#      уже на шарде. Реплика должна применить WAL шарда до текущей позиции - тогда копия содержит всё, что покрыто offset;
#   4. pg_dump на реплике: согласованный снимок на момент старта (запись на шарде его не затрагивает);
#   5. копия читается целиком (повреждённый файл - ошибка сейчас, а не при восстановлении), числа для manifest
#      берутся из самой копии.
# Копия собирается в <время>.partial и переименовывается только после успеха: незаконченную restore не возьмёт.
#
# pg_dump - логическая копия на момент запуска: подходит для учебного стенда и выборочного восстановления,
# но это не восстановление на момент времени (PITR: base backup + архив WAL).
set -euo pipefail

# shellcheck source=shard-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/shard-lib.sh"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DIR="${BACKUP_ROOT}/${STAMP}"
WORK="${DIR}.partial"

on_exit() {
  local rc=$?
  if [[ ${rc} -ne 0 && -d "${WORK}" ]]; then
    echo "[FAIL] backup not completed, incomplete copy left in ${WORK} (restore does not take it)" >&2
  fi
  exit "${rc}"
}
trap on_exit EXIT

echo "==> backup of all shards from replicas -> ${DIR}"
for i in "${SHARDS[@]}"; do
  require_replica_streaming "${i}"
done

# Offset ДО копий (см. пункт 3 в шапке). Группа без коммитов - не ошибка; недоступная Kafka - ошибка:
# копия без offset при восстановлении не дочитала бы сообщения, обработанные после неё
[[ -n "$("${COMPOSE[@]}" ps -q --status running kafka)" ]] \
  || { echo "[FAIL] kafka is not running: offsets of ${CONSUMER_GROUP} cannot be recorded (make up)" >&2; exit 1; }
offsets="$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "kafka:${KAFKA_PORT}" \
    --describe --group "${CONSUMER_GROUP}" < /dev/null 2>/dev/null \
  | awk -v topic="${DEVICES_TOPIC}" '$2 == topic && $3 ~ /^[0-9]+$/ && $4 ~ /^[0-9]+$/ { printf "%s%s:%s", sep, $3, $4; sep = "," }' || true)"
echo "    kafka: group ${CONSUMER_GROUP}, ${DEVICES_TOPIC} offsets ${offsets:-none (group has no commits yet)}"

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
  "${COMPOSE[@]}" exec -T "${replica}" sh -c \
    'pg_dump -U "$POSTGRES_USER" -d "shard_$1" --format=custom --no-owner --no-privileges' sh "${i}" < /dev/null > "${dump}"
  # Файл читается целиком, а не только оглавление
  "${COMPOSE[@]}" exec -T "${replica}" pg_restore --file=/dev/null < "${dump}"

  devices="$(dump_rows "${dump}" devices "${replica}")"
  events="$(dump_rows "${dump}" processed_device_events "${replica}")"
  version="$(dump_schema_version "${dump}" "${replica}")"
  samples="$(dump_sample_ids "${dump}" "${replica}")"
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

mv "${WORK}" "${DIR}"
echo "==> manifest: ${DIR}/manifest.txt"
echo "==> Backup done: ${DIR}"