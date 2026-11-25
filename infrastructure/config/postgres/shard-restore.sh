#!/usr/bin/env bash
# Восстановление всех шардов из копии shard-backup.sh в ЧИСТЫЕ контейнеры PostgreSQL (make shard-restore):
#
#   infrastructure/config/postgres/shard-restore.sh --yes [backups/<время>]
#   (по умолчанию - последняя завершённая копия)
#   infrastructure/config/postgres/shard-restore.sh --yes --from-minio [<метка>]
#   (копия из MinIO - backups/postgres/<метка>/, по умолчанию последняя; make shard-restore FROM=minio BACKUP=<метка>)
#   Копия из MinIO сначала скачивается во временный каталог через backup-runner, дальше путь тот же, что у локальной:
#   SHA-256 файлов сверяется с manifest, и только потом что-либо останавливается или удаляется.
#
# ВНИМАНИЕ: текущие данные шардов и их реплик удаляются - контейнеры и тома создаются заново.
#   1. SHA-256 файлов сверяется с manifest - до того, как что-либо остановлено или удалено;
#   2. device-collector-service и ShardingSphere-Proxy останавливаются: в шарды никто не пишет;
#   3. каждый шард: контейнеры шарда и его реплики и их тома удаляются, поднимается чистый шард,
#      pg_restore из копии (одной транзакцией: ошибка - пустая база, а не половина данных);
#      копия без прав (--no-privileges), поэтому право чтения device_reader выдаётся заново после shard-replication-init (роль создаёт он;
#      миграция V3 во восстановленной базе уже применена), до реплик: они получают права вместе с pg_basebackup;
#      число строк и версия схемы сверяются с manifest;
#   4. shard-replication-init заново создаёт роль replicator и слоты (слоты жили в удалённых томах),
#      реплики снимают новый pg_basebackup с восстановленных шардов и догоняют их;
#   5. позиции группы device-collector в теме devices по каждому разделу возвращаются к моменту копии (из manifest;
#      у раздела, где коммитов к тому моменту не было, - к началу раздела) и сверяются с Kafka: сообщения,
#      обработанные после копии, сервис обработает снова - журнал processed_device_events отсекает повторы,
#      реестр догоняет тему devices (пока сообщения в ней есть: тема хранит их KAFKA_DEVICES_RETENTION_MS, ADR-0011);
#   6. Proxy запускается заново (метаданные таблиц он читает при старте): устройства из manifest читаются через него и от имени учётной записи чтения device-service (база devices_read);
#   7. запускается device-collector-service.
# Состояние реестра после восстановления - на момент копии плюс то, что сервис дочитает из Kafka.
set -euo pipefail

# shellcheck source=shard-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/shard-lib.sh"

confirm=""
dir=""
from_minio=""
tmp_dir=""
# Временный каталог скачанной копии убирается при любом выходе (on_exit ниже его тоже убирает)
trap '[[ -z "${tmp_dir}" ]] || rm -rf "${tmp_dir}"' EXIT
for arg in "$@"; do
  case "${arg}" in
    --yes) confirm="yes" ;;
    --from-minio) from_minio="yes" ;;
    -*) echo "[FAIL] unknown option ${arg}" >&2; exit 2 ;;
    *) dir="${arg}" ;;
  esac
done

# Объект копии PostgreSQL из MinIO ($1 - путь после postgres/) в stdout. Ключи - из окружения backup-runner, на хост не попадают
minio_cat() {
  "${COMPOSE[@]}" exec -T backup-runner sh -c \
    '. /etc/backup.env; MC_HOST_bpg="http://${BACKUP_POSTGRES_S3_ACCESS_KEY}:${BACKUP_POSTGRES_S3_SECRET_KEY}@${MINIO_ENDPOINT:-minio:9000}"; export MC_HOST_bpg; mc cat "bpg/${MINIO_BUCKET_BACKUPS}/postgres/$1"' \
    sh "$1" < /dev/null
}
minio_labels() {
  "${COMPOSE[@]}" exec -T backup-runner sh -c \
    '. /etc/backup.env; MC_HOST_bpg="http://${BACKUP_POSTGRES_S3_ACCESS_KEY}:${BACKUP_POSTGRES_S3_SECRET_KEY}@${MINIO_ENDPOINT:-minio:9000}"; export MC_HOST_bpg; mc ls "bpg/${MINIO_BUCKET_BACKUPS}/postgres/"' \
    < /dev/null | awk '{ print $NF }' | tr -d / | grep -E '^[0-9]{8}T[0-9]{6}Z$' | sort
}

if [[ -n "${from_minio}" ]]; then
  [[ -n "$("${COMPOSE[@]}" ps -q --status running backup-runner)" ]] \
    || { echo "[FAIL] backup-runner is not running: the copy cannot be taken from MinIO (make up)" >&2; exit 1; }
  label="${dir}"
  if [[ -z "${label}" ]]; then
    # Завершённая копия - каталог с меткой времени (.partial в список не попадает)
    label="$(minio_labels | tail -1)"
    [[ -n "${label}" ]] || { echo "[FAIL] no completed PostgreSQL backups in MinIO (make backup)" >&2; exit 1; }
  fi
  [[ "${label}" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] \
    || { echo "[FAIL] backup label '${label}': expected <UTC-time>, e.g. 20261122T030000Z (make backup-list)" >&2; exit 1; }
  tmp_dir="$(mktemp -d)"
  dir="${tmp_dir}/${label}"
  mkdir "${dir}"
  echo "==> download backups/postgres/${label}/ from MinIO"
  for f in manifest.txt $(printf 'shard_%s.dump ' "${SHARDS[@]}"); do
    minio_cat "${label}/${f}" > "${dir}/${f}" \
      || { echo "[FAIL] ${f} of backup ${label} was not downloaded from MinIO (make backup-list)" >&2; exit 1; }
    [[ -s "${dir}/${f}" ]] || { echo "[FAIL] ${f} of backup ${label} is empty or missing in MinIO" >&2; exit 1; }
  done
elif [[ -z "${dir}" ]]; then
  # Незавершённые копии (*.partial) не выбираются
  dir="$(ls -1d "${BACKUP_ROOT}"/*/ 2>/dev/null | grep -v '\.partial/$' | sort | tail -1 || true)"
  [[ -n "${dir}" ]] || { echo "[FAIL] no completed backups in ${BACKUP_ROOT}" >&2; exit 1; }
fi
dir="${dir%/}"
[[ "${dir}" != *.partial ]] || { echo "[FAIL] ${dir} is an incomplete backup" >&2; exit 1; }
manifest="${dir}/manifest.txt"
[[ -f "${manifest}" ]] || { echo "[FAIL] ${manifest} not found" >&2; exit 1; }
value() { sed -n "s/^$1=//p" "${manifest}"; }

project="$("${COMPOSE[@]}" config | sed -n 's/^name: //p' | head -1)"
[[ -n "${project}" ]] || { echo "[FAIL] cannot determine the compose project name" >&2; exit 1; }
echo "==> restore ${dir} into clean shards of compose project ${project}"
# Подтверждение только явным флагом: без него отказ и в терминале (вопроса «yes?» нет, случайный Enter ничего не удалит)
if [[ "${confirm}" != "yes" ]]; then
  echo "[FAIL] not confirmed: data of ${project} shards and replicas would be DELETED (use --yes, make shard-restore CONFIRM=yes)" >&2
  exit 1
fi

# Проверки до остановки чего-либо: файлы целы и совпадают с manifest
ids=()
for i in "${SHARDS[@]}"; do
  [[ -f "${dir}/shard_${i}.dump" ]] || { echo "[FAIL] ${dir}/shard_${i}.dump not found" >&2; exit 1; }
  expected="$(value "shard_${i}.sha256")"
  actual="$(sha256 "${dir}/shard_${i}.dump")"
  [[ -n "${expected}" && "${expected}" == "${actual}" ]] \
    || { echo "[FAIL] shard_${i}.dump: SHA-256 differs from manifest" >&2; exit 1; }
  read -r -a sample <<<"$(value "shard_${i}.sample_device_ids")"
  if [[ ${#sample[@]} -gt 0 ]]; then ids+=("${sample[@]}"); fi
done
echo "==> SHA-256 of dumps matches manifest"

# Возраст копии против срока хранения темы devices (FR-022, ADR-0011): сообщения старше
# KAFKA_DEVICES_RETENTION_MS тема удаляет, и изменения, сделанные после копии и уже удалённые из темы,
# восстановление не вернёт (смещение группы вне диапазона темы: device-collector-service с auto-offset-reset=none
# остановит обработчик, health DOWN). Здесь только предупреждение - до остановки сервисов
# Время копии (UTC, формат created_at из manifest) в секундах эпохи; пусто, если не разобрать.
# BSD date (macOS) и GNU/busybox date (alpine) разные: BSD пробуем первым (в GNU у него нет -j)
stamp_epoch() {
  local s="$1"
  date -u -j -f '%Y%m%dT%H%M%SZ' "${s}" +%s 2>/dev/null \
    || date -u -d "${s:0:4}-${s:4:2}-${s:6:2} ${s:9:2}:${s:11:2}:${s:13:2}" +%s 2>/dev/null \
    || true
}
retention_ms="$(env_value KAFKA_DEVICES_RETENTION_MS)"
[[ "${retention_ms}" =~ ^[0-9]+$ ]] \
  || { echo "[FAIL] KAFKA_DEVICES_RETENTION_MS is not set in .env (see .env.example)" >&2; exit 1; }
created="$(value created_at)"
created_epoch=""
[[ "${created}" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] && created_epoch="$(stamp_epoch "${created}")"
if [[ -z "${created_epoch}" ]]; then
  echo "[WARN] manifest has no readable created_at (${created:-empty}): age of the copy vs devices retention is unknown" >&2
  echo "       the topic keeps messages for $((retention_ms / 1000)) s: changes made after the copy may be already deleted" >&2
else
  age=$(( $(date -u +%s) - created_epoch ))
  if (( age * 1000 > retention_ms )); then
    echo "[WARN] copy ${created} is older than KAFKA_DEVICES_RETENTION_MS ($((retention_ms / 1000)) s, copy age ${age} s)" >&2
    echo "       the devices topic may have deleted messages (admin commands and router events) made after the copy: restore will not bring them back" >&2
    echo "       if so, device-collector-service stops its listener (committed offset out of range, health DOWN) instead of skipping them" >&2
  fi
fi

# Позиции группы из manifest должны покрывать все разделы темы: пустой список (копия старой версии скрипта)
# или другое число разделов - отказ до остановки чего-либо, а не молчаливое оставление текущих offset
group="$(value kafka.group)"
topic="$(value kafka.topic)"
offsets="$(value kafka.offsets)"
[[ -n "${group}" && -n "${topic}" && -n "${offsets}" ]] \
  || { echo "[FAIL] manifest has no Kafka positions of the group (copy made by an older version of shard-backup.sh): make a new copy" >&2; exit 1; }
[[ -n "$("${COMPOSE[@]}" ps -q --status running kafka)" ]] \
  || { echo "[FAIL] kafka is not running: positions of ${group} cannot be returned (make up)" >&2; exit 1; }
current_positions="$(group_positions "${group}" "${topic}")" || exit 1
[[ "$(position_partitions "${offsets}")" == "$(position_partitions "${current_positions}")" ]] \
  || { echo "[FAIL] partitions of ${topic} differ: manifest $(position_partitions "${offsets}"), Kafka $(position_partitions "${current_positions}")" >&2; exit 1; }

# С этого места стенд меняется: при сбое - что осталось остановленным и как повторить (копия не тронута)
stopped=""
on_exit() {
  local rc=$?
  [[ -z "${tmp_dir}" ]] || rm -rf "${tmp_dir}"
  if [[ ${rc} -ne 0 && -n "${stopped}" ]]; then
    echo "[FAIL] restore interrupted: shardingsphere-proxy and device-collector-service may be stopped," >&2
    echo "       shards or replicas may be empty. The backup is intact - repeat: make shard-restore CONFIRM=yes BACKUP=${dir}" >&2
  fi
  exit "${rc}"
}
trap on_exit EXIT

echo "==> stop device-collector-service and shardingsphere-proxy"
stopped="yes"
quiet "${COMPOSE[@]}" stop device-collector-service shardingsphere-proxy

for i in "${SHARDS[@]}"; do
  shard="shard_${i}"
  echo "==> ${shard}: clean shard and replica"
  quiet "${COMPOSE[@]}" rm -sf "postgres-shard-${i}-replica" "postgres-shard-${i}"
  # Тома может не быть (шард ещё не поднимался); остаток данных поймает проверка пустой базы ниже
  docker volume rm "${project}_postgres-shard-${i}-replica-data" "${project}_postgres-shard-${i}-data" > /dev/null 2>&1 || true
  quiet "${COMPOSE[@]}" up -d --wait --no-deps "postgres-shard-${i}"
  [[ "$(primary_sql "${i}" "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")" == "0" ]] \
    || { echo "[FAIL] ${shard}: new container is not empty" >&2; exit 1; }
  echo "==> ${shard}: pg_restore"
  "${COMPOSE[@]}" exec -T "postgres-shard-${i}" sh -c \
    'pg_restore -U "$POSTGRES_USER" -d "shard_$1" --no-owner --no-privileges --exit-on-error --single-transaction' sh "${i}" \
    < "${dir}/${shard}.dump"
  for table in devices processed_device_events; do
    expected="$(value "${shard}.${table}")"
    actual="$(primary_sql "${i}" "SELECT count(*) FROM ${table}")"
    [[ "${actual}" == "${expected}" ]] || { echo "[FAIL] ${shard}.${table}: ${actual} rows, manifest ${expected}" >&2; exit 1; }
  done
  version="$(primary_sql "${i}" "SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank DESC LIMIT 1")"
  [[ "${version}" == "$(value "${shard}.schema_version")" ]] || { echo "[FAIL] ${shard}: schema V${version}" >&2; exit 1; }
  echo "    devices=$(value "${shard}.devices") processed_device_events=$(value "${shard}.processed_device_events") schema=V${version}: match manifest"
done

# Роль replicator и слоты были в удалённых томах шардов: создаются заново, затем реплики снимают новый pg_basebackup
echo "==> replicas: replicator role, slots, new pg_basebackup"
quiet "${COMPOSE[@]}" run -T --rm --no-deps shard-replication-init
for i in "${SHARDS[@]}"; do
  grant_reader_access "${i}" || exit 1
  quiet "${COMPOSE[@]}" up -d --wait --no-deps "postgres-shard-${i}-replica"
done
for i in "${SHARDS[@]}"; do
  deadline=$(( $(date +%s) + LAG_TIMEOUT ))
  until require_replica_streaming "${i}" 2>/dev/null; do
    [[ $(date +%s) -ge ${deadline} ]] && { require_replica_streaming "${i}"; exit 1; }
    sleep 1
  done
  wait_replica_caught_up "${i}" > /dev/null
  replica_devices="$(replica_sql "${i}" "SELECT count(*) FROM devices")"
  [[ "${replica_devices}" == "$(value "shard_${i}.devices")" ]] \
    || { echo "[FAIL] shard_${i}: replica has ${replica_devices} devices, manifest $(value "shard_${i}.devices")" >&2; exit 1; }
  echo "    shard_${i}: replica streaming, devices=${replica_devices}"
done

# Позиции группы (все разделы, в том числе без коммитов к моменту копии) - к моменту копии. Группа должна быть
# неактивной: сервис остановлен, но брокер снимает участника с группы не мгновенно - ждём
echo "==> Kafka: group ${group}, ${topic} positions back to the backup point (${offsets})"
tr ',' '\n' <<<"${offsets}" | awk -F: -v topic="${topic}" '{ print topic "," $1 "," $2 }' \
  | "${COMPOSE[@]}" exec -T kafka sh -c 'cat > /tmp/restore-offsets.csv'
deadline=$(( $(date +%s) + 60 ))
until reset_out="$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "kafka:${KAFKA_PORT}" \
    --group "${group}" --reset-offsets --from-file /tmp/restore-offsets.csv --execute < /dev/null 2>&1)"; do
  [[ $(date +%s) -ge ${deadline} ]] && { echo "[FAIL] offsets of ${group} were not reset (group still active?):" >&2; echo "${reset_out}" >&2; exit 1; }
  sleep 3
done
# Сверка: Kafka показывает ровно те позиции, что записаны в manifest
after="$(group_positions "${group}" "${topic}")" || exit 1
[[ "${after}" == "${offsets}" ]] || { echo "[FAIL] positions of ${group} after reset: ${after}, manifest ${offsets}" >&2; exit 1; }
echo "    positions of ${group} match manifest"

echo "==> start shardingsphere-proxy (reloads table metadata)"
quiet "${COMPOSE[@]}" start shardingsphere-proxy
wait_healthy shardingsphere-proxy

if [[ ${#ids[@]} -eq 0 ]]; then
  echo "==> backup has no devices: nothing to read back"
else
  echo "==> read restored devices through ShardingSphere-Proxy"
  proxy_sql="${ROOT_DIR}/infrastructure/config/shardingsphere/proxy-sql.sh"
  for id in "${ids[@]}"; do
    found="$("${proxy_sql}" -tA -c "SELECT count(*) FROM devices WHERE device_id = '${id}'")"
    [[ "${found}" == "1" ]] || { echo "[FAIL] device ${id} is not readable through Proxy (count=${found})" >&2; exit 1; }
  done
  echo "    ${#ids[@]} devices read through Proxy"
  # Тем же путём читает device-service: база devices_read (реплики) от имени device_reader
  echo "==> read restored devices through ShardingSphere-Proxy as the device-service reader (devices_read)"
  proxy_read_sql="${ROOT_DIR}/infrastructure/config/shardingsphere/proxy-read-sql.sh"
  for id in "${ids[@]}"; do
    found="$("${proxy_read_sql}" -tA -c "SELECT count(*) FROM devices WHERE device_id = '${id}'" 2>&1)" || found="${found:-error}"
    [[ "${found}" == "1" ]] || { echo "[FAIL] device ${id} is not readable through devices_read (${found})" >&2; exit 1; }
  done
  echo "    ${#ids[@]} devices read through devices_read"
fi

echo "==> start device-collector-service"
quiet "${COMPOSE[@]}" start device-collector-service
wait_healthy device-collector-service
stopped=""
echo "==> Restore done from ${dir}"