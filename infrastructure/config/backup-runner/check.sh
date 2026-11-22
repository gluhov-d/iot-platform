#!/usr/bin/env bash
# Проверка копий баз в MinIO на поднятом стенде (make backup-check CONFIRM=yes): копии PostgreSQL и ClickHouse,
# расписание, блокировка двух одновременных копий, восстановление из MinIO. Код 1, если проверка не прошла.
#
# ВНИМАНИЕ: после копии данные реестра устройств (шарды) и база ClickHouse проекта compose УДАЛЯЮТСЯ и
# восстанавливаются из копии; device-collector-service на это время останавливается. Запускать только на своём
# проекте compose (COMPOSE_PROJECT_NAME), не на стенде с нужными данными. Без CONFIRM=yes скрипт отказывается.
#
# Запуск через make backup-check (COMPOSE_CMD - команда compose с --env-file и -f). Что проверяется (R18, FR-020,
# FR-021, SC-004):
#   1. образ backup-runner запускается на этой архитектуре: pg_dump, mc;
#   2. make backup: в backups/postgres/<метка>/ и backups/clickhouse/<метка>/ появились копии с manifest.txt;
#      SHA-256 дампов PostgreSQL в MinIO равны manifest;
#   3. расписание: при BACKUP_SCHEDULE="* * * * *" crond сам делает обе копии, а журнал backup-runner не пуст;
#   4. flock: два одновременных backup-postgres.sh - одна копия получается, второй запуск отказывается (код не 0);
#   5. после удаления данных: make shard-restore FROM=minio BACKUP=<метка> CONFIRM=yes возвращает число устройств
#      каждого шарда из manifest и позиции группы device-collector (их перед этим сдвигает сама проверка);
#      make clickhouse-restore BACKUP=<метка> возвращает число строк таблиц из manifest.
#
# Договорённости, которые обязана выполнять реализация T061-T066 (проверка написана раньше кода):
#   - контейнер backup-runner: скрипты в /opt/backup-runner/ (backup-postgres.sh, backup-clickhouse.sh), /etc/backup.env
#     с переменными контейнера (BACKUP_*_S3_*, MINIO_BUCKET_BACKUPS), mc и pg_dump в PATH;
#   - manifest.txt лежит рядом с копией; у ClickHouse в нём строки clickhouse.<таблица>.rows=<N>
#     (device_events, device_outbox, device_publication_audit), у PostgreSQL - формат shard-backup.sh;
#   - цели make: backup, shard-restore FROM=minio BACKUP=<метка>, clickhouse-restore BACKUP=<метка>.
# Ключи MinIO берутся из окружения контейнера backup-runner и в вывод не попадают.
set -u

: "${COMPOSE_CMD:?запускайте через make backup-check}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
MAKE_CMD="${MAKE:-make}"
SCHEDULE_TIMEOUT="${BACKUP_CHECK_SCHEDULE_TIMEOUT:-240}"
RUNNER_DIR=/opt/backup-runner
CH_TABLES="device_events device_outbox device_publication_audit"

if [ "${BACKUP_CHECK_CONFIRM:-}" != yes ]; then
  echo "FAIL  не подтверждено: данные шардов и ClickHouse проекта compose будут УДАЛЕНЫ и восстановлены (make backup-check CONFIRM=yes)"
  exit 1
fi

# shellcheck disable=SC2086
compose() { $COMPOSE_CMD "$@"; }
# shellcheck source=../postgres/shard-lib.sh
. "${ROOT_DIR}/infrastructure/config/postgres/shard-lib.sh"
PROXY_SQL="${ROOT_DIR}/infrastructure/config/shardingsphere/proxy-sql.sh"

ok=0
bad=0
pass() { ok=$((ok + 1)); echo "PASS  $1"; }
fail() { bad=$((bad + 1)); echo "FAIL  $1"; }
# expect ОПИСАНИЕ ОЖИДАЕМОЕ ФАКТИЧЕСКОЕ
expect() { if [ "$2" = "$3" ]; then pass "$1: $3"; else fail "$1: ожидали «$2», получили «$3»"; fi; }
finish() {
  echo "== итог: PASS ${ok}, FAIL ${bad}"
  [ "${bad}" -eq 0 ]
  exit $?
}
# Дальше без смысла: ошибка подготовки, а не проверяемого свойства
abort() { fail "$1"; finish; }

# Команда в контейнере backup-runner: учётки MinIO только из /etc/backup.env, алиасы mc: bpg (PostgreSQL), bch (ClickHouse)
RUNNER_PRELUDE='set -a; . /etc/backup.env; set +a
MC_HOST_bpg="http://${BACKUP_POSTGRES_S3_ACCESS_KEY}:${BACKUP_POSTGRES_S3_SECRET_KEY}@minio:9000"
MC_HOST_bch="http://${BACKUP_CLICKHOUSE_S3_ACCESS_KEY}:${BACKUP_CLICKHOUSE_S3_SECRET_KEY}@minio:9000"
export MC_HOST_bpg MC_HOST_bch; B="${MINIO_BUCKET_BACKUPS}"'
in_runner() { local s="$1"; shift; compose exec -T backup-runner sh -c "${RUNNER_PRELUDE}"'
'"${s}" sh "$@" < /dev/null; }

# Метки завершённых копий (алиас, каталог) по возрастанию; *.partial не считаются
labels() {
  in_runner 'mc ls "$1/$B/$2/" 2>/dev/null | awk "{print \$NF}" | tr -d / | sort' "$1" "$2" \
    | grep -E '^[0-9]{8}T[0-9]{6}Z$' || true
}
last_label() { labels "$1" "$2" | tail -1; }
count_labels() { labels "$1" "$2" | grep -c . || true; }
# Объект копии (алиас, путь внутри бакета)
cat_object() { in_runner 'mc cat "$1/$B/$2"' "$1" "$2"; }
manifest_value() { sed -n "s/^$2=//p" <<<"$1" | head -1; }

ch_query() {
  compose exec -T clickhouse sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "$1"' sh "$1" < /dev/null
}
ch_rows() { ch_query "SELECT count() FROM \`${CH_DB}\`.$1" 2>/dev/null | tr -d '[:space:]'; }

echo "== подготовка"
running="$(compose ps --services --status running 2>/dev/null | tr '\n' ' ')"
for s in backup-runner clickhouse minio kafka shardingsphere-proxy postgres-shard-0 postgres-shard-1; do
  case " ${running} " in *" ${s} "*) ;; *) abort "${s} не запущен: make up" ;; esac
done

CH_DB="$(compose exec -T clickhouse sh -c 'printf %s "$CLICKHOUSE_DB"' < /dev/null 2>/dev/null)"

echo "== 1. образ backup-runner на этой архитектуре"
arch="$(in_runner 'uname -m')"
pgv="$(in_runner 'pg_dump --version' 2>&1)" && pass "pg_dump (${arch}): ${pgv}" || fail "pg_dump в backup-runner: ${pgv}"
mcv="$(in_runner 'mc --version' 2>&1 | head -1)"
case "${mcv}" in *mc*) pass "mc (${arch}): ${mcv}" ;; *) fail "mc в backup-runner: ${mcv}" ;; esac
in_runner 'test -r /etc/backup.env && test -x '"${RUNNER_DIR}"'/backup-postgres.sh && test -x '"${RUNNER_DIR}"'/backup-clickhouse.sh' \
  && pass "/etc/backup.env и скрипты копий на месте" || abort "нет /etc/backup.env или ${RUNNER_DIR}/backup-*.sh"

echo "== 2. make backup"
pg_before="$(count_labels bpg postgres)"
ch_before="$(count_labels bch clickhouse)"
"${MAKE_CMD}" -C "${ROOT_DIR}" backup > /tmp/backup-check-backup.log 2>&1 \
  && pass "make backup завершился успешно" || { tail -20 /tmp/backup-check-backup.log; abort "make backup упал"; }
expect "копий PostgreSQL прибавилось" "$((pg_before + 1))" "$(count_labels bpg postgres)"
expect "копий ClickHouse прибавилось" "$((ch_before + 1))" "$(count_labels bch clickhouse)"

pg_label="$(last_label bpg postgres)"
ch_label="$(last_label bch clickhouse)"
pg_manifest="$(cat_object bpg "postgres/${pg_label}/manifest.txt" 2>/dev/null)"
ch_manifest="$(cat_object bch "clickhouse/${ch_label}/manifest.txt" 2>/dev/null)"
[ -n "${pg_manifest}" ] && pass "manifest PostgreSQL ${pg_label}" || abort "нет postgres/${pg_label}/manifest.txt"
[ -n "${ch_manifest}" ] && pass "manifest ClickHouse ${ch_label}" || abort "нет clickhouse/${ch_label}/manifest.txt"
for i in "${SHARDS[@]}"; do
  want="$(manifest_value "${pg_manifest}" "shard_${i}.sha256")"
  got="$(in_runner 'mc cat "$1/$B/$2" | sha256sum | cut -d" " -f1' bpg "postgres/${pg_label}/shard_${i}.dump")"
  if [ -n "${want}" ]; then expect "SHA-256 shard_${i}.dump в MinIO равен manifest" "${want}" "${got}"; else fail "в manifest нет shard_${i}.sha256"; fi
done
for t in ${CH_TABLES}; do
  [ -n "$(manifest_value "${ch_manifest}" "clickhouse.${t}.rows")" ] \
    && pass "manifest ClickHouse: clickhouse.${t}.rows" || fail "manifest ClickHouse: нет clickhouse.${t}.rows"
done

echo "== 3. расписание (BACKUP_SCHEDULE=\"* * * * *\", до ${SCHEDULE_TIMEOUT} с)"
pg_before="$(count_labels bpg postgres)"
ch_before="$(count_labels bch clickhouse)"
BACKUP_SCHEDULE='* * * * *' compose up -d --force-recreate --no-deps backup-runner > /dev/null 2>&1 \
  && pass "backup-runner пересоздан с расписанием каждую минуту" || abort "не удалось пересоздать backup-runner"
deadline=$(( $(date +%s) + SCHEDULE_TIMEOUT ))
while [ "$(date +%s)" -lt "${deadline}" ]; do
  [ "$(count_labels bpg postgres)" -gt "${pg_before}" ] && [ "$(count_labels bch clickhouse)" -gt "${ch_before}" ] && break
  sleep 10
done
[ "$(count_labels bpg postgres)" -gt "${pg_before}" ] && pass "по расписанию появилась копия PostgreSQL" || fail "по расписанию копия PostgreSQL не появилась за ${SCHEDULE_TIMEOUT} с"
[ "$(count_labels bch clickhouse)" -gt "${ch_before}" ] && pass "по расписанию появилась копия ClickHouse" || fail "по расписанию копия ClickHouse не появилась за ${SCHEDULE_TIMEOUT} с"
[ -n "$(compose logs --no-color --tail 50 backup-runner 2>&1)" ] && pass "журнал backup-runner не пуст" || fail "журнал backup-runner пуст"
# Расписание возвращается к значению из .env до следующих шагов: копии по минутам мешали бы проверке блокировки
compose up -d --force-recreate --wait --no-deps backup-runner > /dev/null 2>&1 \
  && pass "backup-runner пересоздан с расписанием из .env" || abort "backup-runner не поднялся с расписанием из .env"

echo "== 4. flock: два одновременных backup-postgres.sh"
pg_before="$(count_labels bpg postgres)"
res="$(in_runner "${RUNNER_DIR}"'/backup-postgres.sh > /tmp/flock-1.log 2>&1 &
p1=$!
'"${RUNNER_DIR}"'/backup-postgres.sh > /tmp/flock-2.log 2>&1
r2=$?
wait $p1
r1=$?
echo "$r1 $r2"')"
r1="${res%% *}"
r2="${res##* }"
# Какой из запусков первый, не важно: ровно один успех и ровно один отказ
if { [ "${r1}" = 0 ] && [ "${r2}" != 0 ]; } || { [ "${r1}" != 0 ] && [ "${r2}" = 0 ]; }; then
  pass "ровно один из двух запусков отказал (коды ${r1} и ${r2})"
else
  fail "ожидали один успех и один отказ, коды: ${r1} и ${r2}"
fi
expect "копий PostgreSQL прибавилось ровно на одну" "$((pg_before + 1))" "$(count_labels bpg postgres)"

echo "== 5. восстановление из MinIO"
"${MAKE_CMD}" -C "${ROOT_DIR}" backup > /tmp/backup-check-backup.log 2>&1 \
  && pass "свежая пара копий для восстановления" || { tail -20 /tmp/backup-check-backup.log; abort "make backup упал"; }
pg_label="$(last_label bpg postgres)"
ch_label="$(last_label bch clickhouse)"
pg_manifest="$(cat_object bpg "postgres/${pg_label}/manifest.txt")"
ch_manifest="$(cat_object bch "clickhouse/${ch_label}/manifest.txt")"
offsets="$(manifest_value "${pg_manifest}" kafka.offsets)"
group="$(manifest_value "${pg_manifest}" kafka.group)"
topic="$(manifest_value "${pg_manifest}" kafka.topic)"
[ -n "${offsets}" ] && [ -n "${group}" ] && [ -n "${topic}" ] || abort "в manifest PostgreSQL ${pg_label} нет kafka.group/topic/offsets"

# Позиции группы сдвигаются, чтобы возврат был различим: сначала на начало разделов, иначе на конец
compose stop device-collector-service > /dev/null 2>&1
moved=""
for mode in --to-earliest --to-latest; do
  deadline=$(( $(date +%s) + 60 ))
  until kafka_tool kafka-consumer-groups --group "${group}" --topic "${topic}" --reset-offsets "${mode}" --execute > /dev/null 2>&1; do
    [ "$(date +%s)" -ge "${deadline}" ] && break
    sleep 3
  done
  now="$(group_positions "${group}" "${topic}" 2>/dev/null)"
  if [ -n "${now}" ] && [ "${now}" != "${offsets}" ]; then moved="${now}"; break; fi
done
[ -n "${moved}" ] && pass "позиции группы сдвинуты: ${moved} (в manifest ${offsets})" \
  || abort "позиции группы нельзя отличить от manifest (${offsets}): отправьте события в ${topic} (make dcs-quick-check) и повторите"

# Удаление данных: реестр устройств (через Proxy) и база ClickHouse
"${PROXY_SQL}" -tA -c "DELETE FROM processed_device_events" > /dev/null 2>&1
"${PROXY_SQL}" -tA -c "DELETE FROM devices" > /dev/null 2>&1
expect "устройств в реестре после удаления" 0 "$("${PROXY_SQL}" -tA -c "SELECT count(*) FROM devices" 2>/dev/null | tr -d '[:space:]')"
ch_query "DROP DATABASE IF EXISTS \`${CH_DB}\` SYNC" > /dev/null 2>&1
expect "база ClickHouse удалена, таблицы читаются с ошибкой" "" "$(ch_rows device_events)"

echo "-- make shard-restore FROM=minio BACKUP=${pg_label}"
"${MAKE_CMD}" -C "${ROOT_DIR}" shard-restore FROM=minio BACKUP="${pg_label}" CONFIRM=yes > /tmp/backup-check-shard-restore.log 2>&1 \
  && pass "make shard-restore FROM=minio завершился успешно" || { tail -30 /tmp/backup-check-shard-restore.log; fail "make shard-restore FROM=minio упал"; }
for i in "${SHARDS[@]}"; do
  expect "shard_${i}: устройств равно manifest" "$(manifest_value "${pg_manifest}" "shard_${i}.devices")" "$(primary_sql "${i}" "SELECT count(*) FROM devices" 2>/dev/null)"
  expect "shard_${i}: processed_device_events равно manifest" "$(manifest_value "${pg_manifest}" "shard_${i}.processed_device_events")" "$(primary_sql "${i}" "SELECT count(*) FROM processed_device_events" 2>/dev/null)"
done
expect "позиции группы ${group} возвращены к manifest" "${offsets}" "$(group_positions "${group}" "${topic}" 2>/dev/null)"

echo "-- make clickhouse-restore BACKUP=${ch_label}"
"${MAKE_CMD}" -C "${ROOT_DIR}" clickhouse-restore BACKUP="${ch_label}" > /tmp/backup-check-ch-restore.log 2>&1 \
  && pass "make clickhouse-restore завершился успешно" || { tail -30 /tmp/backup-check-ch-restore.log; fail "make clickhouse-restore упал"; }
for t in ${CH_TABLES}; do
  expect "ClickHouse ${t}: строк равно manifest" "$(manifest_value "${ch_manifest}" "clickhouse.${t}.rows")" "$(ch_rows "${t}")"
done

finish
