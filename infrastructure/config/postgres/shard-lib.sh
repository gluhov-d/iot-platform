#!/usr/bin/env bash
# Общие функции shard-backup.sh и shard-restore.sh (подключается через source).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# COMPOSE_FILE из окружения - другой compose-файл (например, тестового стенда)
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${COMPOSE_FILE:-${ROOT_DIR}/infrastructure/docker-compose.yaml}")
BACKUP_ROOT="${BACKUP_ROOT:-${ROOT_DIR}/backups}"
SHARDS=(0 1)
# Сколько секунд ждать, пока реплика применит WAL шарда
LAG_TIMEOUT="${BACKUP_LAG_TIMEOUT:-60}"

[[ -f "${ROOT_DIR}/.env" ]] || { echo "[FAIL] ${ROOT_DIR}/.env not found: cp .env.example .env" >&2; exit 1; }

# Значения из .env в том виде, как их разобрал Compose
env_dump="$("${COMPOSE[@]}" config --environment)"
env_value() { sed -n "s/^$1=//p" <<<"${env_dump}"; }
DEVICES_TOPIC="$(env_value KAFKA_DEVICES_TOPIC)"; DEVICES_TOPIC="${DEVICES_TOPIC:-devices}"
CONSUMER_GROUP="$(env_value DEVICE_COLLECTOR_CONSUMER_GROUP)"; CONSUMER_GROUP="${CONSUMER_GROUP:-device-collector}"
KAFKA_PORT="$(env_value KAFKA_INTERNAL_PORT)"; KAFKA_PORT="${KAFKA_PORT:-9092}"

# Инструмент Kafka в контейнере kafka: $1 - имя без .sh, дальше его аргументы. Код возврата и сообщения об ошибках
# не скрываются: недоступный брокер не должен выглядеть как «у группы нет коммитов»
kafka_tool() {
  local tool="$1"; shift
  "${COMPOSE[@]}" exec -T kafka "/opt/kafka/bin/${tool}.sh" --bootstrap-server "kafka:${KAFKA_PORT}" "$@" < /dev/null
}

# Позиции группы $1 по КАЖДОМУ разделу темы $2: раздел:offset,раздел:offset,...
# Раздел с коммитом - подтверждённый offset; раздел без коммита - его начало: consumer без коммита читает с earliest
# (auto-offset-reset), поэтому «нет коммита» тоже позиция, к которой нужно вернуться при восстановлении.
# Группы, которой нет в Kafka (коммитов не было нигде), - все разделы с начала.
group_positions() {
  local group="$1" topic="$2" starts groups committed=""
  starts="$(kafka_tool kafka-get-offsets --topic "${topic}" --time -2)" \
    || { echo "[FAIL] cannot read partitions of ${topic} from Kafka" >&2; return 1; }
  groups="$(kafka_tool kafka-consumer-groups --list)" \
    || { echo "[FAIL] cannot list consumer groups in Kafka" >&2; return 1; }
  if grep -qx -- "${group}" <<<"${groups}"; then
    committed="$(kafka_tool kafka-consumer-groups --describe --group "${group}")" \
      || { echo "[FAIL] cannot read offsets of group ${group} from Kafka" >&2; return 1; }
  fi
  { sed 's/^/C /' <<<"${committed}"; sed 's/^/S /' <<<"${starts}"; } | awk -v topic="${topic}" '
    # C: строка --describe (GROUP TOPIC PARTITION CURRENT-OFFSET ...), "-" в CURRENT-OFFSET - коммита нет
    $1 == "C" && $3 == topic && $4 ~ /^[0-9]+$/ && $5 ~ /^[0-9]+$/ { committed[$4] = $5 }
    # S: строка kafka-get-offsets (тема:раздел:начало)
    $1 == "S" { split($2, f, ":"); if (f[1] == topic && f[2] ~ /^[0-9]+$/ && f[3] ~ /^[0-9]+$/) { start[f[2]] = f[3]; if (f[2] + 0 > max) max = f[2] + 0; seen = 1 } }
    END {
      if (!seen) exit 1
      for (p = 0; p <= max; p++) {
        if (!(p in start)) exit 1
        printf "%s%s:%s", sep, p, (p in committed) ? committed[p] : start[p]; sep = ","
      }
    }' || { echo "[FAIL] cannot determine positions of group ${group} in ${topic}" >&2; return 1; }
}

# Номера разделов из списка позиций (раздел:offset,...) через запятую, по возрастанию
position_partitions() { tr ',' '\n' <<<"$1" | cut -d: -f1 | sort -n | paste -sd, -; }

# Команда без шума в выводе; при ошибке её вывод печатается, код возврата сохраняется
quiet() {
  local out
  out="$("$@" 2>&1)" || { local rc=$?; printf '%s\n' "${out}" >&2; return "${rc}"; }
}

# SQL в контейнере PostgreSQL (шард или реплика) по сокету: учётная запись - из окружения контейнера
# $1 - контейнер, $2 - номер шарда (имя базы shard_N), $3 - запрос
sql_in() {
  "${COMPOSE[@]}" exec -T "$1" sh -c \
    'psql -U "$POSTGRES_USER" -d "shard_$1" -tA -v ON_ERROR_STOP=1 -c "$2"' sh "$2" "$3" < /dev/null
}
primary_sql() { sql_in "postgres-shard-$1" "$1" "$2"; }
replica_sql() { sql_in "postgres-shard-$1-replica" "$1" "$2"; }

sha256() { if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

# Ждёт healthy у контейнеров сервиса
wait_healthy() {
  local service="$1" deadline=$(( $(date +%s) + ${2:-120} )) ids status=""
  while true; do
    ids="$("${COMPOSE[@]}" ps -q "${service}")"
    if [[ -n "${ids}" ]]; then
      status="$(docker inspect -f '{{.State.Health.Status}}' ${ids} | sort -u | tr '\n' ' ')"
      [[ "${status}" == "healthy " ]] && return 0
    fi
    [[ $(date +%s) -ge ${deadline} ]] && { echo "[FAIL] ${service} is not healthy: ${status:-no containers}" >&2; return 1; }
    sleep 2
  done
}

# Реплика шарда $1 применила всю WAL, которую шард записал к этому моменту.
# Без этой проверки копия с реплики могла бы оказаться старее offset, зафиксированных в manifest.
wait_replica_caught_up() {
  local n="$1" lsn deadline caught
  lsn="$(primary_sql "${n}" "SELECT pg_current_wal_lsn()")"
  deadline=$(( $(date +%s) + LAG_TIMEOUT ))
  while true; do
    caught="$(replica_sql "${n}" "SELECT coalesce(pg_last_wal_replay_lsn() >= '${lsn}'::pg_lsn, false)" 2>/dev/null || true)"
    [[ "${caught}" == "t" ]] && { echo "${lsn}"; return 0; }
    [[ $(date +%s) -ge ${deadline} ]] && {
      echo "[FAIL] shard_${n}: replica did not replay WAL up to ${lsn} in ${LAG_TIMEOUT}s (make replication-status)" >&2
      return 1
    }
    sleep 1
  done
}

# Реплика шарда $1 в режиме standby и получает WAL
require_replica_streaming() {
  local n="$1" recovery receiver
  recovery="$(replica_sql "${n}" "SELECT pg_is_in_recovery()" 2>/dev/null || true)"
  receiver="$(replica_sql "${n}" "SELECT status FROM pg_stat_wal_receiver" 2>/dev/null || true)"
  [[ "${recovery}" == "t" && "${receiver}" == "streaming" ]] || {
    echo "[FAIL] shard_${n}: replica postgres-shard-${n}-replica is not streaming (recovery=${recovery:-?}, receiver=${receiver:-?}): make replication-status" >&2
    return 1
  }
}

# --- Содержимое копии читается из самого файла: manifest описывает копию, а не живую базу ---
# (на момент копии сервис мог дописать строки, и число строк в базе уже другое)

# Данные таблицы $2 из копии $1 в формате COPY (текст)
dump_table_data() {
  "${COMPOSE[@]}" exec -T "${3:-postgres-shard-0}" pg_restore --data-only --table="$2" --file=- < "$1"
}
# Число строк таблицы в копии
dump_rows() {
  dump_table_data "$1" "$2" "$3" | awk 'c && /^\\\.$/ { c = 0; next } c { n++; next } /^COPY / { c = 1 } END { print n + 0 }'
}
# До 5 deviceId из копии (только безопасные для командной строки символы)
dump_sample_ids() {
  dump_table_data "$1" devices "$2" | awk -F'\t' '
    c && /^\\\.$/ { c = 0; next }
    c { if ($1 ~ /^[A-Za-z0-9._:-]+$/ && k < 5) { ids = ids sep $1; sep = " "; k++ } next }
    /^COPY / { c = 1 }
    END { print ids }'
}
# Версия схемы в копии - последняя успешная миграция Flyway
dump_schema_version() {
  dump_table_data "$1" flyway_schema_history "$2" | awk -F'\t' '
    c && /^\\\.$/ { c = 0; next }
    c { if ($10 == "t" && $1 + 0 >= rank) { rank = $1 + 0; v = $2 } next }
    /^COPY / { c = 1 }
    END { print v }'
}