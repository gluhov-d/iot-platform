#!/usr/bin/env bash
# Быстрая проверка локального стенда: device-collector-service сохраняет устройство на ожидаемом шарде
# (не замена интеграционным тестам)
#
# services/device-collector-service/scripts/quick-check.sh      (или make dcs-quick-check)
#
# Отправляет в тему devices два сообщения DeviceInfo - для устройства каждого шарда: идентификаторы подбираются
# по правилу шардирования (shard_N, N = |hashCode(deviceId) % 2|), поэтому проверка не зависит от того, куда
# Proxy отправляет запись, а сверяет её с правилом. Затем для каждого устройства:
#   - ждёт, пока сервис его сохранит (сообщение обрабатывается асинхронно): строка появляется на ожидаемом шарде;
#   - убеждается, что на другом шарде этого устройства нет;
#   - читает устройство через Proxy и через REST сервиса: ответ указывает тот же шард.
# Выход 0 - все в порядке ("All checks passed"), иначе номер упавшего шага в выводе.
#
# Все выполняется внутри контейнеров стенда (docker compose exec): на хосте нужны только docker и bash.
# Пароли берутся из окружения контейнеров, а не из командной строки. Устройства quick-check-* остаются в реестре.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Корневой .env передается явно: скрипт запускают из разных каталогов (make, Gradle, вручную),
# а без --env-file Compose ищет .env рядом с compose-файлом и без него подставляет пустые значения переменных
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${ROOT_DIR}/infrastructure/docker-compose.yaml")
PROXY_SQL="${ROOT_DIR}/infrastructure/config/shardingsphere/proxy-sql.sh"
TIMEOUT="${CHECK_TIMEOUT:-60}"
RUN_ID="$(date +%s)-${RANDOM}"
SHARDS=(0 1)

step=0
ok()   { printf '  [OK]   %s\n' "$1"; }
fail() { printf '  [FAIL] step %d: %s\n' "${step}" "$1" >&2; exit 1; }
next() { step=$((step + 1)); printf '%d. %s\n' "${step}" "$1"; }

# Ждет, пока команда не выведет ожидаемую строку, не дольше TIMEOUT секунд
wait_for() {
  local expected="$1"; shift
  local deadline=$(( $(date +%s) + TIMEOUT )) out
  while true; do
    out="$("$@" 2>/dev/null || true)"
    [[ "${out}" == "${expected}" ]] && return 0
    [[ $(date +%s) -ge ${deadline} ]] && { echo "    last response: '${out}'" >&2; return 1; }
    sleep 1
  done
}

# Шард устройства по правилу реестра: |hashCode(deviceId) % 2|. Чётность 32-битного hashCode не зависит от знака,
# поэтому достаточно посчитать его по модулю 2^32 (идентификаторы - ASCII)
shard_of() {
  local s="$1" h=0 i c
  for ((i = 0; i < ${#s}; i++)); do
    printf -v c '%d' "'${s:i:1}"
    h=$(( (h * 31 + c) & 0xFFFFFFFF ))
  done
  echo $(( h % 2 ))
}

# Идентификатор устройства, которое по правилу попадает на шард $1
device_for_shard() {
  local n="$1" i=0 id
  while true; do
    id="quick-check-${RUN_ID}-${i}"
    [[ "$(shard_of "${id}")" == "${n}" ]] && { echo "${id}"; return 0; }
    i=$((i + 1))
  done
}

# Число строк устройства прямо на шарде $1 (минуя Proxy): psql в контейнере шарда, учётная запись - из его окружения
rows_on_shard() {
  local sql="SELECT count(*) FROM devices WHERE device_id = '$2'"
  "${COMPOSE[@]}" exec -T "postgres-shard-$1" sh -c \
    'psql -U "$POSTGRES_USER" -d "shard_$1" -tA -v ON_ERROR_STOP=1 -c "$2"' sh "$1" "${sql}" < /dev/null | tr -d '[:space:]'
}

# Число строк устройства через Proxy (логическая база devices)
rows_via_proxy() { "${PROXY_SQL}" -tA -c "SELECT count(*) FROM devices WHERE device_id = '$1'" | tr -d '[:space:]'; }

# Шард из ответа REST сервиса: GET выполняется внутри контейнера (в образе есть только bash, curl нет)
shard_via_rest() {
  "${COMPOSE[@]}" exec -T device-collector-service bash -c \
    'exec 3<>/dev/tcp/127.0.0.1/8080; printf "GET /api/v1/devices/%s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n" "$1" >&3; cat <&3' \
    _ "$1" < /dev/null | tr -d '\r' | sed -n 's/.*"shard":"\([^"]*\)".*/\1/p'
}

# Схема DeviceInfo для консольного продюсера: тип DeviceSnapshot объявлен в отдельном файле,
# поэтому ссылка на него по имени заменяется самим объявлением
SNAPSHOT_SCHEMA="$(tr -d '\n' < "${ROOT_DIR}/avros/avro/device-snapshot.avsc")"
INFO_SCHEMA="$(tr -d '\n' < "${ROOT_DIR}/avros/avro/device-info.avsc")"
before="${INFO_SCHEMA%%\"DeviceSnapshot\"*}"
after="${INFO_SCHEMA#*\"DeviceSnapshot\"}"
SCHEMA="${before}${SNAPSHOT_SCHEMA}${after}"

# $1 - deviceId: DeviceInfo CREATED со снимком в Avro JSON (значения union-полей - в обёртке с именем типа)
send_device_info() {
  local id="$1" now
  now="$(( $(date +%s) * 1000 ))"
  local snapshot="{\"com.github.gluhov.iotplatform.events.avro.DeviceSnapshot\":{\"deviceId\":\"${id}\",\"tenantId\":\"check-tenant\",\"ownerId\":null,\"deviceType\":\"check-sensor\",\"serialNumber\":null,\"manufacturer\":null,\"model\":null,\"firmwareVersion\":{\"string\":\"1.0.0\"},\"name\":{\"string\":\"quick-check\"},\"osName\":{\"string\":\"Linux\"},\"osVersion\":{\"string\":\"6.1\"},\"status\":\"ACTIVE\",\"registeredAtMs\":null,\"lastSeenAtMs\":{\"long\":${now}},\"settings\":{\"mode\":\"check\"},\"sensors\":[],\"attributes\":{}}}"
  printf '%s|%s\n' "${id}" "{\"deviceId\":\"${id}\",\"emittedAtMs\":${now},\"eventType\":\"CREATED\",\"eventId\":{\"string\":\"quick-check-${id}\"},\"eventTimeMs\":{\"long\":${now}},\"device\":${snapshot}}" \
    | "${COMPOSE[@]}" exec -T schema-registry \
      kafka-avro-console-producer \
        --bootstrap-server kafka:9092 \
        --topic devices \
        --property schema.registry.url=http://localhost:8081 \
        --property value.schema="${SCHEMA}" \
        --property auto.register.schemas=false \
        --property use.latest.version=true \
        --property parse.key=true \
        --property key.separator='|' \
        --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
      >/dev/null 2>&1
}

echo "==> device-collector-service: local stand quick check"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  printf '  [FAIL] %s not found: cp .env.example .env\n' "${ROOT_DIR}/.env" >&2
  exit 1
fi

# ------------------------------------------------------------------------------
next "Service, ShardingSphere-Proxy and both shards are running and healthy"
for service in device-collector-service shardingsphere-proxy postgres-shard-0 postgres-shard-1; do
  # || state="": при ошибке Compose (pipefail) скрипт иначе завершился бы молча, без сообщения о шаге
  state="$("${COMPOSE[@]}" ps --format '{{.Health}}' "${service}" 2>/dev/null | sort -u | paste -sd' ' -)" || state=""
  [[ "${state}" == "healthy" ]] || fail "${service} is not healthy (state: '${state:-not running}'); see docker compose ps"
done
ok "all four containers healthy"

# ------------------------------------------------------------------------------
next "Pick one device per shard by the sharding rule and send DeviceInfo to topic devices (Avro + Schema Registry, key deviceId)"
device=()   # индексный массив: ассоциативные (declare -A) в bash 3.2 на macOS недоступны
for n in "${SHARDS[@]}"; do
  device[${n}]="$(device_for_shard "${n}")"
  echo "    shard_${n}: ${device[${n}]}"
  send_device_info "${device[${n}]}" || fail "failed to send message (are Kafka and Schema Registry available?)"
done
ok "2 messages sent"

# ------------------------------------------------------------------------------
for n in "${SHARDS[@]}"; do
  other=$(( 1 - n ))
  id="${device[${n}]}"

  next "${id}: stored on shard_${n} (waiting for the service to process the message)"
  wait_for 1 rows_on_shard "${n}" "${id}" \
    || fail "device is not on shard_${n} within ${TIMEOUT}s (on shard_${other}: $(rows_on_shard "${other}" "${id}" || echo '?')); see docker compose logs device-collector-service and topic devices-dlt"
  ok "shard_${n}: 1 row"

  next "${id}: absent on shard_${other}"
  [[ "$(rows_on_shard "${other}" "${id}")" == "0" ]] || fail "device is also on shard_${other}: routing differs from the sharding rule"
  ok "shard_${other}: 0 rows"

  next "${id}: read through ShardingSphere-Proxy and REST of the service"
  [[ "$(rows_via_proxy "${id}")" == "1" ]] || fail "Proxy does not return the device"
  rest_shard="$(shard_via_rest "${id}")" || rest_shard=""
  [[ "${rest_shard}" == "shard_${n}" ]] || fail "REST reports shard '${rest_shard:-none}', expected shard_${n}"
  ok "Proxy: 1 row, REST: shard_${n}"
done

echo "==> All checks passed"
