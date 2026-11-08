#!/usr/bin/env bash
# Быстрая проверка локального стенда после docker compose up (не замена интеграционным тестам)
#
# services/events-collector-service/scripts/check-local.sh
#
# Отправляет в тему events событие со снимком нового устройства и проверяет всю цепочку:
# device_events (со снимком) -> device_outbox -> публикация (SENT) -> DeviceInfo CREATED со снимком в теме devices;
# затем телеметрию того же устройства без снимка -> пропуск публикации (SKIPPED) -> /actuator/health.
# Выход 0 - все в порядке, иначе номер упавшего шага в выводе.
#
# Все выполняется внутри контейнеров стенда (docker compose exec): на хосте нужны
# только docker и bash. Пароли берутся из окружения контейнеров, а не из командной строки.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Корневой .env передается явно, как в Makefile: скрипт запускают из разных каталогов (make, Gradle, вручную),
# а без --env-file Compose ищет .env рядом с compose-файлом и без него подставляет пустые значения переменных
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${ROOT_DIR}/infrastructure/docker-compose.yaml")
TIMEOUT="${CHECK_TIMEOUT:-60}"

EVENT_ID="check-$(date +%s)-${RANDOM}"
TELEMETRY_ID="${EVENT_ID}-telemetry"
# Уникальное устройство: иначе публикация была бы пропущена из-за devices:published
DEVICE_ID="check-device-$(date +%s)-${RANDOM}"
TIMESTAMP_MS="$(( $(date +%s) * 1000 ))"

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

clickhouse() {
  "${COMPOSE[@]}" exec -T clickhouse sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"' _ \
    --param_eid="${EVENT_ID}" --param_tid="${TELEMETRY_ID}" --query "$1"
}

# Схема DeviceEvent для консольного продюсера: тип DeviceSnapshot объявлен в отдельном файле,
# поэтому ссылка на него по имени заменяется самим объявлением
SNAPSHOT_SCHEMA="$(tr -d '\n' < "${ROOT_DIR}/avros/avro/device-snapshot.avsc")"
EVENT_SCHEMA="$(tr -d '\n' < "${ROOT_DIR}/avros/avro/device-event.avsc")"
before="${EVENT_SCHEMA%%\"DeviceSnapshot\"*}"
after="${EVENT_SCHEMA#*\"DeviceSnapshot\"}"
SCHEMA="${before}${SNAPSHOT_SCHEMA}${after}"

# $1 - значение DeviceEvent в Avro JSON
send_event() {
  printf '%s|%s\n' "${DEVICE_ID}" "$1" | "${COMPOSE[@]}" exec -T schema-registry \
    kafka-avro-console-producer \
      --bootstrap-server kafka:9092 \
      --topic events \
      --property schema.registry.url=http://localhost:8081 \
      --property value.schema="${SCHEMA}" \
      --property parse.key=true \
      --property key.separator='|' \
      --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
    >/dev/null 2>&1
}

echo "==> events-collector-service: local stand quick check"
echo "    eventId=${EVENT_ID} deviceId=${DEVICE_ID}"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  printf '  [FAIL] %s not found: cp .env.example .env\n' "${ROOT_DIR}/.env" >&2
  exit 1
fi

# ------------------------------------------------------------------------------
next "Service is running and healthy"
# || state="": при ошибке Compose (pipefail) скрипт иначе завершился бы молча, без сообщения о шаге
state="$("${COMPOSE[@]}" ps --format '{{.Health}}' events-collector-service 2>/dev/null | head -1)" || state=""
[[ "${state}" == "healthy" ]] || fail "events-collector-service is not healthy (state: '${state:-not running}'); see docker compose ps"
ok "container healthy"

# ------------------------------------------------------------------------------
next "Send DeviceEvent with device snapshot to topic events (Avro + Schema Registry, key deviceId)"
# Avro JSON: значения union-полей - в обёртке с именем типа, все поля перечислены явно
SNAPSHOT="{\"com.github.gluhov.iotplatform.events.avro.DeviceSnapshot\":{\"deviceId\":\"${DEVICE_ID}\",\"tenantId\":\"check-tenant\",\"ownerId\":null,\"deviceType\":\"check-sensor\",\"serialNumber\":null,\"manufacturer\":null,\"model\":null,\"firmwareVersion\":{\"string\":\"1.0.0\"},\"status\":\"ACTIVE\",\"registeredAtMs\":null,\"lastSeenAtMs\":{\"long\":${TIMESTAMP_MS}},\"settings\":{\"mode\":\"check\"},\"sensors\":[],\"attributes\":{}}}"
send_event "{\"eventId\":\"${EVENT_ID}\",\"deviceId\":\"${DEVICE_ID}\",\"timestampMs\":${TIMESTAMP_MS},\"eventType\":\"CHECK_LOCAL\",\"device\":${SNAPSHOT},\"sourceSystem\":{\"string\":\"check-local\"},\"payload\":null}" \
  || fail "failed to send message (are Kafka and Schema Registry available?)"
ok "message sent"

#------------------------------------------------------------------------------
next "Event stored in iot_platform.device_events"
wait_for 1 clickhouse "SELECT count() FROM iot_platform.device_events FINAL WHERE event_id = {eid:String} AND device_snapshot != ''" \
  || fail "no row with device_snapshot in device_events within ${TIMEOUT}s (column missing? see clickhouse-init), check service logs"
ok "device_events: 1 row with device snapshot"

# ------------------------------------------------------------------------------
next "Outbox record created in iot_platform.device_outbox"
wait_for 1 clickhouse "SELECT count() FROM iot_platform.device_outbox FINAL WHERE event_id = {eid:String}" \
  || fail "no outbox record"
ok "device_outbox: record created"

# ------------------------------------------------------------------------------
next "Outbox record published (status SENT)"
wait_for SENT clickhouse "SELECT toString(status) FROM iot_platform.device_outbox FINAL WHERE event_id = {eid:String}" \
  || fail "record did not reach SENT, check outbox:lease in Redis and outbox_publish_fail_total"
ok "device_outbox: SENT"

# ------------------------------------------------------------------------------
next "Publication audited in iot_platform.device_publication_audit"
wait_for PUBLISHED clickhouse "SELECT toString(outcome) FROM iot_platform.device_publication_audit WHERE event_id = {eid:String} ORDER BY occurred_at DESC LIMIT 1" \
  || fail "no PUBLISHED audit record (table missing? see infrastructure/init/clickhouse/02-audit.sh)"
ok "device_publication_audit: PUBLISHED"

# ------------------------------------------------------------------------------
next "DeviceInfo CREATED with device snapshot published to topic devices"
found="$("${COMPOSE[@]}" exec -T schema-registry \
  kafka-avro-console-consumer \
    --bootstrap-server kafka:9092 \
    --topic devices \
    --from-beginning \
    --timeout-ms 15000 \
    --property schema.registry.url=http://localhost:8081 \
    --property print.key=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --group "check-local-${EVENT_ID}" \
  2>/dev/null | grep "\"deviceId\":\"${DEVICE_ID}\"" | grep "\"eventType\":\"CREATED\"" | grep -c "\"tenantId\":\"check-tenant\"" || true)"
[[ "${found}" -ge 1 ]] || fail "DeviceInfo CREATED with snapshot for ${DEVICE_ID} not found in topic devices"
ok "devices: DeviceInfo CREATED for ${DEVICE_ID}"

# ------------------------------------------------------------------------------
next "Telemetry without snapshot for the known device is not published (SKIPPED)"
send_event "{\"eventId\":\"${TELEMETRY_ID}\",\"deviceId\":\"${DEVICE_ID}\",\"timestampMs\":$(( TIMESTAMP_MS + 1 )),\"eventType\":\"CHECK_LOCAL\",\"device\":null,\"sourceSystem\":null,\"payload\":null}" \
  || fail "failed to send telemetry"
wait_for SKIPPED clickhouse "SELECT toString(outcome) FROM iot_platform.device_publication_audit WHERE event_id = {tid:String} ORDER BY occurred_at DESC LIMIT 1" \
  || fail "telemetry of a known device was not SKIPPED"
ok "device_publication_audit: SKIPPED"

# ------------------------------------------------------------------------------
next "/actuator/health returns UP"
health="$("${COMPOSE[@]}" exec -T events-collector-service bash -c \
  "exec 3<>/dev/tcp/127.0.0.1/8080; printf 'GET /actuator/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n' >&3; cat <&3" \
  2>/dev/null || true)"
# Ответ с chunked-кодированием: ищем статус во всем ответе, а не в последней строке
[[ "${health}" == *'"status":"UP"'* ]] || fail "health: $(tr -d '\r' <<<"${health}" | grep -m1 '{' || echo 'no response')"
ok "health: UP"

echo "==> All checks passed"