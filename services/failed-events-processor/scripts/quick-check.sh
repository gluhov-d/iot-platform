#!/usr/bin/env bash
# Быстрая проверка локального стенда: ошибка каждого сборщика доходит до объекта в MinIO
# (не замена интеграционным тестам)
#
# services/failed-events-processor/scripts/quick-check.sh      (или make fep-quick-check)
#
# Отправляет три сообщения и ждёт по объекту на каждое в бакете failed-events:
#   1. невалидное событие в тему events (timestampMs = 0) с заданным traceparent
#      -> <yyyy>/<MM>/<dd>/failed-events-<p>-<o>.json (sourceService events-collector-service, VALIDATION_FAILED), traceId в JSON равен заданному;
#   2. невалидное сообщение в тему devices (ключ не равен deviceId)
#      -> тот же вид ключа (sourceService device-collector-service, VALIDATION_FAILED);
#   3. байты-мусор в тему events (не Avro)
#      -> тот же вид ключа (DESERIALIZATION_FAILED), base64(value) равно отправленному.
# Во всех трёх объектах base64(key) равно отправленному ключу. Для Avro-сообщений (1 и 2) байты value заранее неизвестны
# (их строит консольный продюсер): проверяется, что value начинается с байта 0 формата Schema Registry.
# Объекты ищутся среди появившихся после запуска скрипта: ключ объекта строится из раздела, смещения и времени записи
# failed-events (ADR-0014), их заранее не знают, а сервис и код ошибки лежат в теле объекта.
# Выход 0 - все в порядке ("All checks passed"), иначе номер упавшего шага в выводе.
#
# Все выполняется внутри контейнеров стенда (docker compose exec): на хосте нужны только docker и bash.
# Доступ к MinIO - учётная запись root из окружения его контейнера, не из командной строки. Тестовые объекты остаются
# в бакете (Object Lock, срок ILM): после проверки их не удалить.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Корневой .env передается явно: скрипт запускают из разных каталогов (make, вручную),
# а без --env-file Compose ищет .env рядом с compose-файлом и без него подставляет пустые значения переменных
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${ROOT_DIR}/infrastructure/docker-compose.yaml")
TIMEOUT="${CHECK_TIMEOUT:-60}"
RUN_ID="$(date +%s)-${RANDOM}"
BUCKET="${FAILED_EVENTS_BUCKET:-failed-events}"

step=0
ok()   { printf '  [OK]   %s\n' "$1"; }
fail() { printf '  [FAIL] step %d: %s\n' "${step}" "$1" >&2; exit 1; }
next() { step=$((step + 1)); printf '%d. %s\n' "${step}" "$1"; }

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# base64 стандартного алфавита одной строкой (GNU и BSD base64 по-разному переносят строки)
b64() { printf '%s' "$1" | base64 | tr -d '\n'; }

# mc внутри контейнера MinIO: алиас с учёткой root создаётся из окружения контейнера, пароль в командную строку не попадает
mc_exec() {
  "${COMPOSE[@]}" exec -T minio sh -c \
    'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && "$@"' \
    _ mc "$@" < /dev/null
}

# Ключи всех объектов бакета, по одному в строке, отсортированно (пустой бакет - пустой вывод)
list_keys() {
  mc_exec find "local/${BUCKET}" --name '*.json' 2>/dev/null | sort || true
}

# Ищет среди появившихся после snapshot объектов тот, в чьём теле есть строка $1; печатает его путь.
# Ждёт не дольше TIMEOUT секунд (ошибка доходит до объекта асинхронно: сборщик -> failed-events -> процессор -> MinIO)
find_object() {
  local needle="$1" deadline=$(( $(date +%s) + TIMEOUT )) key content
  while true; do
    list_keys > "${WORK_DIR}/after.txt"
    for key in $(comm -13 "${WORK_DIR}/before.txt" "${WORK_DIR}/after.txt"); do
      # Тело - в переменную, а не в конвейер с grep -q: ранний выход grep при pipefail давал бы код ошибки у найденного
      content="$(mc_exec cat "${key}" 2>/dev/null)" || continue
      if grep -qF -- "${needle}" <<< "${content}"; then
        echo "${key}"
        return 0
      fi
    done
    [[ $(date +%s) -ge ${deadline} ]] && return 1
    sleep 2
  done
}

# Схема DeviceEvent и DeviceInfo для консольного продюсера: тип DeviceSnapshot объявлен в отдельном файле,
# поэтому ссылка на него по имени заменяется самим объявлением
SNAPSHOT_SCHEMA="$(tr -d '\n' < "${ROOT_DIR}/avros/avro/device-snapshot.avsc")"
inline_snapshot() {
  local schema before after
  schema="$(tr -d '\n' < "$1")"
  before="${schema%%\"DeviceSnapshot\"*}"
  after="${schema#*\"DeviceSnapshot\"}"
  printf '%s' "${before}${SNAPSHOT_SCHEMA}${after}"
}
EVENT_SCHEMA="$(inline_snapshot "${ROOT_DIR}/avros/avro/device-event.avsc")"
INFO_SCHEMA="$(inline_snapshot "${ROOT_DIR}/avros/avro/device-info.avsc")"

# $1 - тема, $2 - схема значения, stdin - строка "[заголовок\t]ключ|значение в Avro JSON"
send_avro() {
  "${COMPOSE[@]}" exec -T schema-registry \
    kafka-avro-console-producer \
      --bootstrap-server kafka:9092 \
      --topic "$1" \
      --property schema.registry.url=http://localhost:8081 \
      --property value.schema="$2" \
      --property auto.register.schemas=false \
      --property use.latest.version=true \
      --property parse.key=true \
      --property key.separator='|' \
      --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
      --property parse.headers=true \
    >/dev/null 2>&1
}

# stdin - строка "ключ|значение" (значение не Avro: консольный продюсер Kafka шлёт его как есть, без формата Schema Registry)
send_raw() {
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$1" \
    --property parse.key=true \
    --property key.separator='|' \
  >/dev/null 2>&1
}

echo "==> failed-events-processor: local stand quick check"
echo "    runId=${RUN_ID}"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  printf '  [FAIL] %s not found: cp .env.example .env\n' "${ROOT_DIR}/.env" >&2
  exit 1
fi

# ------------------------------------------------------------------------------
next "Processor, both collectors, Kafka and MinIO are running and healthy"
for service in failed-events-processor events-collector-service device-collector-service kafka minio; do
  # || state="": при ошибке Compose (pipefail) скрипт иначе завершился бы молча, без сообщения о шаге
  state="$("${COMPOSE[@]}" ps --format '{{.Health}}' "${service}" 2>/dev/null | sort -u | paste -sd' ' -)" || state=""
  [[ "${state}" == "healthy" ]] || fail "${service} is not healthy (state: '${state:-not running}'); see docker compose ps"
done
ok "all five services healthy"

# ------------------------------------------------------------------------------
next "Snapshot of objects already in ${BUCKET}"
list_keys > "${WORK_DIR}/before.txt"
ok "$(wc -l < "${WORK_DIR}/before.txt" | tr -d '[:space:]') objects before"

# ------------------------------------------------------------------------------
next "Send three bad messages: invalid event (with traceparent) and garbage to events, invalid DeviceInfo to devices"
TRACE_ID="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
SPAN_ID="$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
TRACEPARENT="00-${TRACE_ID}-${SPAN_ID}-01"
EVENT_KEY="fep-check-${RUN_ID}-event"
DEVICE_KEY="fep-check-${RUN_ID}-wrong-key"
DEVICE_ID="fep-check-${RUN_ID}-device"
GARBAGE_KEY="fep-check-${RUN_ID}-garbage-key"
GARBAGE_VALUE="fep-check-${RUN_ID}-not-avro"
echo "    traceId=${TRACE_ID}"

# 1. timestampMs = 0 не проходит проверку сборщика событий (InvalidEventException -> VALIDATION_FAILED)
printf 'traceparent:%s\t%s|%s\n' "${TRACEPARENT}" "${EVENT_KEY}" \
  "{\"eventId\":\"fep-check-${RUN_ID}\",\"deviceId\":\"${EVENT_KEY}\",\"timestampMs\":0,\"eventType\":\"FEP_CHECK\",\"device\":null,\"sourceSystem\":null,\"payload\":null}" \
  | send_avro events "${EVENT_SCHEMA}" \
  || fail "failed to send the event to events (are Kafka and Schema Registry available?)"

# 2. ключ сообщения не равен deviceId (InvalidDeviceInfoException -> VALIDATION_FAILED)
NOW_MS="$(( $(date +%s) * 1000 ))"
printf 'x-check:fep-quick-check\t%s|%s\n' "${DEVICE_KEY}" \
  "{\"deviceId\":\"${DEVICE_ID}\",\"emittedAtMs\":${NOW_MS},\"eventType\":\"CREATED\",\"eventId\":{\"string\":\"fep-check-${RUN_ID}\"},\"eventTimeMs\":{\"long\":${NOW_MS}},\"device\":null,\"operation\":null}" \
  | send_avro devices "${INFO_SCHEMA}" \
  || fail "failed to send the message to devices"

# 3. не Avro: у значения нет байта 0 и номера схемы (DeserializationException -> DESERIALIZATION_FAILED)
printf '%s|%s\n' "${GARBAGE_KEY}" "${GARBAGE_VALUE}" | send_raw events \
  || fail "failed to send the garbage to events"
ok "3 messages sent"

# ------------------------------------------------------------------------------
next "Invalid event: object VALIDATION_FAILED of events-collector-service with the given traceId"
object="$(find_object "\"traceId\":\"${TRACE_ID}\"")" \
  || fail "no object with traceId ${TRACE_ID} within ${TIMEOUT}s; see docker compose logs events-collector-service failed-events-processor"
echo "    ${object}"
[[ "${object}" =~ /[0-9]{4}/[0-9]{2}/[0-9]{2}/failed-events-[0-9]+-[0-9]+\.json$ ]] \
  || fail "unexpected object key: ${object}"
body="$(mc_exec cat "${object}")"
grep -qF -- '"sourceService":"events-collector-service"' <<< "${body}" || fail "sourceService in the object is not events-collector-service"
grep -qF -- '"errorCode":"VALIDATION_FAILED"' <<< "${body}" || fail "errorCode in the object is not VALIDATION_FAILED"
grep -qF -- "\"key\":\"$(b64 "${EVENT_KEY}")\"" <<< "${body}" || fail "base64(key) in the object differs from the sent key"
grep -qF -- "\"value\":\"AAAA" <<< "${body}" || fail "value in the object does not start with the Schema Registry magic byte"
grep -qF -- "{\"name\":\"traceparent\",\"value\":\"$(b64 "${TRACEPARENT}")\"}" <<< "${body}" || fail "header traceparent is missing in the object or differs"
ok "key, traceId and traceparent match; value is the original Avro message"

# ------------------------------------------------------------------------------
next "Invalid DeviceInfo: object VALIDATION_FAILED of device-collector-service"
object="$(find_object "\"deviceId\":\"${DEVICE_ID}\"")" \
  || fail "no object with deviceId ${DEVICE_ID} within ${TIMEOUT}s; see docker compose logs device-collector-service failed-events-processor"
echo "    ${object}"
[[ "${object}" =~ /[0-9]{4}/[0-9]{2}/[0-9]{2}/failed-events-[0-9]+-[0-9]+\.json$ ]] \
  || fail "unexpected object key: ${object}"
body="$(mc_exec cat "${object}")"
grep -qF -- '"sourceService":"device-collector-service"' <<< "${body}" || fail "sourceService in the object is not device-collector-service"
grep -qF -- '"errorCode":"VALIDATION_FAILED"' <<< "${body}" || fail "errorCode in the object is not VALIDATION_FAILED"
grep -qF -- "\"key\":\"$(b64 "${DEVICE_KEY}")\"" <<< "${body}" || fail "base64(key) in the object differs from the sent key"
grep -qF -- "\"value\":\"AAAA" <<< "${body}" || fail "value in the object does not start with the Schema Registry magic byte"
ok "key and deviceId match; value is the original Avro message"

# ------------------------------------------------------------------------------
next "Garbage: object DESERIALIZATION_FAILED, base64(value) equals the sent bytes"
object="$(find_object "\"value\":\"$(b64 "${GARBAGE_VALUE}")\"")" \
  || fail "no object with the sent value within ${TIMEOUT}s; see docker compose logs events-collector-service failed-events-processor"
echo "    ${object}"
[[ "${object}" =~ /[0-9]{4}/[0-9]{2}/[0-9]{2}/failed-events-[0-9]+-[0-9]+\.json$ ]] \
  || fail "unexpected object key: ${object}"
body="$(mc_exec cat "${object}")"
grep -qF -- '"sourceService":"events-collector-service"' <<< "${body}" || fail "sourceService in the object is not events-collector-service"
grep -qF -- '"errorCode":"DESERIALIZATION_FAILED"' <<< "${body}" || fail "errorCode in the object is not DESERIALIZATION_FAILED"
grep -qF -- "\"key\":\"$(b64 "${GARBAGE_KEY}")\"" <<< "${body}" || fail "base64(key) in the object differs from the sent key"
ok "key and value match the sent bytes"

echo "==> All checks passed"
