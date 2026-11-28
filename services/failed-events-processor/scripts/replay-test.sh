#!/usr/bin/env bash
# Проверка возврата объекта в исходную тему (FR-024, ADR-0019): make failed-replay-test
# services/failed-events-processor/scripts/replay-test.sh
#
# Работает на поднятом стенде (нужны kafka и minio), остальное не требуется. В бакет failed-events кладутся два
# служебных объекта (раздел 99 и 98 в ключе - настоящих записей с такими разделами нет) и создаётся временная тема
# failed-replay-test-<номер>. Проверяет:
#   1. возврат: в теме одно сообщение, побайтно равное исходному (key и value с байтами 0x00, 0x0a, 0x0d, 0xff), traceparent
#      и прочие заголовки сохранены, добавлен заголовок x-replayed-from с ключом объекта;
#   2. повторный возврат: второе сообщение с теми же байтами и тем же заголовком (защиты от повтора нет, ADR-0019);
#   3. неизвестный ключ - код 2, в тему ничего не пишется;
#   4. нечитаемая запись (kind = unreadable) - код 3: исходного сообщения в объекте нет, возвращать нечего.
# Служебные объекты после проверки удаляются обходом режима блокировки (учётка root), тема - удаляется. Код 0 - все в порядке.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${ROOT_DIR}/infrastructure/docker-compose.yaml")
BUCKET="${FAILED_EVENTS_BUCKET:-failed-events}"
RUN_ID="$(date +%s)${RANDOM}"
TOPIC="failed-replay-test-${RUN_ID}"
DAY="$(date -u +%Y/%m/%d)"
KEY_OK="${DAY}/failed-events-99-${RUN_ID}.json"
KEY_UNREADABLE="${DAY}/failed-events-98-${RUN_ID}.json"
KEY_MISSING="${DAY}/failed-events-97-${RUN_ID}.json"
KEY_LIST="${DAY}/failed-events-96-${RUN_ID}.json"
BOOTSTRAP="localhost:9092"

step=0
ok()   { printf '  [OK]   %s\n' "$1"; }
fail() { printf '  [FAIL] step %d: %s\n' "${step}" "$1" >&2; exit 1; }
next() { step=$((step + 1)); printf '%d. %s\n' "${step}" "$1"; }

WORK_DIR="$(mktemp -d)"

# mc внутри контейнера MinIO: алиас root создаётся из окружения контейнера, пароль в командную строку не попадает
mc_exec() {
  "${COMPOSE[@]}" exec -T minio sh -c \
    'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && "$@"' \
    _ mc "$@" < /dev/null
}

# Записать тело из файла $2 в объект $1 (stdin контейнера - файл)
put_object() {
  "${COMPOSE[@]}" exec -T minio sh -c \
    'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc pipe "local/$1"' \
    _ "${BUCKET}/$1" < "$2" >/dev/null
}

kafka_sh() { "${COMPOSE[@]}" exec -T kafka sh -c "$@" < /dev/null; }

cleanup() {
  for k in "${KEY_OK}" "${KEY_UNREADABLE}" "${KEY_LIST}"; do
    mc_exec rm --versions --bypass --force "local/${BUCKET}/${k}" >/dev/null 2>&1 || true
  done
  kafka_sh "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --delete --topic ${TOPIC}" >/dev/null 2>&1 || true
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

# Байты по шестнадцатеричной записи (без пробелов) и обратно; base64 стандартного алфавита одной строкой
hex_to_b64() { local f=""; local i; for ((i = 0; i < ${#1}; i += 2)); do f+="\\x${1:i:2}"; done; printf "${f}" | base64 | tr -d '\n'; }
ascii_hex()  { printf '%s' "$1" | od -An -tx1 | tr -d ' \n'; }

KEY_HEX="6b6579000aff"
VALUE_HEX="000000000a0d00fffe0a"
TRACEPARENT="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
EXTRA_HEX="01ff0a"

object_json() { # $1 - kind-поле (пусто или "kind":"unreadable",)
  cat <<EOF
{${1}"schemaVersion":1,"sourceService":"events-collector-service","sourceTopic":"${TOPIC}","sourcePartition":0,"sourceOffset":1,
"sourceTimestamp":"2026-01-01T00:00:00Z","failedAt":"2026-01-01T00:00:01Z","errorCode":"DESERIALIZATION_FAILED","errorType":"PERMANENT",
"description":"replay-test","attempts":1,"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","deviceId":null,
"key":"$(hex_to_b64 "${KEY_HEX}")","value":"$(hex_to_b64 "${VALUE_HEX}")",
"headers":{"traceparent":"$(printf '%s' "${TRACEPARENT}" | base64 | tr -d '\n')","x-extra":"$(hex_to_b64 "${EXTRA_HEX}")"}}
EOF
}

# Объект schemaVersion 2: заголовки массивом в порядке записи, два tag и nullable без значения
object_json_v2() {
  cat <<EOF
{"schemaVersion":2,"sourceService":"events-collector-service","sourceTopic":"${TOPIC}","sourcePartition":0,"sourceOffset":2,
"sourceTimestamp":"2026-01-01T00:00:00Z","failedAt":"2026-01-01T00:00:01Z","errorCode":"DESERIALIZATION_FAILED","errorType":"PERMANENT",
"description":"replay-test","attempts":1,"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","deviceId":null,
"key":"$(hex_to_b64 "${KEY_HEX}")","value":"$(hex_to_b64 "${VALUE_HEX}")",
"headers":[{"name":"tag","value":"YQ=="},{"name":"tag","value":"Yg=="},{"name":"nullable","value":null}]}
EOF
}

# Сообщения темы: по строке на запись (partition, key, value, заголовки в шестнадцатеричном виде)
dump_topic() {
  "${SCRIPT_DIR}/replay-tool.sh" dump "${TOPIC}" < /dev/null 2>/dev/null
}

next "Стенд: kafka и minio запущены"
"${COMPOSE[@]}" ps --status running --services 2>/dev/null | grep -qx kafka || fail "kafka не запущен (make up)"
"${COMPOSE[@]}" ps --status running --services 2>/dev/null | grep -qx minio || fail "minio не запущен (make up)"
ok "kafka, minio"

next "Тема ${TOPIC} и служебные объекты в ${BUCKET}"
kafka_sh "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --create --topic ${TOPIC} --partitions 1 --replication-factor 1" >/dev/null 2>&1 \
  || fail "тема не создана"
object_json "" > "${WORK_DIR}/ok.json";                          put_object "${KEY_OK}" "${WORK_DIR}/ok.json"
object_json '"kind":"unreadable",' > "${WORK_DIR}/unreadable.json"; put_object "${KEY_UNREADABLE}" "${WORK_DIR}/unreadable.json"
object_json_v2 > "${WORK_DIR}/list.json";                          put_object "${KEY_LIST}" "${WORK_DIR}/list.json"
ok "объекты ${KEY_OK}, ${KEY_UNREADABLE}, ${KEY_LIST}"

next "Возврат объекта: байты key и value, заголовки, x-replayed-from"
[[ -x "${SCRIPT_DIR}/replay.sh" && -x "${SCRIPT_DIR}/replay-tool.sh" ]] || fail "нет исполняемых replay.sh и replay-tool.sh (T080)"
"${SCRIPT_DIR}/replay.sh" "${KEY_OK}" >"${WORK_DIR}/replay1.out" 2>&1 || { cat "${WORK_DIR}/replay1.out" >&2; fail "replay.sh завершился с ошибкой"; }
dump_topic > "${WORK_DIR}/dump1.txt"
[[ "$(wc -l < "${WORK_DIR}/dump1.txt" | tr -d ' ')" == "1" ]] || { cat "${WORK_DIR}/dump1.txt" >&2; fail "в теме должно быть одно сообщение"; }
grep -q "key=${KEY_HEX} " "${WORK_DIR}/dump1.txt"   || fail "ключ не равен исходному: $(cat "${WORK_DIR}/dump1.txt")"
grep -q "value=${VALUE_HEX} " "${WORK_DIR}/dump1.txt" || fail "значение не равно исходному: $(cat "${WORK_DIR}/dump1.txt")"
grep -q "h.traceparent=$(ascii_hex "${TRACEPARENT}")" "${WORK_DIR}/dump1.txt" || fail "traceparent не сохранён"
grep -q "h.x-extra=${EXTRA_HEX}" "${WORK_DIR}/dump1.txt" || fail "прочие заголовки не сохранены"
grep -q "h.x-replayed-from=$(ascii_hex "${KEY_OK}")" "${WORK_DIR}/dump1.txt" || fail "нет заголовка x-replayed-from с ключом объекта"
ok "key, value и заголовки побайтно, x-replayed-from = ключ объекта"

next "Повторный возврат: второе сообщение, тоже помечено"
"${SCRIPT_DIR}/replay.sh" "${KEY_OK}" >/dev/null 2>&1 || fail "повторный возврат завершился с ошибкой"
dump_topic > "${WORK_DIR}/dump2.txt"
[[ "$(wc -l < "${WORK_DIR}/dump2.txt" | tr -d ' ')" == "2" ]] || fail "в теме должно быть два сообщения"
[[ "$(grep -c "h.x-replayed-from=$(ascii_hex "${KEY_OK}")" "${WORK_DIR}/dump2.txt")" == "2" ]] || fail "оба сообщения должны нести x-replayed-from"
[[ "$(grep -c "value=${VALUE_HEX} " "${WORK_DIR}/dump2.txt")" == "2" ]] || fail "байты второго сообщения отличаются"
ok "два сообщения с одинаковыми байтами и пометкой"

next "Неизвестный ключ: код 2, в тему ничего не пишется"
rc=0; "${SCRIPT_DIR}/replay.sh" "${KEY_MISSING}" >/dev/null 2>&1 || rc=$?
[[ "${rc}" == "2" ]] || fail "ожидался код 2, получен ${rc}"
[[ "$(dump_topic | wc -l | tr -d ' ')" == "2" ]] || fail "в теме появилось лишнее сообщение"
ok "код 2"

next "Нечитаемая запись (kind = unreadable): код 3"
rc=0; "${SCRIPT_DIR}/replay.sh" "${KEY_UNREADABLE}" >/dev/null 2>&1 || rc=$?
[[ "${rc}" == "3" ]] || fail "ожидался код 3, получен ${rc}"
[[ "$(dump_topic | wc -l | tr -d ' ')" == "2" ]] || fail "в теме появилось лишнее сообщение"
ok "код 3"

next "Объект schemaVersion 2: заголовки по порядку, с повторами и null, x-replayed-from"
"${SCRIPT_DIR}/replay.sh" "${KEY_LIST}" >"${WORK_DIR}/replay3.out" 2>&1 || { cat "${WORK_DIR}/replay3.out" >&2; fail "replay.sh завершился с ошибкой"; }
dump_topic > "${WORK_DIR}/dump3.txt"
[[ "$(wc -l < "${WORK_DIR}/dump3.txt" | tr -d ' ')" == "3" ]] || { cat "${WORK_DIR}/dump3.txt" >&2; fail "в теме должно быть три сообщения"; }
last="$(tail -n 1 "${WORK_DIR}/dump3.txt")"
[[ "${last}" == *" h.tag=61 h.tag=62 h.nullable=- "* ]] || fail "заголовки не по порядку или без повторов/null: ${last}"
[[ "${last}" == *"h.x-replayed-from=$(ascii_hex "${KEY_LIST}")"* ]] || fail "нет заголовка x-replayed-from с ключом объекта"
ok "tag=a, tag=b, nullable без значения - по порядку; x-replayed-from = ключ объекта"

echo "==> All checks passed"
