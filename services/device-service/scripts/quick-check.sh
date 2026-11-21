#!/usr/bin/env bash
# Быстрая проверка локального стенда: device-service отвечает по REST, команды доходят до реплик
# (не замена интеграционным тестам)
#
# services/device-service/scripts/quick-check.sh      (или make ds-quick-check)
#
# Сценарий quickstart.md, запускается с хоста (нужны bash, curl и docker):
#   - сервисный токен Keycloak (client credentials, клиент device-service-consumer) получен с хоста;
#   - POST на первой реплике -> 202; GET на второй реплике возвращает устройство (до TIMEOUT с);
#   - повторный POST -> 409 DEVICE_ALREADY_EXISTS;
#   - PUT -> 202; GET на второй реплике видит новые данные не позже чем через SLA с;
#   - DELETE -> 202; GET -> 404 DEVICE_NOT_FOUND не позже чем через SLA с;
#   - GET без токена -> 401;
#   - сессии device_reader есть на репликах шардов и отсутствуют на основных узлах (чтение только из реплик).
# Выход 0 - все в порядке (\"All checks passed\"), иначе номер упавшего шага в выводе.
#
# Секрет клиента читается из корневого .env через docker compose config --environment (как в avros/scripts/nexus-env.sh)
# и не выводится: ни в вывод, ни в командную строку curl (передаётся через stdin). Ответ Keycloak с токеном не печатается.
# Совместим с bash 3.2 (macOS): без declare -A, mapfile, readarray; только BSD-совместимые утилиты.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Корневой .env передается явно: скрипт запускают из разных каталогов (make, вручную),
# а без --env-file Compose ищет .env рядом с compose-файлом и без него подставляет пустые значения переменных
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${ROOT_DIR}/infrastructure/docker-compose.yaml")
TIMEOUT="${CHECK_TIMEOUT:-60}"      # создание: сообщение обрабатывает device-collector-service асинхронно
SLA="${CHECK_SLA:-5}"               # изменение и удаление видны в GET не позже чем через 5 с (R7)
EXPECTED_REPLICAS=2
DEVICE_ID="ds-check-$(date +%s)-${RANDOM}"
SHARDS=(0 1)

BODY_FILE="$(mktemp)"
trap 'rm -f "${BODY_FILE}"' EXIT

step=0
ok()   { printf '  [OK]   %s\n' "$1"; }
fail() { printf '  [FAIL] step %d: %s\n' "${step}" "$1" >&2; exit 1; }
next() { step=$((step + 1)); printf '%d. %s\n' "${step}" "$1"; }

# Значение переменной в том виде, как его разобрал Compose из .env; сам файл не читается и не печатается
env_dump=""
env_value() { sed -n "s/^$1=//p" <<<"${env_dump}"; }

# HTTP-запрос к реплике: печатает код ответа, тело - в BODY_FILE
# $1 - база (http://localhost:PORT), $2 - метод, $3 - путь, $4 - токен или пусто, $5 - тело JSON или пусто
api() {
  local args=(-sS -o "${BODY_FILE}" -w '%{http_code}' --max-time 10 -X "$2" "$1$3")
  if [[ -n "$4" ]]; then args+=(-H "Authorization: Bearer $4"); fi
  if [[ -n "$5" ]]; then args+=(-H 'Content-Type: application/json' -d "$5"); fi
  # При сбое соединения curl сам печатает 000 (код не дописываем, иначе получится 000000)
  curl "${args[@]}" 2>/dev/null || true
}

# Порт хоста реплики $1 (1 или 2): docker compose port выдаёт адрес вида 0.0.0.0:8105
replica_base() {
  local port
  port="$("${COMPOSE[@]}" port --index "$1" device-service 8080 < /dev/null 2>/dev/null | sed -n '1s/.*:\([0-9][0-9]*\)$/\1/p')"
  [[ -n "${port}" ]] || return 1
  echo "http://localhost:${port}"
}

# Ждёт, пока GET на реплике $1 вернёт код $2 и тело, содержащее $3 (если задано); не дольше $4 секунд.
# Последний код - в LAST_CODE
LAST_CODE=""
wait_get() {
  local base="$1" code="$2" needle="$3" limit="$4" deadline
  deadline=$(( $(date +%s) + limit ))
  while true; do
    LAST_CODE="$(api "${base}" GET "/api/v1/devices/${DEVICE_ID}" "${TOKEN}" "")"
    if [[ "${LAST_CODE}" == "${code}" ]]; then
      if [[ -z "${needle}" ]] || grep -q -F "${needle}" "${BODY_FILE}"; then
        return 0
      fi
    fi
    [[ $(date +%s) -ge ${deadline} ]] && return 1
    sleep 1
  done
}

# Число сессий device_reader на узле шарда: $1 - имя контейнера, $2 - номер шарда (база shard_N);
# учётная запись - из окружения контейнера
reader_sessions() {
  "${COMPOSE[@]}" exec -T "$1" sh -c \
    'psql -U "$POSTGRES_USER" -d "shard_$1" -tA -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM pg_stat_activity WHERE usename = '\''device_reader'\''"' \
    sh "$2" < /dev/null | tr -d '[:space:]'
}

echo "==> device-service: local stand quick check"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  printf '  [FAIL] %s not found: cp .env.example .env\n' "${ROOT_DIR}/.env" >&2
  exit 1
fi
env_dump="$("${COMPOSE[@]}" config --environment)" || fail "docker compose config: не удалось прочитать ${ROOT_DIR}/.env"

# ------------------------------------------------------------------------------
next "Both device-service replicas are healthy"
states="$("${COMPOSE[@]}" ps --format '{{.Health}}' device-service < /dev/null 2>/dev/null || true)"
healthy="$(printf '%s\n' "${states}" | grep -c '^healthy$' || true)"
[[ "${healthy}" == "${EXPECTED_REPLICAS}" ]] \
  || fail "healthy replicas: ${healthy}, expected ${EXPECTED_REPLICAS}; see docker compose ps device-service"
ok "${healthy} replicas healthy"

# ------------------------------------------------------------------------------
next "Service token from the host (client credentials, device-service-consumer)"
secret="$(env_value DEVICE_SERVICE_CONSUMER_SECRET)"
kc_port="$(env_value KEYCLOAK_PORT)"
realm="$(env_value KEYCLOAK_REALM)"
[[ -n "${secret}" && -n "${kc_port}" && -n "${realm}" ]] || fail "DEVICE_SERVICE_CONSUMER_SECRET, KEYCLOAK_PORT или KEYCLOAK_REALM не задан в .env"
# Параметры формы - через stdin (curl -K -): секрет не попадает в список процессов
token_response="$(curl -sS --max-time 10 -K - <<CFG 2>/dev/null || true
url = "http://localhost:${kc_port}/realms/${realm}/protocol/openid-connect/token"
data = "grant_type=client_credentials"
data = "client_id=device-service-consumer"
data-urlencode = "client_secret=${secret}"
CFG
)"
TOKEN="$(sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p' <<<"${token_response}")"
token_response=""
secret=""
[[ -n "${TOKEN}" ]] || fail "Keycloak did not issue a token (realm ${realm}, порт ${kc_port}; см. docker compose logs keycloak)"
ok "token received"

BASE1="$(replica_base 1)" || fail "replica 1 has no published port"
BASE2="$(replica_base 2)" || fail "replica 2 has no published port"
echo "    replica 1: ${BASE1}   replica 2: ${BASE2}"

# ------------------------------------------------------------------------------
next "POST creates the device (202) and replica 2 returns it"
code="$(api "${BASE1}" POST /api/v1/devices "${TOKEN}" \
  "{\"deviceId\":\"${DEVICE_ID}\",\"tenantId\":\"check-tenant\",\"deviceType\":\"check-sensor\",\"status\":\"ACTIVE\",\"name\":\"quick-check-v1\"}")"
[[ "${code}" == 202 ]] || fail "POST: HTTP ${code}, expected 202"
wait_get "${BASE2}" 200 "quick-check-v1" "${TIMEOUT}" \
  || fail "device not readable within ${TIMEOUT}s (last HTTP ${LAST_CODE}); see docker compose logs device-collector-service"
ok "202, then 200 with name quick-check-v1"

next "Repeated POST gives 409 DEVICE_ALREADY_EXISTS"
code="$(api "${BASE1}" POST /api/v1/devices "${TOKEN}" \
  "{\"deviceId\":\"${DEVICE_ID}\",\"tenantId\":\"check-tenant\",\"deviceType\":\"check-sensor\",\"status\":\"ACTIVE\",\"name\":\"quick-check-v1\"}")"
[[ "${code}" == 409 ]] && grep -q -F '"code":"DEVICE_ALREADY_EXISTS"' "${BODY_FILE}" \
  || fail "POST again: HTTP ${code}, expected 409 DEVICE_ALREADY_EXISTS"
ok "409 DEVICE_ALREADY_EXISTS"

next "PUT updates the device (202); replica 2 shows the new data within ${SLA}s"
code="$(api "${BASE1}" PUT "/api/v1/devices/${DEVICE_ID}" "${TOKEN}" \
  '{"tenantId":"check-tenant","deviceType":"check-sensor","status":"MAINTENANCE","name":"quick-check-v2"}')"
[[ "${code}" == 202 ]] || fail "PUT: HTTP ${code}, expected 202"
wait_get "${BASE2}" 200 "quick-check-v2" "${SLA}" \
  || fail "new data not visible within ${SLA}s (last HTTP ${LAST_CODE}); the cache or the read model lags"
ok "202, then 200 with name quick-check-v2 within ${SLA}s"

next "DELETE removes the device (202); replica 2 returns 404 within ${SLA}s"
code="$(api "${BASE1}" DELETE "/api/v1/devices/${DEVICE_ID}" "${TOKEN}" "")"
[[ "${code}" == 202 ]] || fail "DELETE: HTTP ${code}, expected 202"
wait_get "${BASE2}" 404 "DEVICE_NOT_FOUND" "${SLA}" \
  || fail "device still visible after ${SLA}s (last HTTP ${LAST_CODE})"
ok "202, then 404 DEVICE_NOT_FOUND within ${SLA}s"

next "GET without a token gives 401"
code="$(api "${BASE1}" GET "/api/v1/devices/${DEVICE_ID}" "" "")"
[[ "${code}" == 401 ]] || fail "GET without token: HTTP ${code}, expected 401"
ok "401"

next "device_reader sessions: on replicas, none on the primaries"
# Устройство проверки живёт на одном шарде, а Proxy открывает соединения с репликой лениво:
# сессии есть только у реплики шарда этого устройства, поэтому для реплик проверяется сумма
replicas_total=0
for n in "${SHARDS[@]}"; do
  primary="$(reader_sessions "postgres-shard-${n}" "${n}")" || primary="?"
  replica="$(reader_sessions "postgres-shard-${n}-replica" "${n}")" || replica="?"
  [[ "${primary}" == 0 ]] || fail "shard_${n}: ${primary} device_reader session(s) on the primary, expected 0"
  [[ "${replica}" =~ ^[0-9]+$ ]] || fail "shard_${n}: не удалось прочитать сессии device_reader на реплике: '${replica}'"
  replicas_total=$(( replicas_total + replica ))
  echo "    shard_${n}: primary ${primary}, replica ${replica}"
done
(( replicas_total > 0 )) || fail "device_reader sessions on the replicas: 0, expected > 0 (после запросов чтения)"
ok "primaries: 0 sessions, replicas: ${replicas_total}"

echo "==> All checks passed"
