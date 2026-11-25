#!/usr/bin/env bash
# Тест ожидания ответа HTTP (avros/scripts/http-wait.sh): Nexus после убийства по нехватке памяти поднимается
# 30-40 секунд, а публикация avros не должна падать с HTTP 000 на первой же проверке.
# Запуск: avros/scripts/http-wait-test.sh. Нужны bash, curl и python3; Docker не нужен.
set -uo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=http-wait.sh
source "${DIR}/http-wait.sh" 2>/dev/null || { echo "FAIL  http-wait.sh не найден или не читается"; exit 1; }

pass=0 fail=0
check() { # <имя> <ожидаемое> <получено>
  if [[ "$2" == "$3" ]]; then echo "PASS  $1"; pass=$((pass + 1)); else echo "FAIL  $1: ожидали '$2', получили '$3'"; fail=$((fail + 1)); fi
}
check_between() { # <имя> <мин> <макс> <получено>
  if (( $4 >= $2 && $4 <= $3 )); then echo "PASS  $1 (${4} с)"; pass=$((pass + 1)); else echo "FAIL  $1: ${4} с вне ${2}..${3}"; fail=$((fail + 1)); fi
}

tmp="$(mktemp -d)"
pids=()
cleanup() { for p in "${pids[@]:-}"; do [[ -n "${p}" ]] && kill "${p}" 2>/dev/null; done; rm -rf "${tmp}"; }
trap cleanup EXIT

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# Сервер: слушает порт $1 после паузы $2 секунд; первые $3 запросов отвечает 503, остальные - $4
cat >"${tmp}/server.py" <<'PY'
import sys, time
from http.server import BaseHTTPRequestHandler, HTTPServer
port, delay, unavailable, code = int(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
time.sleep(delay)
seen = {"n": 0}
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        seen["n"] += 1
        self.send_response(503 if seen["n"] <= unavailable else code)
        self.send_header("Content-Length", "0")
        self.end_headers()
    def log_message(self, *a):
        pass
HTTPServer(("127.0.0.1", port), H).serve_forever()
PY
start_server() { python3 "${tmp}/server.py" "$@" & pids+=("$!"); disown; }
timed() { local t0=$SECONDS; RESULT="$("$@")"; ELAPSED=$((SECONDS - t0)); }

echo "== givenServerReady_whenWait_thenReturnsCodeAtOnce"
port="$(free_port)"; start_server "${port}" 0 0 404; sleep 1
timed http_code_wait "http://127.0.0.1:${port}/x" 10 1
check "код 404 возвращён как есть" 404 "${RESULT}"
check_between "без ожидания" 0 2 "${ELAPSED}"

echo "== givenServerStartsLate_whenWait_thenWaitsForIt"
port="$(free_port)"; start_server "${port}" 4 0 404
timed http_code_wait "http://127.0.0.1:${port}/x" 20 1
check "после запуска сервера получен 404" 404 "${RESULT}"
check_between "ждали запуска" 3 12 "${ELAPSED}"

echo "== givenServerStartingUp503_whenWait_thenWaitsForReadiness"
port="$(free_port)"; start_server "${port}" 0 3 200; sleep 1
timed http_code_wait "http://127.0.0.1:${port}/x" 20 1
check "после трёх 503 получен 200" 200 "${RESULT}"
check_between "ждали готовности" 2 12 "${ELAPSED}"

echo "== givenNobodyListens_whenWait_thenGivesUpWithCode000"
port="$(free_port)"
timed http_code_wait "http://127.0.0.1:${port}/x" 4 1
check "код 000 после тайм-аута" 000 "${RESULT}"
check_between "ждали около срока" 3 9 "${ELAPSED}"

echo "== givenWaitFromEnvironment_whenNobodyListens_thenUsesIt"
port="$(free_port)"
NEXUS_WAIT_SECONDS=2 NEXUS_WAIT_INTERVAL=1 timed http_code_wait "http://127.0.0.1:${port}/x"
check "код 000" 000 "${RESULT}"
check_between "срок из NEXUS_WAIT_SECONDS" 1 6 "${ELAPSED}"

echo "== итог: PASS ${pass}, FAIL ${fail}"
[[ "${fail}" -eq 0 ]]
