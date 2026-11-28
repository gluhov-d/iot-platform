# shellcheck shell=bash
# Ожидание ответа HTTP (подключается через source, сама не запускается).
#
# Nexus убивается по нехватке памяти виртуальной машины Docker и поднимается заново 30-40 секунд: в это время порт
# закрыт (curl отдаёт 000) или сервер отвечает 502/503/504. Такой ответ - не результат проверки, а повод подождать.
#
# http_code_wait <url> [макс. секунд] [пауза между попытками]
# Печатает код первого ответа, кроме 000/502/503/504; по истечении срока - последний полученный код.
# Срок и пауза по умолчанию - NEXUS_WAIT_SECONDS (180) и NEXUS_WAIT_INTERVAL (3) из окружения. Код возврата всегда 0:
# что делать с кодом, решает вызывающий.
http_code_wait() {
  local url="$1" wait="${2:-${NEXUS_WAIT_SECONDS:-180}}" pause="${3:-${NEXUS_WAIT_INTERVAL:-3}}" code deadline
  deadline=$((SECONDS + wait))
  while :; do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "${url}" || true)"
    case "${code}" in
      000 | 502 | 503 | 504) ;;
      *) printf '%s' "${code}"; return 0 ;;
    esac
    if (( SECONDS + pause > deadline )); then
      printf '%s' "${code}"
      return 0
    fi
    sleep "${pause}"
  done
}
