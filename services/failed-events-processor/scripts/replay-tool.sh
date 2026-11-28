#!/usr/bin/env bash
# Запуск Replay.java для возврата объекта и проверки темы (используют replay.sh и replay-test.sh):
#   replay-tool.sh produce <ключ объекта> < объект.json   - одно сообщение в sourceTopic объекта
#   replay-tool.sh dump <тема>                            - сообщения темы побайтно, по строке на запись
#
# В образе Kafka только JRE, исходный запуск Java из одного файла там недоступен (нет jdk.compiler). Поэтому скрипт запускает тот же
# JDK, что собирает сервисы (образ первого этапа Dockerfile процессора), одноразовым контейнером в сети стенда; клиенты Kafka и
# Jackson - те же jar из /opt/kafka/libs контейнера kafka (копируются во временный каталог и удаляются).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${ROOT_DIR}/infrastructure/docker-compose.yaml")
BOOTSTRAP="kafka:9092"

MODE="${1:-}"
case "${MODE}" in
  produce|dump) ;;
  *) echo "использование: replay-tool.sh produce <ключ объекта> < объект.json | dump <тема>" >&2; exit 1 ;;
esac
[[ -n "${2:-}" ]] || { echo "не задан второй аргумент" >&2; exit 1; }

# Образ JDK - тот же, что на первом этапе Dockerfile процессора (версия закреплена, не latest)
JDK_IMAGE="$(awk '/^FROM .* AS build/ { print $2; exit }' "${ROOT_DIR}/services/failed-events-processor/Dockerfile")"
[[ -n "${JDK_IMAGE}" ]] || { echo "не найден образ JDK в Dockerfile процессора" >&2; exit 1; }

KAFKA_ID="$("${COMPOSE[@]}" ps -q kafka)"
[[ -n "${KAFKA_ID}" ]] || { echo "контейнер kafka не запущен" >&2; exit 1; }
NETWORK="$(docker inspect "${KAFKA_ID}" --format '{{range $name, $net := .NetworkSettings.Networks}}{{$name}} {{end}}' | awk '{ print $1 }')"

LIBS_DIR="$(mktemp -d)"
trap 'rm -rf "${LIBS_DIR}"' EXIT
docker cp "${KAFKA_ID}:/opt/kafka/libs" "${LIBS_DIR}/libs" >/dev/null

# stdin контейнера - ввод скрипта (объект для produce); для dump он пустой
docker run --rm -i --network "${NETWORK}" \
  -v "${LIBS_DIR}/libs:/libs:ro" -v "${SCRIPT_DIR}:/work:ro" \
  "${JDK_IMAGE}" java -cp '/libs/*' /work/Replay.java "${MODE}" "${BOOTSTRAP}" "$2"
