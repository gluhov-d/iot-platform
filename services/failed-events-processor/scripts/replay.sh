#!/usr/bin/env bash
# Вернуть один объект ошибки из MinIO в исходную тему (FR-024, ADR-0019): make failed-replay KEY=<ключ объекта>
# services/failed-events-processor/scripts/replay.sh <ключ объекта>
#
# Читает объект бакета failed-events, декодирует из base64 исходные key и value и заголовки и публикует их в sourceTopic
# без пересериализации (байты те же, traceparent и прочие заголовки сохранены, раздел выбирает ключ сообщения). К сообщению
# добавляется заголовок x-replayed-from = ключ объекта. Объект из MinIO не удаляется.
#
# Возврат точечный: одна команда - одно сообщение. Повторный запуск публикует сообщение ещё раз (с тем же заголовком);
# защиты от повтора нет, ответственность - у оператора: пока причина ошибки не устранена, сообщение снова попадёт в failed-events.
# Коды выхода: 0 - возвращено; 1 - неверные аргументы или ошибка публикации; 2 - объекта нет; 3 - нечитаемая запись
# (kind = unreadable): в ней нет исходного сообщения.
#
# MinIO читается внутри его контейнера (docker compose exec) учёткой root из окружения контейнера, пароль в командную строку не
# попадает; публикует Replay.java (запуск - replay-tool.sh: JDK-контейнер в сети стенда, клиенты Kafka из контейнера kafka).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE=(docker compose --env-file "${ROOT_DIR}/.env" -f "${ROOT_DIR}/infrastructure/docker-compose.yaml")
BUCKET="${FAILED_EVENTS_BUCKET:-failed-events}"

KEY="${1:-}"
if [[ -z "${KEY}" ]]; then
  echo "использование: replay.sh <ключ объекта>, например 2026/11/23/failed-events-0-113.json (make failed-replay KEY=...)" >&2
  exit 1
fi
if [[ ! "${KEY}" =~ ^[0-9]{4}/[0-9]{2}/[0-9]{2}/failed-events-[0-9]+-[0-9]+\.json$ ]]; then
  echo "ключ объекта должен быть вида <yyyy>/<MM>/<dd>/failed-events-<раздел>-<смещение>.json: ${KEY}" >&2
  exit 1
fi

OBJECT="$("${COMPOSE[@]}" exec -T minio sh -c \
  'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc cat "local/$1"' \
  _ "${BUCKET}/${KEY}" < /dev/null 2>/dev/null)" || { echo "объект не найден: ${BUCKET}/${KEY}" >&2; exit 2; }

rc=0
out="$(printf '%s' "${OBJECT}" | "${SCRIPT_DIR}/replay-tool.sh" produce "${KEY}" 2>&1)" || rc=$?
# Предупреждения JVM и SLF4J из вывода убираем; сообщения Replay.java - по-русски
echo "${out}" | grep -v -E '^(OpenJDK|SLF4J|WARNING|log4j)' || true
exit "${rc}"
