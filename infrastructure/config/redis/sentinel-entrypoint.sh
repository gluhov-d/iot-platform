#!/bin/sh
# Точка входа Sentinel (образ redis:alpine, sh без bash). Конфигурация создаётся один раз и хранится в томе /data:
# Sentinel сам переписывает её при переключении основного узла (новый адрес, известные реплики и Sentinel).
# Пересоздание контейнера не возвращает старый адрес из переменных окружения, иначе Sentinel снова считал бы
# основным узел, которого Sentinel уже сменил (research.md R14).
# Изменения переменных окружения (имя, адрес основного узла) применяются только к новому тому.
#
# Переменные окружения (из compose):
#   REDIS_MASTER_IP       - адрес основного узла при первом запуске;
#   REDIS_SENTINEL_MASTER - имя основного узла;
#   REDIS_PASSWORD        - пароль узлов Redis, Sentinel передаёт его при подключении к основному узлу.
set -eu

: "${REDIS_MASTER_IP:?REDIS_MASTER_IP не задан}"
: "${REDIS_SENTINEL_MASTER:?REDIS_SENTINEL_MASTER не задан}"
: "${REDIS_PASSWORD:?REDIS_PASSWORD не задан}"

CONF=/data/sentinel.conf
if [ ! -s "${CONF}" ]; then
  # Кворум 2 из 3 (ADR-0008); переключение после 5 с недоступности основного узла
  cat > "${CONF}" <<CONF_EOF
port 26379
dir /data
sentinel monitor ${REDIS_SENTINEL_MASTER} ${REDIS_MASTER_IP} 6379 2
sentinel auth-pass ${REDIS_SENTINEL_MASTER} ${REDIS_PASSWORD}
sentinel down-after-milliseconds ${REDIS_SENTINEL_MASTER} 5000
sentinel failover-timeout ${REDIS_SENTINEL_MASTER} 10000
sentinel parallel-syncs ${REDIS_SENTINEL_MASTER} 1
CONF_EOF
  echo "==> Sentinel: новая конфигурация, основной узел ${REDIS_MASTER_IP}"
else
  echo "==> Sentinel: конфигурация из тома"
fi

exec redis-server "${CONF}" --sentinel
