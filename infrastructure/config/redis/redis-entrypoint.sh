#!/bin/sh
# Точка входа узла Redis (образ redis:alpine, sh без bash). Роль узла - основной или реплика - узнаётся у Sentinel
# при каждом старте, а не задаётся командной строкой (research.md R14): перезапущенный бывший основной узел, которого
# Sentinel сменил, встаёт репликой и не перезаписывает данные нового основного узла.
#
# Переменные окружения (из compose):
#   REDIS_SELF_IP         - статический адрес этого узла;
#   REDIS_MASTER_IP       - адрес основного узла при первом запуске (тот же, что у Sentinel);
#   REDIS_SENTINEL_IPS    - адреса трёх Sentinel через запятую;
#   REDIS_SENTINEL_MASTER - имя основного узла в Sentinel;
#   REDIS_PASSWORD        - пароль узлов Redis (он же для репликации);
#   REDIS_MAXMEMORY       - предел памяти; вытесняются только ключи с TTL (volatile-lru, ADR-0008);
#   REDIS_DEFAULT_ROLE    - запасная роль (master | replica), если ни один Sentinel не ответил.
set -eu

: "${REDIS_SELF_IP:?REDIS_SELF_IP не задан}"
: "${REDIS_MASTER_IP:?REDIS_MASTER_IP не задан}"
: "${REDIS_SENTINEL_IPS:?REDIS_SENTINEL_IPS не задан}"
: "${REDIS_SENTINEL_MASTER:?REDIS_SENTINEL_MASTER не задан}"
: "${REDIS_PASSWORD:?REDIS_PASSWORD не задан}"
: "${REDIS_MAXMEMORY:?REDIS_MAXMEMORY не задан}"
: "${REDIS_DEFAULT_ROLE:?REDIS_DEFAULT_ROLE не задан}"

# REDISCLI_AUTH нужен для проверки здоровья и для клиентов узла; Sentinel без пароля, поэтому к нему не подключаемся с ним
unset REDISCLI_AUTH

# Спрашивает Sentinel по очереди, печатает адрес основного узла. Пустой ответ - Sentinel недоступен или не знает узла
ask_sentinel() {
  for ip in $(echo "${REDIS_SENTINEL_IPS}" | tr ',' ' '); do
    answer=$(redis-cli -h "${ip}" -p 26379 sentinel get-master-addr-by-name "${REDIS_SENTINEL_MASTER}" 2>/dev/null | sed -n 1p | tr -d '\r') || answer=""
    if [ -n "${answer}" ]; then
      echo "${answer}"
      return 0
    fi
  done
  return 1
}

# Несколько коротких попыток: при первом старте стенда Sentinel ещё не поднят, тогда работает запасная роль
master_ip=""
attempt=0
while [ "${attempt}" -lt 3 ]; do
  if master_ip=$(ask_sentinel); then
    break
  fi
  master_ip=""
  attempt=$((attempt + 1))
  sleep 1
done

if [ -n "${master_ip}" ]; then
  if [ "${master_ip}" = "${REDIS_SELF_IP}" ]; then
    role=master
  else
    role=replica
  fi
  echo "==> Sentinel: основной узел ${master_ip}, этот узел (${REDIS_SELF_IP}) - ${role}"
else
  role="${REDIS_DEFAULT_ROLE}"
  master_ip="${REDIS_MASTER_IP}"
  echo "==> Sentinel не ответил: запасная роль ${role} (основной узел ${master_ip})"
fi

# Аргументы сервера. Пароль - в аргументах, как и раньше; файл паролей не вводим, чтобы не менять образ
set -- redis-server --requirepass "${REDIS_PASSWORD}" --masterauth "${REDIS_PASSWORD}" \
  --appendonly yes --maxmemory "${REDIS_MAXMEMORY}" --maxmemory-policy volatile-lru \
  --replica-announce-ip "${REDIS_SELF_IP}"
if [ "${role}" = replica ]; then
  set -- "$@" --replicaof "${master_ip}" 6379
fi

# docker-entrypoint.sh образа понижает права до пользователя redis, когда первый аргумент - redis-server
exec docker-entrypoint.sh "$@"
