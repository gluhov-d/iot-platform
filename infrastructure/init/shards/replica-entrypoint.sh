#!/bin/sh
# Точка входа реплики шарда (образ postgres, sh без bash; контейнер работает от пользователя postgres).
# Первый старт на пустом томе: pg_basebackup с шарда (слот REPLICATION_SLOT уже создан задачей shard-replication-init),
# -R включает режим standby и primary_slot_name. Дальше - обычный запуск postgres: реплика только для чтения,
# догоняет шард по WAL. Повторный старт данные не трогает.
set -eu

: "${PRIMARY_HOST:?PRIMARY_HOST не задан}"
: "${REPLICATION_SLOT:?REPLICATION_SLOT не задан}"
: "${REPLICATION_PASSWORD:?REPLICATION_PASSWORD не задан}"

PGDATA="${PGDATA:-/var/lib/postgresql/18/docker}"
export PGDATA

# Пароль - в файле паролей libpq (его читают и pg_basebackup, и walreceiver работающей реплики), а не в аргументах
# и не в postgresql.auto.conf. В файле паролей \ и : экранируются
escaped=$(printf '%s' "${REPLICATION_PASSWORD}" | sed -e 's/\\/\\\\/g' -e 's/:/\\:/g')
export PGPASSFILE=/tmp/.pgpass
printf '%s:5432:replication:replicator:%s\n' "${PRIMARY_HOST}" "${escaped}" > "${PGPASSFILE}"
chmod 600 "${PGPASSFILE}"

if [ ! -s "${PGDATA}/PG_VERSION" ]; then
  echo "==> реплика: пустой том, pg_basebackup с ${PRIMARY_HOST} (слот ${REPLICATION_SLOT})"
  mkdir -p "${PGDATA}"
  # Остатки прерванной копии: pg_basebackup требует пустой каталог
  find "${PGDATA}" -mindepth 1 -delete
  chmod 700 "${PGDATA}"
  attempt=0
  until pg_basebackup -h "${PRIMARY_HOST}" -U replicator -w -D "${PGDATA}" -X stream -c fast -R -S "${REPLICATION_SLOT}"; do
    attempt=$((attempt + 1))
    if [ "${attempt}" -ge 15 ]; then
      echo "!!! pg_basebackup не удался за ${attempt} попыток" >&2
      exit 1
    fi
    echo "    повтор через 2 с (${attempt}/15)"
    find "${PGDATA}" -mindepth 1 -delete
    sleep 2
  done
  echo "==> копия получена, запуск в режиме standby"
fi

exec docker-entrypoint.sh "$@"