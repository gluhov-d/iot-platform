#!/bin/sh
# Точка входа backup-runner: переменные окружения -> /etc/backup.env, расписание -> crontab, затем crond.
# crond не передаёт окружение заданиям, поэтому задания начинаются с `. /etc/backup.env` (права 600: в файле ключи MinIO).
# Расписание - BACKUP_SCHEDULE (cron, 5 полей, UTC); обе копии (PostgreSQL и ClickHouse) идут по одному расписанию.
# Вывод заданий уходит в журнал контейнера (/proc/1/fd/1).
set -eu

SCRIPTS=/opt/backup-runner
: "${BACKUP_SCHEDULE:?BACKUP_SCHEDULE не задана (cron, 5 полей)}"

# 5 полей из символов cron: иначе crond молча пропустит строку, а копий не будет
set -f
# shellcheck disable=SC2086
set -- $BACKUP_SCHEDULE
set +f
if [ "$#" -ne 5 ] || ! printf '%s' "$*" | grep -Eq '^[0-9*/,A-Za-z -]+$'; then
  echo "[FAIL] BACKUP_SCHEDULE='${BACKUP_SCHEDULE}': нужны 5 полей cron (например 0 3 * * *)" >&2
  exit 1
fi
schedule="$*"

# export -p печатает export NAME='значение' с экранированием кавычек; служебные переменные оболочки не нужны
( umask 077; export -p | grep -Ev "^export (PWD|OLDPWD|SHLVL|_)=" > /etc/backup.env )

JOB='. /etc/backup.env; %s >> /proc/1/fd/1 2>&1'
{
  # shellcheck disable=SC2059
  printf "%s /bin/sh -c '$JOB'\n" "${schedule}" "${SCRIPTS}/backup-postgres.sh"
  # shellcheck disable=SC2059
  printf "%s /bin/sh -c '$JOB'\n" "${schedule}" "${SCRIPTS}/backup-clickhouse.sh"
} > /etc/crontabs/root

echo "backup-runner: расписание '${schedule}' (UTC), задания:"
sed 's/^/    /' /etc/crontabs/root
for s in backup-postgres.sh backup-clickhouse.sh; do
  [ -x "${SCRIPTS}/${s}" ] || echo "[WARN] ${SCRIPTS}/${s} отсутствует или не исполняемый: задание завершится с ошибкой" >&2
done

# -f - на переднем плане (PID 1), -l 8 - обычный уровень журнала, вывод crond - в stderr контейнера
exec busybox crond -f -l 8 -L /dev/stderr
