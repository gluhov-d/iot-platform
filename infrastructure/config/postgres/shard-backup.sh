#!/usr/bin/env bash
# Резервная копия каждого шарда с его РЕПЛИКИ, без остановки сервисов (make shard-backup):
#
#   infrastructure/config/postgres/shard-backup.sh        -> backups/<UTC-время>/
#
# Копию делает backup-postgres.sh в контейнере backup-runner (его же запускает cron - копия в MinIO): второй реализации нет.
# Здесь только запуск с BACKUP_TARGET=dir: копия остаётся на хосте в backups/<UTC-время>/ (каталог ./backups смонтирован
# в контейнер как /backups) - это тот каталог, из которого shard-restore.sh восстанавливает без MinIO.
# Состав копии (shard_N.dump, manifest.txt), порядок действий и блокировка - в шапке config/backup-runner/backup-postgres.sh.
set -euo pipefail

# shellcheck source=shard-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/shard-lib.sh"

[[ -n "$("${COMPOSE[@]}" ps -q --status running backup-runner)" ]] \
  || { echo "[FAIL] backup-runner is not running: make up" >&2; exit 1; }
[[ "${BACKUP_ROOT}" == "${ROOT_DIR}/backups" ]] \
  || echo "[WARN] BACKUP_ROOT=${BACKUP_ROOT} is ignored: the copy goes to ${ROOT_DIR}/backups (mounted into backup-runner)" >&2

exec "${COMPOSE[@]}" exec -T -e BACKUP_TARGET=dir -e BACKUP_DIR=/backups backup-runner /opt/backup-runner/backup-postgres.sh < /dev/null
