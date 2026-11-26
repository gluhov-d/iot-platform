#!/bin/sh
# Одноразовая инициализация MinIO: сервис minio-init (образ MinIO, базовый стенд, compose).
# Делает: бакеты (failed-events - с Object Lock и версиями, остальные - обычные), режим GOVERNANCE по умолчанию для объектов
# failed-events, ILM по бакетам,
# политики компонентов и учётки компонентов. Идемпотентна: повторный запуск ничего не меняет и объекты не трогает.
# Имена бакетов, сроки и учётки берутся из окружения. Шаблоны в config/minio/ получают значения подстановкой
# токенов: в образе нет sed и awk.
# Бакет failed-events, уже существующий БЕЗ Object Lock, не переделывается: lock задаётся только при создании
# (S3 отвечает 409). Скрипт завершается с кодом 1 и причиной.
set -eu

CONFIG=${MINIO_CONFIG_DIR:-/opt/init/config}
EP=http://minio:9000
EMPTY_SHA=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
POLICY_FILE=/tmp/minio-init-policy.json

say() { echo "[minio-init] $*"; }
die() { echo "[minio-init] ОШИБКА: $*" >&2; exit 1; }

: "${MINIO_ROOT_USER:?MINIO_ROOT_USER не задан}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD не задан}"
: "${FAILED_EVENTS_BUCKET:?FAILED_EVENTS_BUCKET не задан}"
: "${FAILED_EVENTS_LOCK_DAYS:?FAILED_EVENTS_LOCK_DAYS не задан}"
: "${MINIO_BUCKET_MIMIR:?MINIO_BUCKET_MIMIR не задан}"
: "${MINIO_BUCKET_LOKI:?MINIO_BUCKET_LOKI не задан}"
: "${MINIO_BUCKET_TEMPO:?MINIO_BUCKET_TEMPO не задан}"
: "${MINIO_BUCKET_BACKUPS:?MINIO_BUCKET_BACKUPS не задан}"
: "${MINIO_EXPIRE_FAILED_EVENTS_DAYS:?MINIO_EXPIRE_FAILED_EVENTS_DAYS не задан}"
: "${MINIO_EXPIRE_FAILED_EVENTS_NONCURRENT_DAYS:?MINIO_EXPIRE_FAILED_EVENTS_NONCURRENT_DAYS не задан}"
: "${MINIO_EXPIRE_BACKUPS_DAYS:?MINIO_EXPIRE_BACKUPS_DAYS не задан}"
: "${MINIO_EXPIRE_MIMIR_DAYS:?MINIO_EXPIRE_MIMIR_DAYS не задан}"
: "${MINIO_EXPIRE_LOKI_DAYS:?MINIO_EXPIRE_LOKI_DAYS не задан}"
: "${MINIO_EXPIRE_TEMPO_DAYS:?MINIO_EXPIRE_TEMPO_DAYS не задан}"
: "${FAILED_EVENTS_S3_ACCESS_KEY:?FAILED_EVENTS_S3_ACCESS_KEY не задан}"
: "${FAILED_EVENTS_S3_SECRET_KEY:?FAILED_EVENTS_S3_SECRET_KEY не задан}"
: "${MIMIR_S3_ACCESS_KEY:?MIMIR_S3_ACCESS_KEY не задан}"
: "${MIMIR_S3_SECRET_KEY:?MIMIR_S3_SECRET_KEY не задан}"
: "${LOKI_S3_ACCESS_KEY:?LOKI_S3_ACCESS_KEY не задан}"
: "${LOKI_S3_SECRET_KEY:?LOKI_S3_SECRET_KEY не задан}"
: "${TEMPO_S3_ACCESS_KEY:?TEMPO_S3_ACCESS_KEY не задан}"
: "${TEMPO_S3_SECRET_KEY:?TEMPO_S3_SECRET_KEY не задан}"
: "${BACKUP_POSTGRES_S3_ACCESS_KEY:?BACKUP_POSTGRES_S3_ACCESS_KEY не задан}"
: "${BACKUP_POSTGRES_S3_SECRET_KEY:?BACKUP_POSTGRES_S3_SECRET_KEY не задан}"
: "${BACKUP_CLICKHOUSE_S3_ACCESS_KEY:?BACKUP_CLICKHOUSE_S3_ACCESS_KEY не задан}"
: "${BACKUP_CLICKHOUSE_S3_SECRET_KEY:?BACKUP_CLICKHOUSE_S3_SECRET_KEY не задан}"

# subst ТОКЕН ЗНАЧЕНИЕ ТЕКСТ: заменяет все вхождения ТОКЕНА в ТЕКСТЕ (только ${x%%..} и ${x#..}, без sed)
subst() {
  _st=$3
  _so=""
  while :; do
    case "$_st" in
      *"$1"*)
        _so="${_so}${_st%%"$1"*}$2"
        _st=${_st#*"$1"}
        ;;
      *) break ;;
    esac
  done
  printf '%s' "${_so}${_st}"
}

# render ШАБЛОН [ДНИ] [ДНИ_ВЕРСИЙ]: текст шаблона с подставленными бакетами и сроками ILM.
# "__EXPIRE_DAYS__" в кавычках становится числом (JSON), голый токен - строкой (ID правила).
# ДНИ_ВЕРСИЙ - срок прежних версий (только шаблон failed-events, R27)
render() {
  _rd=$(cat "$1")
  _rd=$(subst __BUCKET_FAILED_EVENTS__ "$FAILED_EVENTS_BUCKET" "$_rd")
  _rd=$(subst __BUCKET_MIMIR__ "$MINIO_BUCKET_MIMIR" "$_rd")
  _rd=$(subst __BUCKET_LOKI__ "$MINIO_BUCKET_LOKI" "$_rd")
  _rd=$(subst __BUCKET_TEMPO__ "$MINIO_BUCKET_TEMPO" "$_rd")
  _rd=$(subst __BUCKET_BACKUPS__ "$MINIO_BUCKET_BACKUPS" "$_rd")
  _rd=$(subst '"__EXPIRE_DAYS__"' "${2:-}" "$_rd")
  _rd=$(subst __EXPIRE_DAYS__ "${2:-}" "$_rd")
  _rd=$(subst '"__NONCURRENT_DAYS__"' "${3:-}" "$_rd")
  _rd=$(subst __NONCURRENT_DAYS__ "${3:-}" "$_rd")
  printf '%s\n' "$_rd"
}

mc alias set local "$EP" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null || die "не задан alias MinIO"
mc ls local >/dev/null 2>&1 || die "MinIO не отвечает по адресу $EP"

# lock_on БАКЕТ: 0, если на бакете включён Object Lock (GET ?object-lock; mc retention для этого не годится)
lock_on() {
  case "$(curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD" \
      -H "x-amz-content-sha256: $EMPTY_SHA" "$EP/$1?object-lock")" in
    *"<ObjectLockEnabled>Enabled</ObjectLockEnabled>"*) return 0 ;;
  esac
  return 1
}

ensure_bucket() { # бакет без Object Lock
  if mc ls "local/$1" >/dev/null 2>&1; then
    say "бакет $1 есть"
  else
    mc mb "local/$1" >/dev/null || die "не создан бакет $1"
    say "бакет $1 создан"
  fi
}

# --- 1. Бакеты ---------------------------------------------------------------------------------
if mc ls "local/$FAILED_EVENTS_BUCKET" >/dev/null 2>&1; then
  lock_on "$FAILED_EVENTS_BUCKET" || die "бакет $FAILED_EVENTS_BUCKET существует БЕЗ Object Lock (или запрос к MinIO не прошёл). Lock включается только при создании бакета: пересоздайте бакет пустым (или перенесите объекты) и повторите init"
  say "бакет $FAILED_EVENTS_BUCKET: Object Lock включён"
else
  mc mb --with-lock "local/$FAILED_EVENTS_BUCKET" >/dev/null || die "не создан бакет $FAILED_EVENTS_BUCKET"
  say "бакет $FAILED_EVENTS_BUCKET создан (Object Lock, версии)"
fi
# Режим по умолчанию (FR-025, ADR-0019): каждый новый объект failed-events защищён режимом GOVERNANCE на FAILED_EVENTS_LOCK_DAYS суток
# (равен сроку ILM): версию не удалит ни процессор (нет права), ни администратор без обхода режима (mc rm --bypass). Условная
# запись If-None-Match: * режим не меняет. Правило задаётся по S3 API (PUT ?object-lock): mc retention в этой сборке работает
# с объектами и «Object locking is not enabled» показывает даже на бакете с lock. Уже записанные объекты правило не защищает.
LOCK_XML="<ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>$FAILED_EVENTS_LOCK_DAYS</Days></DefaultRetention></Rule></ObjectLockConfiguration>"
LOCK_SHA=$(printf '%s' "$LOCK_XML" | sha256sum | cut -d' ' -f1)
LOCK_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" --user "$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD" \
  -H "x-amz-content-sha256: $LOCK_SHA" --data-binary "$LOCK_XML" "$EP/$FAILED_EVENTS_BUCKET?object-lock")
[ "$LOCK_CODE" = 200 ] || die "не задан режим GOVERNANCE для $FAILED_EVENTS_BUCKET (ответ $LOCK_CODE)"
say "бакет $FAILED_EVENTS_BUCKET: режим GOVERNANCE по умолчанию, срок $FAILED_EVENTS_LOCK_DAYS дн."
ensure_bucket "$MINIO_BUCKET_MIMIR"
ensure_bucket "$MINIO_BUCKET_LOKI"
ensure_bucket "$MINIO_BUCKET_TEMPO"
ensure_bucket "$MINIO_BUCKET_BACKUPS"

# --- 2. ILM (mc ilm import заменяет правила бакета целиком: повтор не дублирует) -------------------
ilm() { # бакет дни шаблон [дни_версий]
  render "$CONFIG/ilm/$3.json" "$2" "${4:-}" | mc ilm import "local/$1" >/dev/null || die "не задан ILM для $1"
  say "ILM $1: срок $2 дн."
}
ilm "$FAILED_EVENTS_BUCKET" "$MINIO_EXPIRE_FAILED_EVENTS_DAYS" failed-events "$MINIO_EXPIRE_FAILED_EVENTS_NONCURRENT_DAYS"
ilm "$MINIO_BUCKET_BACKUPS" "$MINIO_EXPIRE_BACKUPS_DAYS" backups
ilm "$MINIO_BUCKET_MIMIR" "$MINIO_EXPIRE_MIMIR_DAYS" mimir-blocks
ilm "$MINIO_BUCKET_LOKI" "$MINIO_EXPIRE_LOKI_DAYS" loki-chunks
ilm "$MINIO_BUCKET_TEMPO" "$MINIO_EXPIRE_TEMPO_DAYS" tempo-traces

# --- 3. Политики (mc admin policy create перезаписывает существующую) -----------------------------
for p in failed-events-processor mimir loki tempo backup-postgres backup-clickhouse; do
  render "$CONFIG/policies/$p.json" > "$POLICY_FILE"
  mc admin policy create local "$p" "$POLICY_FILE" >/dev/null || die "не создана политика $p"
done
rm -f "$POLICY_FILE"
say "политики обновлены"

# --- 4. Учётки: mc admin user add создаёт учётку или задаёт ей секрет из окружения; политика привязана ----
user() { # учётка секрет политика
  mc admin user add local "$1" "$2" >/dev/null || die "не создана учётка $1"
  mc admin policy attach local "$3" --user "$1" >/dev/null || die "политика $3 не привязана к учётке $1"
  case "$(mc admin user info local "$1" 2>/dev/null)" in
    *"PolicyName: $3"*) ;;
    *) die "у учётки $1 нет политики $3" ;;
  esac
  say "учётка $1: политика $3"
}
user "$FAILED_EVENTS_S3_ACCESS_KEY" "$FAILED_EVENTS_S3_SECRET_KEY" failed-events-processor
user "$MIMIR_S3_ACCESS_KEY" "$MIMIR_S3_SECRET_KEY" mimir
user "$LOKI_S3_ACCESS_KEY" "$LOKI_S3_SECRET_KEY" loki
user "$TEMPO_S3_ACCESS_KEY" "$TEMPO_S3_SECRET_KEY" tempo
user "$BACKUP_POSTGRES_S3_ACCESS_KEY" "$BACKUP_POSTGRES_S3_SECRET_KEY" backup-postgres
user "$BACKUP_CLICKHOUSE_S3_ACCESS_KEY" "$BACKUP_CLICKHOUSE_S3_SECRET_KEY" backup-clickhouse

say "init OK"
