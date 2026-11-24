#!/bin/sh
# Проверка MinIO на поднятом стенде (make minio-check): права учёток по бакетам и префиксам, сроки ILM,
# Object Lock (режим GOVERNANCE по умолчанию, запрет удаления версии), версии failed-events, условная запись If-None-Match на
# failed-events. Код 1, если проверка не прошла.
#
# Запуск без аргументов - с хоста (из Makefile, COMPOSE_CMD - команда compose с --env-file и -f): одноразовый
# контейнер minio-init выполняет этот же файл с аргументом --in-container. Учётки и секреты берутся из окружения
# контейнера, на хост они не выводятся. Бакет без Object Lock на живом стенде не создаётся, поэтому проверка
# «init падает без lock» выполняется отдельно на одноразовом стенде (см. отчёт о проверке фазы P2).
set -u

if [ "${1:-}" != "--in-container" ]; then
  : "${COMPOSE_CMD:?запускайте через make minio-check}"
  # shellcheck disable=SC2086
  exec $COMPOSE_CMD run --rm -T --no-deps --entrypoint /bin/sh minio-init /opt/init/config/check.sh --in-container
fi

EP=http://minio:9000
EMPTY_SHA=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
TS=$(date +%s)$$
ok=0
bad=0

for v in MINIO_ROOT_USER MINIO_ROOT_PASSWORD FAILED_EVENTS_BUCKET MINIO_BUCKET_MIMIR MINIO_BUCKET_LOKI \
  MINIO_BUCKET_TEMPO MINIO_BUCKET_BACKUPS FAILED_EVENTS_S3_ACCESS_KEY FAILED_EVENTS_S3_SECRET_KEY \
  MIMIR_S3_ACCESS_KEY MIMIR_S3_SECRET_KEY LOKI_S3_ACCESS_KEY LOKI_S3_SECRET_KEY TEMPO_S3_ACCESS_KEY \
  TEMPO_S3_SECRET_KEY BACKUP_POSTGRES_S3_ACCESS_KEY BACKUP_POSTGRES_S3_SECRET_KEY BACKUP_CLICKHOUSE_S3_ACCESS_KEY \
  BACKUP_CLICKHOUSE_S3_SECRET_KEY MINIO_EXPIRE_FAILED_EVENTS_DAYS FAILED_EVENTS_LOCK_DAYS MINIO_EXPIRE_BACKUPS_DAYS MINIO_EXPIRE_MIMIR_DAYS \
  MINIO_EXPIRE_LOKI_DAYS MINIO_EXPIRE_TEMPO_DAYS; do
  eval "_val=\${$v:-}"
  [ -n "$_val" ] || { echo "FAIL  переменная $v не задана"; exit 1; }
done

pass() { ok=$((ok + 1)); echo "PASS  $1"; }
fail() { bad=$((bad + 1)); echo "FAIL  $1"; }

# run ОПИСАНИЕ ok|denied КОМАНДА...: ok - команда завершилась успешно; denied - MinIO ответил Access Denied (403)
run() {
  _d=$1
  _want=$2
  shift 2
  _out=$("$@" 2>&1)
  _rc=$?
  if [ "$_want" = ok ]; then
    if [ "$_rc" -eq 0 ]; then pass "$_d"; else fail "$_d: $_out"; fi
  else
    case "$_out" in
      *"Access Denied"*|*"AccessDenied"*|*"access denied"*|*"Insufficient permissions"*|*"WORM protected"*) pass "$_d" ;;
      *) fail "$_d: ожидали 403, получили: $_out" ;;
    esac
  fi
}

mc alias set root "$EP" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null || { echo "FAIL  alias root"; exit 1; }
mc alias set fe "$EP" "$FAILED_EVENTS_S3_ACCESS_KEY" "$FAILED_EVENTS_S3_SECRET_KEY" >/dev/null
mc alias set mimir "$EP" "$MIMIR_S3_ACCESS_KEY" "$MIMIR_S3_SECRET_KEY" >/dev/null
mc alias set loki "$EP" "$LOKI_S3_ACCESS_KEY" "$LOKI_S3_SECRET_KEY" >/dev/null
mc alias set tempo "$EP" "$TEMPO_S3_ACCESS_KEY" "$TEMPO_S3_SECRET_KEY" >/dev/null
mc alias set bpg "$EP" "$BACKUP_POSTGRES_S3_ACCESS_KEY" "$BACKUP_POSTGRES_S3_SECRET_KEY" >/dev/null
mc alias set bch "$EP" "$BACKUP_CLICKHOUSE_S3_ACCESS_KEY" "$BACKUP_CLICKHOUSE_S3_SECRET_KEY" >/dev/null

B_FE=$FAILED_EVENTS_BUCKET
B_MIMIR=$MINIO_BUCKET_MIMIR
B_LOKI=$MINIO_BUCKET_LOKI
B_TEMPO=$MINIO_BUCKET_TEMPO
B_BK=$MINIO_BUCKET_BACKUPS
# Каталоги проверки: в каждом root создаёт r.txt; учётка пишет и читает только в своём каталоге
D_FE="$B_FE/_healthcheck/$TS"
D_MIMIR="$B_MIMIR/_healthcheck/$TS"
D_LOKI="$B_LOKI/_healthcheck/$TS"
D_TEMPO="$B_TEMPO/_healthcheck/$TS"
D_BPG="$B_BK/postgres/_healthcheck/$TS"
D_BCH="$B_BK/clickhouse/_healthcheck/$TS"
ALL_DIRS="$D_FE $D_MIMIR $D_LOKI $D_TEMPO $D_BPG $D_BCH"

echo "== подготовка: проверочные объекты (root)"
for d in $ALL_DIRS; do
  printf 'probe' | mc pipe "root/$d/r.txt" >/dev/null 2>&1 || { echo "FAIL  root не записал $d/r.txt"; exit 1; }
done

# owner_checks АЛИАС КАТАЛОГ_СВОЙ ОПИСАНИЕ: своя запись/чтение; чужие каталоги - 403 на чтение и запись
owner_checks() {
  _a=$1
  _own=$2
  _label=$3
  run "$_label: запись в свой каталог" ok sh -c "printf probe | mc pipe $_a/$_own/w.txt"
  run "$_label: чтение своего объекта" ok mc cat "$_a/$_own/r.txt"
  for _f in $ALL_DIRS; do
    [ "$_f" = "$_own" ] && continue
    run "$_label: чтение чужого $_f" denied mc cat "$_a/$_f/r.txt"
    run "$_label: запись в чужой $_f" denied sh -c "printf probe | mc pipe $_a/$_f/w.txt"
  done
}

echo "== права учёток"
# processor: только PutObject/GetObject на failed-events; ListBucket намеренно не выдан
owner_checks fe "$D_FE" "processor"
run "processor: LIST failed-events не выдан" denied mc ls "fe/$D_FE/"

# Условная запись If-None-Match: * на failed-events (lock и версии): первый PUT 200, второй 412, одна версия
cond_put() { # тело -> HTTP-код
  _sha=$(printf '%s' "$1" | sha256sum | cut -d' ' -f1)
  curl -s -o /dev/null -w '%{http_code}' -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "$FAILED_EVENTS_S3_ACCESS_KEY:$FAILED_EVENTS_S3_SECRET_KEY" \
    -H "x-amz-content-sha256: $_sha" -H 'If-None-Match: *' --data-binary "$1" \
    "$EP/$B_FE/_healthcheck/$TS/cond.txt"
}
_c1=$(cond_put v1)
_c2=$(cond_put v2)
[ "$_c1" = 200 ] && pass "условная запись If-None-Match: * -> 200" || fail "условная запись: первый PUT дал $_c1, ожидали 200"
[ "$_c2" = 412 ] && pass "условная запись: повтор -> 412" || fail "условная запись: повтор дал $_c2, ожидали 412"
_nv=$(mc ls --versions "root/$D_FE/cond.txt" 2>/dev/null | wc -l | tr -d ' ')
[ "$_nv" = 1 ] && pass "условная запись: одна версия объекта" || fail "условная запись: версий $_nv, ожидали 1"

# mimir, loki, tempo: свой бакет целиком; чужие бакеты и префиксы backups - 403
owner_checks mimir "$D_MIMIR" "mimir"
run "mimir: LIST своего бакета" ok mc ls "mimir/$B_MIMIR/"
run "mimir: LIST чужого бакета failed-events" denied mc ls "mimir/$B_FE/"
owner_checks loki "$D_LOKI" "loki"
run "loki: LIST своего бакета" ok mc ls "loki/$B_LOKI/"
owner_checks tempo "$D_TEMPO" "tempo"
run "tempo: LIST своего бакета" ok mc ls "tempo/$B_TEMPO/"

# backup-postgres / backup-clickhouse: только свой префикс backups/<копия>/
owner_checks bpg "$D_BPG" "backup-postgres"
run "backup-postgres: LIST своего префикса postgres/" ok mc ls "bpg/$B_BK/postgres/"
run "backup-postgres: LIST префикса clickhouse/ -> 403" denied mc ls "bpg/$B_BK/clickhouse/"
run "backup-postgres: LIST корня бакета backups -> 403" denied mc ls "bpg/$B_BK/"
owner_checks bch "$D_BCH" "backup-clickhouse"
run "backup-clickhouse: LIST своего префикса clickhouse/" ok mc ls "bch/$B_BK/clickhouse/"
run "backup-clickhouse: LIST префикса postgres/ -> 403" denied mc ls "bch/$B_BK/postgres/"

echo "== Object Lock, версии, ILM"
case "$(curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD" \
    -H "x-amz-content-sha256: $EMPTY_SHA" "$EP/$B_FE?object-lock")" in
  *"<ObjectLockEnabled>Enabled</ObjectLockEnabled>"*) pass "failed-events: Object Lock включён" ;;
  *) fail "failed-events: Object Lock не включён" ;;
esac
case "$(mc version info "root/$B_FE" 2>&1)" in
  *"versioning is enabled"*) pass "failed-events: версии включены" ;;
  *) fail "failed-events: версии не включены" ;;
esac

# Object Lock: режим GOVERNANCE и срок по умолчанию (FR-025, ADR-0019); правило читается по S3 API (mc retention info в этой сборке
# по умолчанию показывает «not enabled»)
_lock_cfg=$(curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD" \
  -H "x-amz-content-sha256: $EMPTY_SHA" "$EP/$B_FE?object-lock")
case "$_lock_cfg" in
  *"<Mode>GOVERNANCE</Mode>"*"<Days>$FAILED_EVENTS_LOCK_DAYS</Days>"*) pass "failed-events: режим GOVERNANCE, срок $FAILED_EVENTS_LOCK_DAYS дн. по умолчанию" ;;
  *) fail "failed-events: ожидали режим GOVERNANCE на $FAILED_EVENTS_LOCK_DAYS дн. по умолчанию, получили: $_lock_cfg" ;;
esac

# Новый объект нельзя удалить: версию не удалит ни процессор (нет права), ни администратор без обхода режима; с обходом - можно
printf probe | mc pipe "fe/$D_FE/lock.txt" >/dev/null 2>&1 || fail "processor не записал $D_FE/lock.txt"
_lv=$(mc ls --versions --json "root/$D_FE/lock.txt" 2>/dev/null)
_vid=${_lv#*\"versionId\":\"}
_vid=${_vid%%\"*}
if [ -z "$_vid" ] || [ "$_vid" = "$_lv" ]; then
  fail "не получен идентификатор версии lock.txt: $_lv"
else
  run "processor: удаление версии объекта запрещено" denied mc rm --version-id "$_vid" "fe/$D_FE/lock.txt"
  run "администратор: удаление версии без обхода режима запрещено" denied mc rm --version-id "$_vid" "root/$D_FE/lock.txt"
  run "администратор: удаление версии с обходом режима GOVERNANCE" ok mc rm --bypass --version-id "$_vid" "root/$D_FE/lock.txt"
fi

ilm_check() { # бакет дни
  case "$(mc ilm rule ls --json "root/$1" 2>&1)" in
    *"\"Days\":$2}"*) pass "ILM $1: срок $2 дн." ;;
    *) fail "ILM $1: ожидали срок $2 дн." ;;
  esac
}
ilm_check "$B_FE" "$MINIO_EXPIRE_FAILED_EVENTS_DAYS"
ilm_check "$B_MIMIR" "$MINIO_EXPIRE_MIMIR_DAYS"
ilm_check "$B_LOKI" "$MINIO_EXPIRE_LOKI_DAYS"
ilm_check "$B_TEMPO" "$MINIO_EXPIRE_TEMPO_DAYS"
ilm_check "$B_BK" "$MINIO_EXPIRE_BACKUPS_DAYS"

echo "== уборка проверочных каталогов"
# Объекты failed-events под блокировкой: удаляются все версии с обходом режима GOVERNANCE (учётка root)
for d in $ALL_DIRS "$B_FE/_healthcheck/$TS/cond.txt"; do
  case "$d" in
    "$B_FE/"*) mc rm --recursive --force --versions --bypass "root/$d" >/dev/null 2>&1 || true ;;
    *) mc rm --recursive --force "root/$d" >/dev/null 2>&1 || true ;;
  esac
done

echo "ИТОГО: pass=$ok fail=$bad"
[ "$bad" -eq 0 ]
