#!/bin/sh
# Разовая проверка удаления версий по ILM бакета с Object Lock (R27, T096): make minio-ilm-expiry-check STEP=start|verify.
# Что проверяет: правила failed-events (те же, что у стенда: render из init/minio/init.sh и ilm/failed-events.json) со сроками
# 1 и 1 сутки удаляют и прежнюю версию, и пометку удаления объекта, а не только запись в обычном списке.
# start поднимает ОДНОРАЗОВЫЙ контейнер iot-ilm-expiry-minio (образ MinIO того же тега, что стенд; том iot-ilm-expiry-data;
# без проброса портов; --restart unless-stopped), создаёт бакет ilm-check с Object Lock (GOVERNANCE, 1 сутки), применяет
# правила и пишет два объекта: twice.txt - две версии, once.txt - одна. Печатает время записи и что делать дальше.
# verify проверяет оба объекта двумя отдельными условиями: нет в обычном списке (mc ls) и нет в списке версий
# (mc ls --versions). Коды: 0 - удалено, контейнер и том убраны; 1 - провал, контейнер и том оставлены;
# 2 - рано (пометки ещё нет, прошло меньше 48 ч), ничего не удаляется.
# Ничего не трогает на стенде: своих сетей и портов нет, бакеты стенда, учётки и .env не используются.
# Пароль root случайный: передаётся контейнеру через окружение, на экран не выводится; verify берёт его из контейнера.
# Скорость сканера: speed=fastest в конфигурации MinIO (mc admin config set, состояние печатает start; переменная
# MINIO_SCANNER_SPEED в этой сборке значение конфигурации не меняет). Образ - MINIO_IMAGE и MINIO_VERSION из окружения,
# иначе из .env.example (только этот инструмент читает пример; .env не читается).
# Имя контейнера и тома можно сменить ILM_EXPIRY_NAME (для собственных проверок скрипта; по умолчанию iot-ilm-expiry).
# Совместим с /bin/sh и bash 3.2 (macOS): только POSIX-конструкции, ассоциативных массивов нет.
set -eu

ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
NAME=${ILM_EXPIRY_NAME:-iot-ilm-expiry}
CT=$NAME-minio
VOL=$NAME-data
BUCKET=ilm-check
STATE=/tmp/ilm-expiry-check.state
ENV_EXAMPLE=$ROOT/.env.example
INIT=$ROOT/infrastructure/init/minio/init.sh
RULES_TPL=$ROOT/infrastructure/config/minio/ilm/failed-events.json
MC_ALIAS='mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null'

say() { echo "[ilm-expiry] $*"; }
die() { echo "[ilm-expiry] ОШИБКА: $*" >&2; exit 1; }

# env_value ПЕРЕМЕННАЯ: значение из .env.example
env_value() {
  while IFS= read -r _l; do
    case "$_l" in
      "$1="*) printf '%s\n' "${_l#*=}"; return 0 ;;
    esac
  done < "$ENV_EXAMPLE"
  return 1
}

image() {
  _i=${MINIO_IMAGE:-}
  _v=${MINIO_VERSION:-}
  [ -n "$_i" ] || _i=$(env_value MINIO_IMAGE || true)
  [ -n "$_v" ] || _v=$(env_value MINIO_VERSION || true)
  [ -n "$_i" ] && [ -n "$_v" ] || die "не задан образ MinIO (MINIO_IMAGE, MINIO_VERSION)"
  printf '%s:%s\n' "$_i" "$_v"
}

rnd() { head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24; }

wait_ready() {
  _n=0
  until docker exec "$CT" curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; do
    _n=$((_n + 1))
    if [ "$_n" -gt 60 ]; then
      docker logs --tail 20 "$CT" 2>&1 || true
      die "MinIO в $CT не отвечает"
    fi
    sleep 2
  done
}

start() {
  if docker inspect "$CT" >/dev/null 2>&1; then
    say "контейнер $CT уже есть: ничего не пересоздано, отсчёт не сброшен"
    docker exec "$CT" cat "$STATE" 2>/dev/null || say "запись о времени не найдена (контейнер не запущен?)"
    say "проверка: make minio-ilm-expiry-check STEP=verify"
    return 0
  fi
  if docker volume inspect "$VOL" >/dev/null 2>&1; then
    die "том $VOL есть без контейнера: пароль неизвестен; удалите том вручную (docker volume rm $VOL) и повторите"
  fi
  IMG=$(image)
  _tmp=$(mktemp -d)
  trap 'rm -rf "$_tmp"' EXIT
  MINIO_ROOT_USER=ilm-check
  MINIO_ROOT_PASSWORD=$(rnd)
  export MINIO_ROOT_USER MINIO_ROOT_PASSWORD

  docker run -d --name "$CT" --restart unless-stopped \
    -e MINIO_ROOT_USER -e MINIO_ROOT_PASSWORD -e MINIO_PROMETHEUS_AUTH_TYPE=public \
    -v "$VOL:/data" "$IMG" server /data --console-address :9001 >/dev/null
  wait_ready
  say "MinIO $IMG поднят одноразово (без портов на хосте)"

  docker exec -i "$CT" sh -s <<'EOF' || die "не создан бакет ilm-check с Object Lock"
set -eu
mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
mc admin config set local scanner speed=fastest >/dev/null
echo "scanner: $(mc admin config get local scanner 2>&1)"
mc mb --with-lock local/ilm-check >/dev/null
LOCK_XML='<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>'
LOCK_SHA=$(printf '%s' "$LOCK_XML" | sha256sum | cut -d' ' -f1)
LOCK_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD" -H "x-amz-content-sha256: $LOCK_SHA" --data-binary "$LOCK_XML" \
  "http://localhost:9000/ilm-check?object-lock")
echo "object-lock: GOVERNANCE, 1 сутки по умолчанию, HTTP $LOCK_CODE"
[ "$LOCK_CODE" = 200 ] || exit 1
EOF

  # Правила - тот же render, что у init.sh: функции читаются из init.sh и применяются к шаблону failed-events.json со сроками 1 и 1
  sed -n '/^subst() {/,/^}/p;/^render() {/,/^}/p' "$INIT" > "$_tmp/render.sh"
  sh -c '. "$1"; render "$2" 1 1' sh "$_tmp/render.sh" "$RULES_TPL" > "$_tmp/rules.json"
  [ -s "$_tmp/rules.json" ] || die "render не выдал правила"
  docker exec -i "$CT" sh -c "$MC_ALIAS; mc ilm import local/$BUCKET >/dev/null && mc ilm rule ls --json local/$BUCKET" \
    < "$_tmp/rules.json" || die "правила не приняты бакетом с lock"
  say "правила применены (см. JSON выше)"

  docker exec -i "$CT" sh -s <<'EOF' || die "не записаны объекты"
set -eu
mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
printf 'twice v1 %s\n' "$(date -u +%FT%TZ)" | mc pipe local/ilm-check/twice.txt >/dev/null
sleep 5
printf 'twice v2 %s\n' "$(date -u +%FT%TZ)" | mc pipe local/ilm-check/twice.txt >/dev/null
printf 'once %s\n' "$(date -u +%FT%TZ)" | mc pipe local/ilm-check/once.txt >/dev/null
echo "$(date -u +%s) $(date -u +%FT%TZ)" > /tmp/ilm-expiry-check.state
echo "--- mc ls --versions local/ilm-check (сразу после записи)"
mc ls --versions local/ilm-check
EOF

  say "записано (UTC, эпоха и время): $(docker exec "$CT" cat "$STATE")"
  say "ждать 2-4 суток: сроки 1 и 1 сутки, lock 1 сутки; сканер ведёт проход в фоне"
  say "проверка: make minio-ilm-expiry-check STEP=verify (код 2 - рано, 1 - провал, 0 - удалено и убрано)"
}

verify() {
  docker inspect "$CT" >/dev/null 2>&1 || die "контейнера $CT нет: нечего проверять (start поднимает его)"
  _st=$(docker exec "$CT" cat "$STATE" 2>/dev/null) || die "нет записи о времени в $CT (контейнер не запущен?)"
  set -- $_st
  _ep=$1
  _iso=$2
  _now=$(date -u +%s)
  _h=$(( (_now - _ep) / 3600 ))
  say "записано $_iso (UTC), прошло $_h ч"

  _rc=0
  docker exec "$CT" sh -c "$MC_ALIAS; mc ls local/$BUCKET/ >/dev/null" || _rc=$?
  [ "$_rc" -eq 0 ] || die "бакет $BUCKET не отвечает (код $_rc): объекты не проверены, контейнер оставлен"

  _fail=0
  _early=0
  for _o in twice.txt once.txt; do
    _ls=$(docker exec "$CT" sh -c "$MC_ALIAS; mc ls local/$BUCKET/$_o 2>&1")
    echo "\$ mc ls local/$BUCKET/$_o"
    printf '%s\n' "$_ls"
    _lv=$(docker exec "$CT" sh -c "$MC_ALIAS; mc ls --versions local/$BUCKET/$_o 2>&1")
    echo "\$ mc ls --versions local/$BUCKET/$_o"
    printf '%s\n' "$_lv"
    case "$_ls" in *"$_o"*) _cur=1 ;; *) _cur=0 ;; esac
    case "$_lv" in *"$_o"*) _ver=1 ;; *) _ver=0 ;; esac
    if [ "$_cur" = 0 ] && [ "$_ver" = 0 ]; then
      echo "PASS  $_o: нет ни в mc ls, ни в mc ls --versions"
    elif [ "$_cur" = 0 ]; then
      _fail=1
      echo "FAIL  $_o: пометка есть, версия осталась (mc ls --versions)"
    elif [ "$_h" -lt 48 ]; then
      _early=1
      echo "РАНО  $_o: пометки ещё нет, прошло $_h ч (меньше 48 ч)"
    else
      _fail=1
      echo "FAIL  $_o: через $_h ч объект всё ещё в mc ls, пометки удаления нет"
    fi
  done

  if [ "$_fail" -eq 1 ]; then
    echo "[ilm-expiry] ПРОВАЛ: $CT и $VOL оставлены для разбора"
    exit 1
  fi
  if [ "$_early" -eq 1 ]; then
    echo "[ilm-expiry] РАНО: прошло $_h ч, ничего не удалено; $CT и $VOL оставлены"
    exit 2
  fi
  docker rm -f "$CT" >/dev/null
  docker volume rm "$VOL" >/dev/null
  say "ПРОЙДЕНО: версии и пометки удалены; $CT и $VOL убраны"
  exit 0
}

case "${1:-}" in
  start) start ;;
  verify) verify ;;
  *) echo "Использование: $0 start|verify" >&2; exit 1 ;;
esac
