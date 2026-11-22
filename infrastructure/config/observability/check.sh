#!/usr/bin/env bash
# Проверка наблюдаемости на поднятом стенде (make obs-check, профиль observability): Grafana и её источники,
# все PromQL всех дашбордов (ответ 200 и status=success, без 400), up{job} запущенных компонентов,
# объекты Mimir, Loki и Tempo в бакетах MinIO. Код 1, если проверка не прошла.
#
# Запуск через make obs-check (COMPOSE_CMD - команда compose с --env-file и -f). На хосте нужны docker, curl не нужен:
# запросы идут через Grafana (источники prometheus, loki, tempo), поэтому одновременно проверяется и сам источник.
# Для разбора JSON нужен python3 (есть на macOS с Command Line Tools и в Linux). Учётка Grafana и ключи MinIO берутся
# из окружения контейнеров, на хост и в вывод не попадают.
#
# Перед подсчётом объектов скрипт отправляет Alloy пробный спан (OTLP, сервис obs-check: путь Alloy -> Tempo -> MinIO
# проверяется и без трафика сервисов), затем просит Mimir, Loki и Tempo сбросить данные в хранилище
# (POST /ingester/flush, /flush, /flush): без этого блоки Mimir появляются в бакете примерно через два часа,
# чанки Loki - через полчаса простоя, блоки Tempo - после max_block_duration. Пробный трейс читается из Tempo через Grafana.
# Побочный эффект: после принудительного сброса Mimir отклоняет отсчёты, отстающие на секунды (err-mimir-sample-timestamp-too-old),
# поэтому в метриках за момент запуска может быть пропуск не длиннее интервала сбора; данные сервисов и MinIO это не затрагивает.
set -u

: "${COMPOSE_CMD:?запускайте через make obs-check}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DASH_DIR="${ROOT_DIR}/infrastructure/config/grafana/dashboards"
TIMEOUT="${CHECK_TIMEOUT:-120}"

# shellcheck disable=SC2086
compose() { $COMPOSE_CMD "$@"; }

command -v python3 >/dev/null 2>&1 || { echo "FAIL  нужен python3 (разбор JSON)"; exit 1; }

echo "== подготовка"
running="$(compose ps --services --status running 2>/dev/null | tr '\n' ' ')"
case " ${running} " in
  *" grafana "*) ;;
  *) echo "FAIL  grafana не запущена: make up PROFILES=observability"; exit 1 ;;
esac

gport="$(compose port grafana 3000 2>/dev/null | head -1 | sed 's/.*://')"
[ -n "${gport}" ] || { echo "FAIL  не удалось получить порт grafana (docker compose port)"; exit 1; }
creds="$(compose exec -T grafana sh -c 'printf "%s:%s" "$GF_SECURITY_ADMIN_USER" "$GF_SECURITY_ADMIN_PASSWORD"' 2>/dev/null)"
[ -n "${creds}" ] || { echo "FAIL  не удалось получить учётку Grafana из контейнера"; exit 1; }

env_dump="$(compose config --environment 2>/dev/null)"
v() { printf '%s\n' "${env_dump}" | sed -n "s/^$1=//p" | head -1; }
mimir_port="$(v MIMIR_PORT)"
loki_port=3100
tempo_port=3200
# OTLP/HTTP-приёмник Alloy внутри сети compose (config.alloy: otelcol.receiver.otlp, http)
alloy_otlp_port=4320
b_mimir="$(v MINIO_BUCKET_MIMIR)"
b_loki="$(v MINIO_BUCKET_LOKI)"
b_tempo="$(v MINIO_BUCKET_TEMPO)"
for n in mimir_port b_mimir b_loki b_tempo; do
  eval "_val=\${$n}"
  [ -n "${_val}" ] || { echo "FAIL  Compose не отдал значение для ${n}"; exit 1; }
done

echo "== объекты в MinIO (после сброса данных в хранилище)"
# Внутри контейнера minio-init (в нём sh, curl и mc; нет grep, sed, awk): сброс, затем ожидание появления объектов.
# Бакет Mimir - префикс blocks/ (объекты в корне Mimir принимает за тенантов).
# shellcheck disable=SC2016
bucket_out="$(compose run --rm -T --no-deps \
  -e MIMIR_PORT="${mimir_port}" -e LOKI_PORT="${loki_port}" -e TEMPO_PORT="${tempo_port}" -e ALLOY_OTLP_PORT="${alloy_otlp_port}" \
  -e CHECK_TIMEOUT="${TIMEOUT}" -e CHK_MIMIR="${b_mimir}/blocks/" -e CHK_LOKI="${b_loki}/" -e CHK_TEMPO="${b_tempo}/" \
  --entrypoint /bin/sh minio-init -c '
    flush() { curl -s -o /dev/null -m 30 -w "%{http_code}" -X POST "$1" || printf 000; }
    now=$(date +%s)
    tid=$(printf "%016x%016x" "$now" "$$")
    sid=$(printf "%016x" "$$")
    body="{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"obs-check\"}}]},\"scopeSpans\":[{\"scope\":{\"name\":\"obs-check\"},\"spans\":[{\"traceId\":\"$tid\",\"spanId\":\"$sid\",\"name\":\"obs-check\",\"kind\":1,\"startTimeUnixNano\":\"${now}000000000\",\"endTimeUnixNano\":\"${now}500000000\"}]}]}]}"
    code=$(curl -s -o /dev/null -m 15 -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$body" "http://alloy:${ALLOY_OTLP_PORT}/v1/traces" || printf 000)
    echo "probe span $code"
    echo "probe trace $tid"
    sleep 3
    echo "flush mimir $(flush "http://mimir:${MIMIR_PORT}/ingester/flush?wait=true")"
    echo "flush loki $(flush "http://loki:${LOKI_PORT}/flush")"
    echo "flush tempo $(flush "http://tempo:${TEMPO_PORT}/flush")"
    mc alias set root http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 || { echo "alias failed"; exit 1; }
    count() { mc ls --recursive "root/$1" 2>/dev/null | { n=0; while read -r _; do n=$((n + 1)); done; echo "$n"; }; }
    deadline=$(( $(date +%s) + CHECK_TIMEOUT ))
    for pair in "mimir:$CHK_MIMIR" "loki:$CHK_LOKI" "tempo:$CHK_TEMPO"; do
      name=${pair%%:*}; path=${pair#*:}
      while :; do
        n=$(count "$path")
        [ "$n" -gt 0 ] && break
        [ "$(date +%s)" -ge "$deadline" ] && break
        sleep 3
      done
      echo "objects $name $path $n"
    done
  ' 2>&1)"
rc_bucket=0
printf '%s\n' "${bucket_out}"
failed=0
probe_trace=""
while read -r kind name path count; do
  case "${kind}" in
    probe)
      if [ "${name}" = trace ]; then
        probe_trace="${path}"
      elif [ "${name}" = span ]; then
        case "${path}" in
          2*) ;;
          *) echo "FAIL  пробный спан: Alloy ответил HTTP ${path}"; failed=$((failed + 1)) ;;
        esac
      fi
      ;;
    flush)
      # у flush: name=компонент, path=код ответа; не 2xx - предупреждение (итог решает наличие объектов)
      case "${path}" in
        2*) ;;
        *) echo "  WARN  flush ${name}: HTTP ${path}" ;;
      esac
      ;;
    objects)
      if [ "${count:-0}" -gt 0 ] 2>/dev/null; then
        echo "PASS  ${name}: объектов в ${path} - ${count}"
      else
        echo "FAIL  ${name}: в ${path} нет объектов за ${TIMEOUT} с (проверьте, что компонент пишет в MinIO: make logs S=${name})"
        failed=$((failed + 1))
      fi
      ;;
  esac
done <<EOF
$(printf '%s\n' "${bucket_out}" | tr -d '\r')
EOF
[ "${failed}" -eq 0 ] || rc_bucket=1

echo "== Grafana, источники, дашборды, up{job}"
GRAFANA_URL="http://localhost:${gport}" GRAFANA_CREDS="${creds}" DASH_DIR="${DASH_DIR}" PROBE_TRACE="${probe_trace}" \
  RUNNING="${running}" CHECK_TIMEOUT="${TIMEOUT}" python3 -I - <<'PY'
import base64, json, os, re, sys, time, urllib.error, urllib.parse, urllib.request

base = os.environ["GRAFANA_URL"]
auth = "Basic " + base64.b64encode(os.environ["GRAFANA_CREDS"].encode()).decode()
dash_dir = os.environ["DASH_DIR"]
running = set(os.environ["RUNNING"].split())
timeout = int(os.environ["CHECK_TIMEOUT"])
probe_trace = os.environ.get("PROBE_TRACE", "")
bad = 0


def get(path, t=30):
    req = urllib.request.Request(base + path, headers={"Authorization": auth})
    try:
        with urllib.request.urlopen(req, timeout=t) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001 - любая сетевая ошибка - провал проверки
        return 0, str(e)


def check(name, ok, detail=""):
    global bad
    print("%s  %s%s" % ("PASS" if ok else "FAIL", name, (" - " + detail) if detail else ""))
    if not ok:
        bad += 1


def prom(expr):
    return get("/api/datasources/proxy/uid/prometheus/api/v1/query?" + urllib.parse.urlencode({"query": expr}))


# --- Grafana и источники
c, _ = get("/api/health")
check("Grafana отвечает", c == 200, "HTTP %s" % c)

c, b = get("/api/datasources")
sources = {d["uid"]: d for d in json.loads(b)} if c == 200 else {}
for uid in ("prometheus", "loki", "tempo"):
    check("источник %s создан" % uid, uid in sources)
if "prometheus" in sources:
    url = sources["prometheus"].get("url", "")
    check("источник prometheus читает Mimir", "mimir" in url, url)
    c, b = prom("vector(1)")
    check("источник prometheus отвечает на запрос", c == 200 and '"success"' in b, "HTTP %s" % c)
if "loki" in sources:
    c, b = get("/api/datasources/proxy/uid/loki/loki/api/v1/labels")
    check("источник loki отвечает", c == 200, "HTTP %s" % c)
if "tempo" in sources:
    # /api/datasources/uid/tempo/health в Grafana 11.2 отдаёт 404 plugin.notImplemented - проверяем через прокси
    c, b = get("/api/datasources/proxy/uid/tempo/api/echo")
    check("источник tempo отвечает", c == 200, "HTTP %s" % c)

if probe_trace and "tempo" in sources:
    c = 0
    deadline = time.time() + 30
    while True:
        c, b = get("/api/datasources/proxy/uid/tempo/api/traces/" + probe_trace)
        if c == 200 or time.time() >= deadline:
            break
        time.sleep(3)
    check("пробный трейс obs-check виден в Tempo через Grafana", c == 200, "HTTP %s, traceId %s" % (c, probe_trace))

# --- PromQL всех дашбордов
RANGE = {"$__range": "15m", "${__range}": "15m", "$__rate_interval": "1m", "$__interval": "1m"}


def panels(ps):
    for p in ps:
        yield p
        yield from panels(p.get("panels", []))


def is_loki(panel, target):
    for ds in (target.get("datasource"), panel.get("datasource")):
        if isinstance(ds, dict) and ds.get("uid") == "loki":
            return True
        if isinstance(ds, str) and ds.lower() == "loki":
            return True
    return panel.get("type") == "logs"


total = 0
for fname in sorted(os.listdir(dash_dir)):
    if not fname.endswith(".json"):
        continue
    with open(os.path.join(dash_dir, fname), encoding="utf-8") as fh:
        dash = json.load(fh)
    names = [v["name"] for v in dash.get("templating", {}).get("list", [])]
    n = errors = empty = logql = 0
    details = []
    for p in panels(dash.get("panels", [])):
        for t in p.get("targets", []):
            expr = t.get("expr")
            if not expr:
                continue
            if is_loki(p, t):
                logql += 1
                continue
            e = expr
            for k, val in RANGE.items():
                e = e.replace(k, val)
            for nm in names:
                e = re.sub(r"\$\{%s(:[a-z]+)?\}|\$%s\b" % (re.escape(nm), re.escape(nm)), ".*", e)
            n += 1
            c, b = prom(e)
            if c != 200 or '"success"' not in b:
                errors += 1
                details.append("%s: HTTP %s %s" % (p.get("title"), c, b[:120].replace("\n", " ")))
            elif not json.loads(b)["data"]["result"]:
                empty += 1
    total += n
    check("%s: %d PromQL, ошибок %d (пустых %d, LogQL пропущено %d)" % (fname, n, errors, empty, logql),
          errors == 0, " | ".join(details))
check("PromQL во всех дашбордах найдены", total > 0, str(total))

# --- up{job}
JOBS = [
    ("mimir", "mimir"), ("loki", "loki"), ("tempo", "tempo"), ("alloy", "alloy"), ("grafana", "grafana"),
    ("minio", "minio"), ("minio-usage", "minio"), ("kafka", "kafka-exporter"), ("shardingsphere-proxy", "shardingsphere-proxy"),
    ("postgres-shards", "postgres-exporter-shard-0"), ("postgres", "postgres-exporter"), ("keycloak", "keycloak"),
    ("events-collector-service", "events-collector-service"), ("device-collector-service", "device-collector-service"),
    ("device-service", "device-service"), ("failed-events-processor", "failed-events-processor"),
]
deadline = time.time() + timeout
for job, service in JOBS:
    if service not in running:
        print("SKIP  up{job=\"%s\"}: сервис %s не запущен" % (job, service))
        continue
    up = 0
    while True:
        c, b = prom('count(up{job="%s"} == 1) or vector(0)' % job)
        try:
            up = int(float(json.loads(b)["data"]["result"][0]["value"][1])) if c == 200 else 0
        except (KeyError, IndexError, ValueError):
            up = 0
        if up > 0 or time.time() >= deadline:
            break
        time.sleep(5)
    check("up{job=\"%s\"}: экземпляров UP %d" % (job, up), up > 0)

sys.exit(1 if bad else 0)
PY
rc_py=$?

echo
if [ "${rc_bucket}" -ne 0 ] || [ "${rc_py}" -ne 0 ]; then
  echo "obs-check: есть ошибки"
  exit 1
fi
echo "obs-check: всё в порядке"
