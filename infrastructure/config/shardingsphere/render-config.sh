#!/bin/bash
# Точка входа контейнера ShardingSphere-Proxy: подставляет значения из окружения в шаблоны конфигурации
# и запускает Proxy. Proxy 5.5.3 не раскрывает переменные окружения в global.yaml / database-*.yaml,
# поэтому в репозитории лежат шаблоны (infrastructure/config/shardingsphere), а файлы с паролями
# появляются только внутри контейнера.
#
# Подставляются только ${ИМЯ_ЗАГЛАВНЫМИ}: выражение шардирования shard_${(device_id...)} не затрагивается.
# В шаблонах значения стоят в одинарных кавычках YAML, поэтому кавычка в значении удваивается; остальные
# символы (#, ", \, &, пробел) попадают как есть. Незаданная переменная - ошибка запуска, а не пустой пароль.
set -euo pipefail

TEMPLATES_DIR="${PROXY_CONF_TEMPLATES:-/opt/init/conf}"
CONF_DIR="${LOCAL_PATH:-/opt/shardingsphere-proxy}/conf"

for template in "${TEMPLATES_DIR}"/*.yaml; do
  target="${CONF_DIR}/$(basename "${template}")"
  perl -pe 's/\$\{([A-Z][A-Z0-9_]*)\}/defined $ENV{$1} ? do { (my $v = $ENV{$1}) =~ s{\x27}{\x27\x27}g; $v } : die "variable $1 is not set\n"/ge' \
    "${template}" > "${target}"
  echo "==> ${target}: rendered from ${template}"
done

exec "${LOCAL_PATH:-/opt/shardingsphere-proxy}/bin/start.sh" "${PORT:-3307}"