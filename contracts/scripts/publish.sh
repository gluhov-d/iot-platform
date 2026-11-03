#!/usr/bin/env bash
# Конвейер Avro-контрактов: разбор схем и проверка совместимости -> сборка -> публикация в Nexus
#
# contracts/scripts/publish.sh (обычно через make contracts-publish)
#
# Нужен поднятый Nexus (make up-ci). Адрес и учетная запись публикатора берутся из корневого .env;
# пароль передается Gradle через окружение, в командной строке и выводе его нет.
# Выход 0 - версия опубликована, иначе - причина в выводе, в Nexus ничего не попало.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
COMPOSE=(docker compose --env-file "${ENV_FILE}" -f "${ROOT_DIR}/infrastructure/docker-compose.yaml")

fail() { printf '[FAIL] %s\n' "$1" >&2; exit 1; }

[[ -f "${ENV_FILE}" ]] || fail "${ENV_FILE} not found: cp .env.example .env"

# Значения в том виде, как их разбирает Compose (кавычки, экранирование) - те же, что получил nexus-init
env_dump="$("${COMPOSE[@]}" config --environment)" || fail "docker compose config: не удалось прочитать ${ENV_FILE}"
env_value() { sed -n "s/^$1=//p" <<<"${env_dump}"; }
for name in NEXUS_PORT NEXUS_PUBLISH_USER NEXUS_PUBLISH_PASSWORD; do
  [[ -n "$(env_value "${name}")" ]] || fail "${name} не задан в ${ENV_FILE}"
done
NEXUS_URL="http://localhost:$(env_value NEXUS_PORT)"

echo "==> Nexus ${NEXUS_URL}"
code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${NEXUS_URL}/service/rest/v1/status/writable" || true)"
[[ "${code}" == 200 ]] || fail "Nexus не готов к записи (HTTP ${code}): make up-ci"

cd "${ROOT_DIR}"
version="$(./gradlew -q :contracts:properties --property version | sed -n 's/^version: //p')"
echo "==> contracts-avro ${version}: проверка совместимости, сборка, публикация"

ORG_GRADLE_PROJECT_nexusUsername="$(env_value NEXUS_PUBLISH_USER)" \
ORG_GRADLE_PROJECT_nexusPassword="$(env_value NEXUS_PUBLISH_PASSWORD)" \
  ./gradlew --console=plain :contracts:build :contracts:publish -PnexusUrl="${NEXUS_URL}"

repository=maven-releases
[[ "${version}" == *-SNAPSHOT ]] && repository=maven-snapshots
echo "==> Опубликовано: ${NEXUS_URL}/#browse/browse:${repository}:com%2Fgithub%2Fgluhov%2Fiotplatform%2Fcontracts-avro"
