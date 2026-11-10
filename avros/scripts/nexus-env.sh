# shellcheck shell=bash
# Общая часть скриптов avros (подключается через source, сама не запускается):
# адрес Nexus, учётная запись публикатора, адрес Schema Registry и subject'ы схем из корневого .env.
#
# Значения берутся в том виде, как их разбирает Compose (кавычки, экранирование) - те же,
# что получил nexus-init. Пароль не выводится и не попадает в командную строку.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
COMPOSE=(docker compose --env-file "${ENV_FILE}" -f "${ROOT_DIR}/infrastructure/docker-compose.yaml")
ARTIFACT_PATH="com/github/gluhov/iotplatform/avros"

fail() { printf '[FAIL] %s\n' "$1" >&2; exit 1; }

[[ -f "${ENV_FILE}" ]] || fail "${ENV_FILE} not found: cp .env.example .env"
env_dump="$("${COMPOSE[@]}" config --environment)" || fail "docker compose config: не удалось прочитать ${ENV_FILE}"
env_value() { sed -n "s/^$1=//p" <<<"${env_dump}"; }
for name in NEXUS_PORT NEXUS_PUBLISH_USER NEXUS_PUBLISH_PASSWORD SCHEMA_REGISTRY_PORT \
            KAFKA_EVENTS_TOPIC KAFKA_EVENTS_DLT_TOPIC KAFKA_DEVICES_TOPIC KAFKA_DEVICES_DLT_TOPIC; do
  [[ -n "$(env_value "${name}")" ]] || fail "${name} не задан в ${ENV_FILE}"
done
NEXUS_URL="http://localhost:$(env_value NEXUS_PORT)"
SCHEMA_REGISTRY_URL="http://localhost:$(env_value SCHEMA_REGISTRY_PORT)"

# shellcheck source=schema-subjects.sh
source "$(dirname "${BASH_SOURCE[0]}")/schema-subjects.sh"
SCHEMA_SUBJECTS="$(schema_subjects "$(env_value KAFKA_EVENTS_TOPIC)" "$(env_value KAFKA_EVENTS_DLT_TOPIC)" \
  "$(env_value KAFKA_DEVICES_TOPIC)" "$(env_value KAFKA_DEVICES_DLT_TOPIC)")"
# Параметры сверки со Schema Registry для Gradle (проверка перед публикацией обязательна)
REGISTRY_PROPS=("-PschemaRegistryUrl=${SCHEMA_REGISTRY_URL}" "-PschemaSubjects=${SCHEMA_SUBJECTS}")

# Nexus поднят и принимает запись
require_nexus() {
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${NEXUS_URL}/service/rest/v1/status/writable" || true)"
  [[ "${code}" == 200 ]] || fail "Nexus ${NEXUS_URL} не готов к записи (HTTP ${code}): make nexus-up"
}

# Schema Registry отвечает: без неё публикация в Nexus не выполняется
require_schema_registry() {
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${SCHEMA_REGISTRY_URL}/subjects" || true)"
  [[ "${code}" == 200 ]] || fail "Schema Registry ${SCHEMA_REGISTRY_URL} недоступна (HTTP ${code}): make registry-up"
}

# HTTP-код pom выпущенной версии: 200 - есть в maven-releases, 404 - нет
release_status() {
  curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
    "${NEXUS_URL}/repository/maven-releases/${ARTIFACT_PATH}/$1/avros-$1.pom" || true
}

# Версия avros из avros/build.gradle.kts в каталоге $1
avros_version() {
  (cd "$1" && ./gradlew -q :avros:properties --property version | sed -n 's/^version: //p')
}

# Команда с учётной записью публикатора: пароль - только в окружении процесса Gradle
with_publisher() {
  ORG_GRADLE_PROJECT_nexusUsername="$(env_value NEXUS_PUBLISH_USER)" \
  ORG_GRADLE_PROJECT_nexusPassword="$(env_value NEXUS_PUBLISH_PASSWORD)" \
    "$@"
}
