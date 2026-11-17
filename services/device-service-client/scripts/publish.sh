#!/usr/bin/env bash
# Публикация клиента device-service в Nexus: services/device-service-client/scripts/publish.sh (make ds-client-release).
#
# Необратимо: выпущенная версия (maven-releases) не перезаписывается. Перед загрузкой проверяются наличие Nexus,
# отсутствие версии в maven-releases и сборка с тестами. Учётная запись публикатора и адрес Nexus - из корневого
# .env через avros/scripts/nexus-env.sh (общая часть скриптов публикации); пароль - только в окружении Gradle.
# Выход 0 - клиент опубликован, иначе - причина в выводе, в Nexus ничего не попало.
set -euo pipefail

CLIENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "${CLIENT_DIR}/../.." && pwd)"
PROJECT=":services:device-service-client"
ARTIFACT_PATH="com/github/gluhov/iotplatform/device-service-client"

# shellcheck source=../../../avros/scripts/nexus-env.sh
source "${ROOT_DIR}/avros/scripts/nexus-env.sh"
cd "${ROOT_DIR}"

echo "==> Nexus ${NEXUS_URL}"
require_nexus

version="$(./gradlew -q --no-daemon "${PROJECT}:properties" --property version | sed -n 's/^version: //p')"
[[ -n "${version}" ]] || fail "не удалось определить version клиента"
if [[ "${version}" != *-SNAPSHOT ]]; then
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
    "${NEXUS_URL}/repository/maven-releases/${ARTIFACT_PATH}/${version}/device-service-client-${version}.pom" || true)"
  case "${code}" in
    200) fail "версия ${version} уже опубликована: выпущенную версию не перезаписать, поднимите version в build.gradle.kts" ;;
    404) ;;
    *) fail "Nexus: не удалось проверить версию ${version} (HTTP ${code})" ;;
  esac
  repository=maven-releases
else
  repository=maven-snapshots
fi

echo "==> device-service-client ${version}: сборка, тесты, публикация"
with_publisher ./gradlew --console=plain --no-daemon "${PROJECT}:build" "${PROJECT}:publish" -PnexusUrl="${NEXUS_URL}"
echo "==> Опубликовано: ${NEXUS_URL}/#browse/browse:${repository}:${ARTIFACT_PATH//\//%2F}"
