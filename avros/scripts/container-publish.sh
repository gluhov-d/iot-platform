#!/usr/bin/env bash
# Публикация avros в Nexus из контейнера avros-publish (одноразовая задача compose).
#
# Идемпотентна: версия уже в Nexus и схемы совпадают - ничего не делает, код 0;
# версия в Nexus есть, а схемы другие - ошибка (выпущенную версию не перезаписать, поднимите version);
# версии нет - проверка совместимости с последней выпущенной, сборка, публикация.
# Адрес и учётная запись Nexus - из окружения контейнера.
set -euo pipefail

: "${NEXUS_URL:?NEXUS_URL не задан}"
: "${NEXUS_PUBLISH_USER:?NEXUS_PUBLISH_USER не задан}"
: "${NEXUS_PUBLISH_PASSWORD:?NEXUS_PUBLISH_PASSWORD не задан}"
ARTIFACT_PATH="com/github/gluhov/iotplatform/avros"

cd /workspace
fail() { printf '[FAIL] %s\n' "$1" >&2; exit 1; }

version="$(./gradlew -q --offline --no-daemon :avros:properties --property version | sed -n 's/^version: //p')"
[[ -n "${version}" ]] || fail "не удалось определить version из avros/build.gradle.kts"
echo "==> avros ${version}, Nexus ${NEXUS_URL}"

if [[ "${version}" != *-SNAPSHOT ]]; then
  base="${NEXUS_URL}/repository/maven-releases/${ARTIFACT_PATH}/${version}"
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "${base}/avros-${version}.pom" || true)"
  case "${code}" in
    200)
      tmp="$(mktemp -d)"
      curl -fsS --max-time 30 -o "${tmp}/avros.jar" "${base}/avros-${version}.jar" ||
        fail "avros-${version}.jar: не скачать из Nexus"
      (cd "${tmp}" && jar xf avros.jar avro)
      if diff -r "${tmp}/avro" avros/avro >/dev/null; then
        echo "==> avros ${version} уже в Nexus, схемы совпадают: публиковать нечего"
        exit 0
      fi
      diff -r "${tmp}/avro" avros/avro || true
      fail "avros ${version} уже выпущена с другими схемами: выпущенную версию не перезаписать, поднимите version в avros/build.gradle.kts"
      ;;
    404) ;;
    *) fail "Nexus ${NEXUS_URL}: не удалось проверить версию ${version} (HTTP ${code})" ;;
  esac
fi

# Пароль - только в окружении процесса Gradle, не в командной строке
ORG_GRADLE_PROJECT_nexusUsername="${NEXUS_PUBLISH_USER}" \
ORG_GRADLE_PROJECT_nexusPassword="${NEXUS_PUBLISH_PASSWORD}" \
  ./gradlew --no-daemon --console=plain :avros:build :avros:publish -x test -PnexusUrl="${NEXUS_URL}"
echo "==> avros ${version} опубликована"
