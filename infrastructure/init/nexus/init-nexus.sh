#!/bin/sh
# Запускается одноразовым контейнером nexus-init после того, как nexus стал healthy.
# Идемпотентен: приводит пароль admin, EULA, анонимное чтение, роль и пользователя публикации
# к значениям из .env, поэтому повторный запуск безопасен.
#
# В образе sonatype/nexus3 (Alpine) нет bash и jq - только sh, curl и sed.

set -eu

: "${NEXUS_URL:?NEXUS_URL не задан}"
: "${NEXUS_ADMIN_PASSWORD:?NEXUS_ADMIN_PASSWORD не задан}"
: "${NEXUS_PUBLISH_USER:?NEXUS_PUBLISH_USER не задан}"
: "${NEXUS_PUBLISH_PASSWORD:?NEXUS_PUBLISH_PASSWORD не задан}"
API="${NEXUS_URL}/service/rest/v1"
ROLE_ID="contracts-publisher"
# Пароль admin, который Nexus генерирует при первом старте; после смены пароля файл удаляется
GENERATED_PASSWORD_FILE="/nexus-data/admin.password"
ADMIN_PASSWORD="${NEXUS_ADMIN_PASSWORD}"

# Экранирование строки для JSON: обратная косая черта и кавычка
json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }

# Запрос от имени admin: печатает тело ответа, при коде >= 400 печатает тело ошибки и завершается
api() {
  method="$1" path="$2"
  shift 2
  curl -sS --fail-with-body -u "admin:${ADMIN_PASSWORD}" -X "${method}" "$@" "${API}${path}"
}

# Только код ответа - для проверок "существует ли" и "подходит ли пароль"
status() { curl -s -o /dev/null -w '%{http_code}' -u "admin:${ADMIN_PASSWORD}" "${API}$1"; }

echo "==> пароль admin"
if [ "$(status /status/check)" != 200 ]; then
  if [ ! -r "${GENERATED_PASSWORD_FILE}" ]; then
    echo "!!! NEXUS_ADMIN_PASSWORD из .env не подходит, а сгенерированного пароля уже нет."
    echo "    Верните в .env прежний пароль или пересоздайте том nexus-data (данные Nexus будут потеряны)."
    exit 1
  fi
  ADMIN_PASSWORD="$(cat "${GENERATED_PASSWORD_FILE}")"
  printf '%s' "${NEXUS_ADMIN_PASSWORD}" |
    api PUT /security/users/admin/change-password -H 'Content-Type: text/plain' --data-binary @-
  ADMIN_PASSWORD="${NEXUS_ADMIN_PASSWORD}"
  echo "    пароль admin заменен на значение из .env"
fi

echo "==> лицензионное соглашение (EULA)"
# Community Edition не публикует и не отдает артефакты, пока EULA не принято
eula="$(api GET /system/eula)"
if printf '%s' "${eula}" | grep -q '"accepted" *: *false'; then
  if [ "${NEXUS_ACCEPT_EULA:-false}" != true ]; then
    echo "!!! Nexus Community Edition работает только после принятия EULA:"
    echo "    https://links.sonatype.com/products/nxrm/ce-eula"
    echo "    Если вы согласны с условиями, поставьте NEXUS_ACCEPT_EULA=true в .env и повторите make up-ci."
    exit 1
  fi
  # API принимает тот же документ, что вернул, с accepted=true (текст disclaimer должен совпадать)
  printf '%s' "${eula}" | sed 's/"accepted" *: *false/"accepted" : true/' |
    api POST /system/eula -H 'Content-Type: application/json' --data-binary @-
  echo "    EULA принято (NEXUS_ACCEPT_EULA=true)"
fi

echo "==> анонимное чтение"
# Контракты не секретны: сервисы скачивают их без учетной записи, публикация - только с паролем
api PUT /security/anonymous -H 'Content-Type: application/json' \
  -d '{"enabled": true, "userId": "anonymous", "realmName": "NexusAuthorizingRealm"}' >/dev/null

echo "==> роль ${ROLE_ID}"
# Только запись в maven-releases и maven-snapshots; edit нужен для обновления maven-metadata.xml
role="{\"id\": \"${ROLE_ID}\", \"name\": \"${ROLE_ID}\",
  \"description\": \"Publish contracts to maven-releases and maven-snapshots\",
  \"privileges\": [
    \"nx-repository-view-maven2-maven-releases-browse\", \"nx-repository-view-maven2-maven-releases-read\",
    \"nx-repository-view-maven2-maven-releases-add\", \"nx-repository-view-maven2-maven-releases-edit\",
    \"nx-repository-view-maven2-maven-snapshots-browse\", \"nx-repository-view-maven2-maven-snapshots-read\",
    \"nx-repository-view-maven2-maven-snapshots-add\", \"nx-repository-view-maven2-maven-snapshots-edit\"],
  \"roles\": []}"
if [ "$(status "/security/roles/${ROLE_ID}")" = 200 ]; then
  printf '%s' "${role}" | api PUT "/security/roles/${ROLE_ID}" -H 'Content-Type: application/json' --data-binary @-
else
  printf '%s' "${role}" | api POST /security/roles -H 'Content-Type: application/json' --data-binary @- >/dev/null
fi

echo "==> пользователь ${NEXUS_PUBLISH_USER}"
user="$(json_escape "${NEXUS_PUBLISH_USER}")"
# Фильтр userId ищет по префиксу, поэтому проверяем точное совпадение в ответе
if api GET "/security/users?userId=${NEXUS_PUBLISH_USER}" | grep -q "\"userId\" *: *\"${user}\""; then
  printf '%s' "${NEXUS_PUBLISH_PASSWORD}" |
    api PUT "/security/users/${NEXUS_PUBLISH_USER}/change-password" -H 'Content-Type: text/plain' --data-binary @-
  echo "    пароль синхронизирован с .env"
else
  printf '{"userId": "%s", "firstName": "Contracts", "lastName": "Publisher",
    "emailAddress": "%s@iot-platform.local", "password": "%s", "status": "active", "roles": ["%s"]}' \
    "${user}" "${user}" "$(json_escape "${NEXUS_PUBLISH_PASSWORD}")" "${ROLE_ID}" |
    api POST /security/users -H 'Content-Type: application/json' --data-binary @- >/dev/null
  echo "    создан с ролью ${ROLE_ID}"
fi

echo "==> репозитории maven2:"
api GET /repositories | grep -o '"name" *: *"maven-[a-z]*"' | sed 's/.*: *"\(.*\)"/    \1/'