#!/usr/bin/env bash
# Конвейер avros (Avro-схемы платформы): разбор схем и проверка совместимости -> сборка -> публикация в Nexus
#
# avros/scripts/publish.sh (обычно через make avros-release)
#
# Релиз (версия без -SNAPSHOT) = коммит + тег + артефакт: собирается только из закоммиченного
# avros/, после публикации на HEAD ставится тег avros/vX.Y.Z. По тегам make avros-bootstrap
# восстанавливает версии в пустом Nexus. SNAPSHOT публикуется из рабочей папки и без тега.
# Выход 0 - версия опубликована, иначе - причина в выводе, в Nexus ничего не попало.
set -euo pipefail

# shellcheck source=nexus-env.sh
source "$(dirname "${BASH_SOURCE[0]}")/nexus-env.sh"
cd "${ROOT_DIR}"

echo "==> Nexus ${NEXUS_URL}"
require_nexus

version="$(avros_version "${ROOT_DIR}")"
tag="avros/v${version}"
release=true
[[ "${version}" == *-SNAPSHOT ]] && release=false

if ${release}; then
  [[ -z "$(git status --porcelain -- avros)" ]] ||
    fail "в avros/ есть незакоммиченные изменения: релиз ${version} собирается только из коммита"
  if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
    fail "тег ${tag} уже есть - версия выпущена (в пустой Nexus её вернёт make avros-bootstrap); поднимите version"
  fi
fi

echo "==> avros ${version}: проверка совместимости, сборка, публикация"
with_publisher ./gradlew --console=plain :avros:build :avros:publish -PnexusUrl="${NEXUS_URL}"

if ${release}; then
  git tag -a "${tag}" -m "avros ${version}"
  echo "==> Тег ${tag} поставлен на $(git rev-parse --short HEAD); отправьте его вместе с веткой: git push origin ${tag}"
  repository=maven-releases
else
  repository=maven-snapshots
fi
echo "==> Опубликовано: ${NEXUS_URL}/#browse/browse:${repository}:${ARTIFACT_PATH//\//%2F}"
