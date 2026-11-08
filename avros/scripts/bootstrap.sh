#!/usr/bin/env bash
# Публикует в Nexus выпущенные версии avros, которых в нём нет. Источник - git-теги avros/vX.Y.Z.
#
# avros/scripts/bootstrap.sh (обычно через make avros-bootstrap; make up вызывает его сам)
#
# История релизов хранится в git, Nexus - только хранилище артефактов: на чистом клоне или после
# make clean версии, от которых зависят сервисы, собираются заново из своих тегов. Каждая версия
# собирается во временном git worktree на её теге, по возрастанию - так проверка совместимости
# сравнивает её с предыдущей. Уже опубликованные версии пропускаются, повторный запуск безопасен.
set -euo pipefail

# shellcheck source=nexus-env.sh
source "$(dirname "${BASH_SOURCE[0]}")/nexus-env.sh"
cd "${ROOT_DIR}"

tags="$(git tag -l 'avros/v*' --sort=version:refname)"
if [[ -z "${tags}" ]]; then
  echo "==> avros: тегов avros/v* нет, публиковать нечего"
  exit 0
fi
require_nexus

worktree=""
cleanup() { [[ -z "${worktree}" ]] || git worktree remove --force "${worktree}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

for tag in ${tags}; do
  version="${tag#avros/v}"
  case "$(release_status "${version}")" in
    200) echo "==> avros ${version}: уже в Nexus"; continue ;;
    404) ;;
    *) fail "Nexus ${NEXUS_URL}: не удалось проверить версию ${version}" ;;
  esac

  echo "==> avros ${version}: публикация из тега ${tag}"
  worktree="$(mktemp -d "${TMPDIR:-/tmp}/avros-${version}.XXXXXX")"
  git worktree add -q --detach "${worktree}" "${tag}"
  actual="$(avros_version "${worktree}")"
  [[ "${actual}" == "${version}" ]] || fail "в теге ${tag} version = '${actual}', ожидалась ${version}"
  (cd "${worktree}" && with_publisher ./gradlew -q --console=plain :avros:publish -PnexusUrl="${NEXUS_URL}")
  cleanup
  worktree=""
done
