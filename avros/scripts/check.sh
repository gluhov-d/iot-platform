#!/usr/bin/env bash
# Проверка схем avros без публикации (обычно через make avros-check): разбор схем, совместимость с последней
# выпущенной версией в Nexus и со всеми схемами, зарегистрированными в Schema Registry. Те же проверки
# обязательно выполняет публикация перед загрузкой в Nexus. Выход 0 - схемы можно публиковать.
set -euo pipefail

# shellcheck source=nexus-env.sh
source "$(dirname "${BASH_SOURCE[0]}")/nexus-env.sh"
cd "${ROOT_DIR}"

echo "==> Nexus ${NEXUS_URL}, Schema Registry ${SCHEMA_REGISTRY_URL}"
require_schema_registry
./gradlew --console=plain :avros:checkAvrosCompatibility :avros:checkRegisteredSchemas \
  -PnexusUrl="${NEXUS_URL}" "${REGISTRY_PROPS[@]}"