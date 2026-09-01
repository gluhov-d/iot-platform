set -euo pipefail

if [ -z "${POSTGRES_EXTRA_DBS:-}" ]; then
  echo "POSTGRES_EXTRA_DBS не задан — пропускаю"
  exit 0
fi

for db in $(echo "${POSTGRES_EXTRA_DBS}" | tr ',' ' '); do
  echo "==> creating database '${db}'"
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-SQL
    SELECT 'CREATE DATABASE ${db}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${db}')\gexec
    GRANT ALL PRIVILEGES ON DATABASE ${db} TO ${POSTGRES_USER};
SQL
done
