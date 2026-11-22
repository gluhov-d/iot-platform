# Образ backup-runner: копии PostgreSQL и ClickHouse в MinIO по расписанию (ADR-0017, research.md R18).
#   - основа apache/kafka (Alpine): kafka-consumer-groups.sh для смещений группы device-collector;
#   - pg_dump, pg_restore, psql 18.6 и их библиотеки - из postgres:18.6-alpine (версия клиента = версии шардов);
#   - mc - из pgsty/mc (клиент MinIO);
#   - cron - busybox crond и flock из самой основы.
# Пользователь - root: crond не запускается без него. Скрипты копий не входят в образ - Compose монтирует
# config/backup-runner в /opt/backup-runner. Теги образов-источников зафиксированы (без latest); Compose передаёт их через build.args из .env.
ARG PG_IMAGE=postgres:18.6-alpine
ARG MC_IMAGE=pgsty/mc:RELEASE.2026-11-12T00-00-00Z

FROM ${PG_IMAGE} AS pg
FROM ${MC_IMAGE} AS mc

FROM apache/kafka:4.2.0
USER root

COPY --from=pg /usr/local/bin/pg_dump /usr/local/bin/pg_restore /usr/local/bin/psql /usr/local/bin/
COPY --from=pg /usr/local/lib/libpq.so.* /usr/local/lib/
# Библиотеки, от которых зависят клиенты PostgreSQL в Alpine (libssl, libcrypto и libncursesw уже есть в основе)
COPY --from=pg /usr/lib/libcom_err.so.* /usr/lib/libedit.so.* /usr/lib/libgssapi_krb5.so.* /usr/lib/libk5crypto.so.* /usr/lib/libkeyutils.so.* \
     /usr/lib/libkrb5.so.* /usr/lib/libkrb5support.so.* /usr/lib/liblber.so.* /usr/lib/libldap.so.* /usr/lib/liblz4.so.* \
     /usr/lib/libsasl2.so.* /usr/lib/libz.so.* /usr/lib/libzstd.so.* /usr/lib/
COPY --from=mc /usr/bin/mc /usr/local/bin/mc

ENTRYPOINT ["/bin/sh", "/opt/backup-runner/entrypoint.sh"]
