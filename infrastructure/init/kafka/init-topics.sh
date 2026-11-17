#!/bin/bash
# Запускается одноразовым контейнером kafka-init после того, как kafka стала healthy.
# Идемпотентен: существующие темы не пересоздаются (--if-not-exists), поэтому повторный запуск безопасен.
#
# --if-not-exists не меняет настройки уже существующей темы. Если тема была создана с другой
# cleanup.policy, исправьте ее через kafka-configs.sh --alter или пересоздайте том Kafka (make kafka-reset).

set -euo pipefail

: "${KAFKA_BOOTSTRAP:?KAFKA_BOOTSTRAP не задан}"
EVENTS_TOPIC="${KAFKA_EVENTS_TOPIC:-events}"
DEVICES_TOPIC="${KAFKA_DEVICES_TOPIC:-devices}"
EVENTS_DLT_TOPIC="${KAFKA_EVENTS_DLT_TOPIC:-events-dlt}"
DEVICES_DLT_TOPIC="${KAFKA_DEVICES_DLT_TOPIC:-devices-dlt}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION="${KAFKA_TOPIC_REPLICATION:-1}"
# Срок хранения сообщений темы devices (FR-022, ADR-0011): тема не сжимается, сообщения ключа лежат целиком
# и в исходном порядке, пока не выйдут за срок; значение задаёт .env.example
: "${KAFKA_DEVICES_RETENTION_MS:?KAFKA_DEVICES_RETENTION_MS не задан}"
DEVICES_RETENTION_MS="${KAFKA_DEVICES_RETENTION_MS}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

topics() { "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${KAFKA_BOOTSTRAP}" "$@"; }
configs() { "${KAFKA_BIN}/kafka-configs.sh" --bootstrap-server "${KAFKA_BOOTSTRAP}" "$@"; }

echo "==> ожидание брокера ${KAFKA_BOOTSTRAP}"
for _ in $(seq 1 30); do
  topics --list >/dev/null 2>&1 && break
  sleep 2
done
topics --list >/dev/null

create() {
  local name="$1" policy="$2" retention="${3:-}"
  echo "==> тема ${name}: partitions=${PARTITIONS}, rf=${REPLICATION}, cleanup.policy=${policy}"
  if [[ -n "${retention}" ]]; then
    topics --create --if-not-exists --topic "${name}" \
      --partitions "${PARTITIONS}" --replication-factor "${REPLICATION}" \
      --config "cleanup.policy=${policy}" --config "retention.ms=${retention}"
  else
    topics --create --if-not-exists --topic "${name}" \
      --partitions "${PARTITIONS}" --replication-factor "${REPLICATION}" \
      --config "cleanup.policy=${policy}"
  fi
}

# Сырой поток событий устройств: старые сообщения удаляются по retention
create "${EVENTS_TOPIC}" delete
# Поток устройств: команды администратора и события роутера с ключом deviceId. Сжатие выключено (ADR-0011) - оно
# оставляло на ключ последнее сообщение и стирало порядок: команда DELETE исчезала, а снимок роутера оставался
create "${DEVICES_TOPIC}" delete "${DEVICES_RETENTION_MS}"
# Уже существующая тема: --if-not-exists не меняет её настройки, поэтому политика и срок задаются явно
# (повторно - без изменений). Настройка сжатия снимается: с cleanup.policy=delete она не действует и только путает
echo "==> тема ${DEVICES_TOPIC}: cleanup.policy=delete, retention.ms=${DEVICES_RETENTION_MS}"
configs --entity-type topics --entity-name "${DEVICES_TOPIC}" --alter \
  --add-config "cleanup.policy=delete,retention.ms=${DEVICES_RETENTION_MS}"
configs --entity-type topics --entity-name "${DEVICES_TOPIC}" --alter \
  --delete-config "min.compaction.lag.ms" 2>/dev/null \
  || echo "    (min.compaction.lag.ms у темы не задан - снимать нечего)"
# Входные сообщения, которые нельзя обработать (не Avro, не прошли валидацию): сервис откладывает их сюда,
# а не теряет; число разделов как у events - сообщение уходит в раздел с тем же номером
create "${EVENTS_DLT_TOPIC}" delete
# Реестр устройств: сообщения devices, которые device-collector-service не смог обработать (невалидные или не записанные
# в PostgreSQL после повторов); число разделов как у devices - сообщение уходит в раздел с тем же номером
create "${DEVICES_DLT_TOPIC}" delete

echo "==> итоговая конфигурация:"
topics --describe --topic "${EVENTS_TOPIC}"
topics --describe --topic "${DEVICES_TOPIC}"
configs --entity-type topics --entity-name "${DEVICES_TOPIC}" --describe
topics --describe --topic "${DEVICES_DLT_TOPIC}"
topics --describe --topic "${EVENTS_DLT_TOPIC}"