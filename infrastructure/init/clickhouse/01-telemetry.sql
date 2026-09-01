CREATE DATABASE IF NOT EXISTS iot_telemetry;

CREATE TABLE IF NOT EXISTS iot_telemetry.events
(
    event_id    UUID,
    device_id   String,
    event_type  LowCardinality(String),
    metric      LowCardinality(String),
    value       Float64,
    payload     String,
    event_time  DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (device_id, event_time)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS iot_telemetry.dead_letters
(
    id           UUID,
    topic        String,
    partition_id UInt32,
    offset_id    UInt64,
    error        String,
    raw_payload  String,
    failed_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (failed_at, topic);
