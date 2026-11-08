package com.github.gluhov.iotplatform.devicecollector.it;

import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.snapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Сценарии из задания (1-6) и совместимость с модулем 2. Порядок фиксирован: сценарий с остановленным шардом
 * идёт перед проверкой метрик, чтобы devices_shard_route_fail_total уже был в /actuator/prometheus.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ItDeviceCollectorTest extends AbstractItTest {
    // Время событий - в прошлом относительно часов сервиса (будущее время отклоняет валидатор)
    private static final Instant T = Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);

    @Test
    @Order(1)
    @DisplayName("1. DeviceInfo from devices is stored through Proxy on the shard of its deviceId")
    void givenDeviceInfo_whenConsumed_thenStoredOnItsShard() throws Exception {
        String id = deviceOnShard(1);

        RecordMetadata sent = send(id, info(id, "evt-1", DeviceUpsertEventType.CREATED, T).build(), null);

        awaitCommitted(sent);
        assertThat(onShard(1, "SELECT seen_count, tenant_id, status, settings->>'mode', jsonb_array_length(sensors), "
                + "last_event_type FROM devices WHERE device_id = '" + id + "'")).isEqualTo("1|tenant-1|ACTIVE|eco|1|CREATED");
        assertThat(onShard(1, "SELECT name, os_name, os_version, firmware_version FROM devices "
                + "WHERE device_id = '" + id + "'")).isEqualTo("Boiler room sensor|Linux|6.1|1.0.0");
        assertThat(onShard(1, "SELECT outcome, kafka_topic, kafka_partition, kafka_offset FROM processed_device_events "
                + "WHERE device_id = '" + id + "'")).isEqualTo("APPLIED|devices|" + sent.partition() + "|" + sent.offset());
        assertThat(assertOnlyOnExpectedShard(id)).isEqualTo(1);
        HttpResponse<String> device = get("/api/v1/devices/" + id);
        assertThat(device.statusCode()).isEqualTo(200);
        assertThat(device.body()).contains("\"shard\":\"shard_1\"", "\"seenCount\":1", "\"sensorId\":\"t1\"",
                "\"name\":\"Boiler room sensor\"", "\"osName\":\"Linux\"", "\"osVersion\":\"6.1\"");
    }

    @Test
    @Order(2)
    @DisplayName("2. The same message delivered again does not increase seen_count")
    void givenSameMessageTwice_whenConsumed_thenSeenCountOne() throws Exception {
        String id = deviceOnShard(0);
        DeviceInfo message = info(id, "evt-1", DeviceUpsertEventType.CREATED, T).build();

        send(id, message, null);
        RecordMetadata repeated = send(id, message, null);

        awaitCommitted(repeated);
        assertThat(assertOnlyOnExpectedShard(id)).isZero();
        assertThat(onShard(0, "SELECT seen_count FROM devices WHERE device_id = '" + id + "'")).isEqualTo("1");
        assertThat(onShard(0, "SELECT count(*) FROM processed_device_events WHERE device_id = '" + id + "'")).isEqualTo("1");
    }

    @Test
    @Order(3)
    @DisplayName("3. Different deviceIds are spread over both shards, each where Proxy routes it")
    void givenManyDevices_whenConsumed_thenSpreadOverShards() throws Exception {
        String prefix = "spread-" + System.nanoTime() + "-";
        List<String> ids = new ArrayList<>();
        RecordMetadata last = null;
        for (int i = 0; i < 12; i++) {
            String id = prefix + i;
            ids.add(id);
            last = send(id, info(id, "evt-1", DeviceUpsertEventType.CREATED, T).build(), 0);
        }

        awaitCommitted(last);
        int onShard0 = Integer.parseInt(onShard(0, "SELECT count(*) FROM devices WHERE device_id LIKE '" + prefix + "%'"));
        int onShard1 = Integer.parseInt(onShard(1, "SELECT count(*) FROM devices WHERE device_id LIKE '" + prefix + "%'"));
        assertThat(onShard0).isPositive();
        assertThat(onShard1).isPositive();
        assertThat(onShard0 + onShard1).isEqualTo(ids.size());
        for (String id : ids) {
            int expected = assertOnlyOnExpectedShard(id);
            assertThat(get("/api/v1/devices/" + id).body()).contains("\"shard\":\"shard_" + expected + "\"");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Older event after a newer one: state kept, seen_count + 1, first_seen_at moves back")
    void givenOlderEventAfterNewer_whenConsumed_thenStale() throws Exception {
        String id = deviceOnShard(1);

        send(id, info(id, "evt-new", DeviceUpsertEventType.UPDATED, T)
                .setDevice(snapshot(id).setFirmwareVersion("2.0.0").build()).build(), null);
        RecordMetadata older = send(id, info(id, "evt-old", DeviceUpsertEventType.UPDATED, T.minusSeconds(60))
                .setDevice(snapshot(id).setFirmwareVersion("1.0.0").build()).build(), null);

        awaitCommitted(older);
        assertThat(assertOnlyOnExpectedShard(id)).isEqualTo(1);
        assertThat(onShard(1, "SELECT seen_count, firmware_version, last_event_id, "
                + "(extract(epoch FROM first_seen_at) * 1000)::bigint "
                + "FROM devices WHERE device_id = '" + id + "'"))
                .isEqualTo("2|2.0.0|evt-new|" + T.minusSeconds(60).toEpochMilli());
        assertThat(onShard(1, "SELECT string_agg(event_key || ':' || outcome, ',' ORDER BY event_key) "
                + "FROM processed_device_events WHERE device_id = '" + id + "'")).isEqualTo("evt-new:APPLIED,evt-old:STALE");
    }

    @Test
    @Order(5)
    @DisplayName("4. Validation error: message moved to devices-dlt, the next message of the partition is processed")
    void givenInvalidMessage_whenConsumed_thenDeadLetteredAndNextProcessed() throws Exception {
        String id = deviceOnShard(0);

        RecordMetadata invalid = send("another-key", info(id, "evt-1", DeviceUpsertEventType.CREATED, T).build(), 0);
        RecordMetadata next = send(id, info(id, "evt-2", DeviceUpsertEventType.CREATED, T).build(), 0);

        awaitCommitted(next);
        assertThat(assertOnlyOnExpectedShard(id)).isZero();
        assertThat(onShard(0, "SELECT last_event_id FROM devices WHERE device_id = '" + id + "'")).isEqualTo("evt-2");
        ConsumerRecord<String, byte[]> dead = deadLetter(invalid).orElseThrow();
        assertThat(dead.partition()).isEqualTo(invalid.partition());
        assertThat(header(dead, "kafka_dlt-original-topic")).isEqualTo(DEVICES);
        assertThat(header(dead, "kafka_dlt-exception-cause-fqcn")).endsWith("InvalidDeviceInfoException");
        try (var avro = new KafkaAvroDeserializer()) {
            avro.configure(Map.of("schema.registry.url", schemaRegistryUrl(), "specific.avro.reader", true), false);
            DeviceInfo parked = (DeviceInfo) avro.deserialize(DLT, dead.value());
            assertThat(parked.getDeviceId()).isEqualTo(id);
        }
    }

    @Test
    @Order(6)
    @DisplayName("Not Avro: moved to devices-dlt with the original bytes")
    void givenUndeserializableMessage_whenConsumed_thenDeadLetteredAsIs() throws Exception {
        byte[] garbage = "not-an-avro-message".getBytes(StandardCharsets.UTF_8);

        RecordMetadata poison = sendRaw("garbage", garbage, 1);

        awaitCommitted(poison);
        ConsumerRecord<String, byte[]> dead = deadLetter(poison).orElseThrow();
        assertThat(dead.value()).isEqualTo(garbage);
    }

    @Test
    @Order(7)
    @DisplayName("DeviceInfo of contracts 1.0.0 (module 2 before the upgrade) is stored as SNAPSHOT")
    void givenDeviceInfoOfVersion100_whenConsumed_thenStored() throws Exception {
        String id = deviceOnShard(0);
        Schema v100 = new Schema.Parser().parse("""
                {"type": "record", "name": "DeviceInfo", "namespace": "com.github.gluhov.iotplatform.events.avro",
                 "fields": [{"name": "deviceId", "type": "string"}, {"name": "emittedAtMs", "type": "long"}]}""");
        GenericRecord old = new GenericData.Record(v100);
        old.put("deviceId", id);
        old.put("emittedAtMs", T.toEpochMilli());

        awaitCommitted(send(id, old, null));

        assertThat(assertOnlyOnExpectedShard(id)).isZero();
        assertThat(onShard(0, "SELECT seen_count, status, last_event_type, last_event_id IS NULL FROM devices "
                + "WHERE device_id = '" + id + "'")).isEqualTo("1|UNKNOWN|SNAPSHOT|t");
        assertThat(onShard(0, "SELECT event_key FROM processed_device_events WHERE device_id = '" + id + "'"))
                .isEqualTo("emitted:" + T.toEpochMilli());
    }

    @Test
    @Order(8)
    @DisplayName("5. One shard down: its message fails and goes to devices-dlt after retries, the other shard keeps working")
    void givenShardDown_whenConsumed_thenOtherShardProcessed() throws Exception {
        String onDownShard = deviceOnShard(1);
        String onLiveShard = deviceOnShard(0);
        String onLiveShardOtherPartition = deviceOnShard(0);
        double receivedBefore = metric("devices_messages_received_total", "");
        double errorsBefore = metric("devices_processing_errors_total", "reason=\"storage\"");
        double failedBefore = metric("devices_messages_failed_total", "reason=\"storage\"");
        stopShard(SHARD_1);
        try {
            // Один раздел: сообщение недоступного шарда не должно навсегда задержать следующее
            RecordMetadata failed = send(onDownShard, info(onDownShard, "evt-1", DeviceUpsertEventType.CREATED, T).build(), 2);
            RecordMetadata behind = send(onLiveShard, info(onLiveShard, "evt-1", DeviceUpsertEventType.CREATED, T).build(), 2);
            RecordMetadata parallel = send(onLiveShardOtherPartition,
                    info(onLiveShardOtherPartition, "evt-1", DeviceUpsertEventType.CREATED, T).build(), 1);

            awaitCommitted(parallel);
            assertThat(onShard(0, "SELECT count(*) FROM devices WHERE device_id = '" + onLiveShardOtherPartition + "'"))
                    .isEqualTo("1");
            awaitCommitted(behind);
            assertThat(onShard(0, "SELECT count(*) FROM devices WHERE device_id = '" + onLiveShard + "'")).isEqualTo("1");
            ConsumerRecord<String, byte[]> dead = deadLetter(failed).orElseThrow();
            assertThat(header(dead, "kafka_dlt-exception-cause-fqcn")).endsWith("DeviceNotStoredException");
            assertThat(header(dead, "kafka_dlt-exception-stacktrace")).contains("(shard shard_1, connection)");
            // Ровно max-attempts попыток, затем devices-dlt; сообщение получено и завершилось ошибкой один раз
            assertThat(metric("devices_processing_errors_total", "reason=\"storage\"") - errorsBefore).isEqualTo(3);
            assertThat(metric("devices_messages_failed_total", "reason=\"storage\"") - failedBefore).isEqualTo(1);
            assertThat(metric("devices_messages_received_total", "") - receivedBefore).isEqualTo(3);
            // Строка не попала на живой шард: Proxy не перенаправляет запись при недоступном шарде
            assertThat(onShard(0, "SELECT count(*) FROM devices WHERE device_id = '" + onDownShard + "'")).isEqualTo("0");
            // Ответ преподавателя 7 (решение): шард недоступен, Proxy отвечает - сервис UP
            HttpResponse<String> health = get("/actuator/health");
            assertThat(health.statusCode()).isEqualTo(200);
            assertThat(health.body()).contains("\"status\":\"UP\"");
            // Чтение устройства недоступного шарда - 503, а не 404 или 500
            assertThat(get("/api/v1/devices/" + onDownShard).statusCode()).isEqualTo(503);
            assertThat(get("/actuator/prometheus").body())
                    .containsPattern("devices_shard_route_fail_total\\{[^}]*reason=\"connection\"[^}]*shard=\"shard_1\"[^}]*\\} [1-9]");
        } finally {
            startShard(SHARD_1);
        }
        // Шард вернулся: новые сообщения его устройств снова записываются
        String afterRestart = deviceOnShard(1);
        await().atMost(WAIT).untilAsserted(() -> assertThat(get("/api/v1/devices/" + afterRestart).statusCode()).isEqualTo(404));
        awaitCommitted(send(afterRestart, info(afterRestart, "evt-1", DeviceUpsertEventType.CREATED, T).build(), null));
        assertThat(assertOnlyOnExpectedShard(afterRestart)).isEqualTo(1);
        assertThat(onShard(1, "SELECT seen_count FROM devices WHERE device_id = '" + afterRestart + "'")).isEqualTo("1");
    }

    @Test
    @Order(9)
    @DisplayName("6. Metrics are available through /actuator/prometheus")
    void givenProcessedMessages_whenScrape_thenRequiredMetricsExported() throws Exception {
        HttpResponse<String> scrape = get("/actuator/prometheus");

        assertThat(scrape.statusCode()).isEqualTo(200);
        assertThat(scrape.body()).contains(
                "devices_messages_received_total",
                "devices_messages_processed_total",
                "devices_messages_duplicate_total",
                "devices_messages_failed_total",
                "devices_dlt_published_total",
                "devices_db_write_seconds_bucket",
                "devices_processing_seconds_bucket",
                "devices_shard_route_fail_total");
        assertThat(scrape.body()).containsPattern("devices_messages_duplicate_total\\{[^}]*\\} [1-9]");
        assertThat(scrape.body()).containsPattern("devices_dlt_published_total\\{[^}]*reason=\"invalid\"[^}]*\\} [1-9]");
        assertThat(get("/actuator/health").body()).contains("\"status\":\"UP\"");
        // Все отправленные сообщения обработаны: каждое получено один раз и имеет ровно один исход
        double received = metric("devices_messages_received_total", "");
        assertThat(received).isPositive();
        assertThat(metric("devices_messages_processed_total", "") + metric("devices_messages_duplicate_total", "")
                + metric("devices_messages_failed_total", "")).isEqualTo(received);
    }

    @Test
    @Order(10)
    @DisplayName("7. Proxy down: health DOWN (503) - the service has no way to the registry")
    void givenProxyDown_whenHealth_thenDown() {
        // Последний тест: Proxy не запускается снова (после рестарта у контейнера был бы другой порт на хосте)
        stopShard(PROXY);

        await().atMost(WAIT).untilAsserted(() -> {
            HttpResponse<String> health = get("/actuator/health");
            assertThat(health.statusCode()).isEqualTo(503);
            assertThat(health.body()).contains("\"status\":\"DOWN\"");
        });
    }

    /** Сумма значений метрики в /actuator/prometheus по строкам, содержащим метку labels (пусто - все строки). */
    private double metric(String name, String labels) throws Exception {
        String body = get("/actuator/prometheus").body();
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("(?m)^" + java.util.regex.Pattern.quote(name) + "(\\{[^}]*\\})? (\\S+)$").matcher(body);
        double sum = 0;
        while (m.find()) {
            if (labels.isEmpty() || (m.group(1) != null && m.group(1).contains(labels))) {
                sum += Double.parseDouble(m.group(2));
            }
        }
        return sum;
    }
}