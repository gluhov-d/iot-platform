package com.github.gluhov.iotplatform.eventscollector.it;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.github.gluhov.iotplatform.eventscollector.config.KafkaTestContainerConfig.KAFKA_INTERNAL;
import static com.github.gluhov.iotplatform.eventscollector.config.SchemaRegistryTestContainerConfig.schemaRegistryUrl;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.EVENT_TYPE;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// Ошибки приема на отдельной теме с одним разделом и своей consumer group: проверяется committed offset
// после ошибки и следующего корректного сообщения - ни одно сообщение не должно быть молча пропущено
@TestPropertySource(properties = {
        "app.kafka.events-topic=" + ItKafkaErrorHandlingTest.TOPIC,
        "spring.kafka.consumer.group-id=" + ItKafkaErrorHandlingTest.GROUP,
        "spring.kafka.consumer.properties[spring.deserializer.value.delegate.class]="
            + "com.github.gluhov.iotplatform.eventscollector.it.FlakySchemaRegistryDeserializer"
})
class ItKafkaErrorHandlingTest extends AbstractItTest {
    static final String TOPIC = "events-errors-it";
    static final String GROUP = "events-collector-errors-it";
    private static final String DLT = TOPIC + ".DLT";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Duration WAIT = Duration.ofSeconds(30);

    @Autowired
    private JdbcClient jdbcClient;

    // Тема и ее DLT создаются тем же init-topics.sh, что и на стенде, но  с одним разделом
    static {
        try {
            var result = kafka.execInContainer("bash", "-c", "KAFKA_BOOTSTRAP=" + KAFKA_INTERNAL
            + " KAFKA_EVENTS_TOPIC=" + TOPIC + " KAFKA_TOPIC_PARTITIONS=1 /opt/init/init-topics.sh");
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(result.getStdout() + result.getStderr());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create error-handling topics", e);
        }
    }

    @Test
    @DisplayName("Schema Registry times out once then message retried, not skipped, and offset committed after both")
    void givenTransientSchemaRegistryError_whenConsume_thenRetriedAndOffsetCommitted() throws Exception {
        String firstEvent = unique("evt");
        String secondEvent = unique("evt");
        FlakySchemaRegistryDeserializer.FAILURES.set(1);

        RecordMetadata first = sendEvent(firstEvent);
        RecordMetadata second = sendEvent(secondEvent);

        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(second.offset() + 1));
        assertThat(FlakySchemaRegistryDeserializer.FAILURES.get()).as("failure was injected").isZero();
        assertThat(storedEvents(firstEvent)).as("first event retried and stored").isEqualTo(1);
        assertThat(storedEvents(secondEvent)).isEqualTo(1);
        assertThat(deadLetter(first.offset())).as("transient error is not a dead letter").isEmpty();
    }

    @Test
    @DisplayName("Message is not Avro then moved to DLT with original bytes before its offset is committed")
    void givenUndeserializableMessage_whenConsume_thenMovedToDltAndOffsetCommitted() throws Exception {
        byte[] garbage = "not-an-avro-message".getBytes(StandardCharsets.UTF_8);
        String nextEvent = unique("evt");

        RecordMetadata poison = sendRaw(garbage);

        // Offset "ядовитого" сообщения подтвержден сам, после записи в DLT, а не перекрыт следующим сообщением
        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(poison.offset() + 1));
        ConsumerRecord<String, byte[]> dead = deadLetter(poison.offset()).orElseThrow();

        RecordMetadata next = sendEvent(nextEvent);

        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(next.offset() + 1));
        assertThat(storedEvents(nextEvent)).isEqualTo(1);
        assertThat(dead.value()).as("original bytes kept").isEqualTo(garbage);
        assertThat(header(dead, "kafka_dlt-original-topic")).isEqualTo(TOPIC);
    }

    @Test
    @DisplayName("Event fails validation then moved to DLT as Avro before its offset is committed")
    void givenInvalidEvent_whenConsume_thenMovedToDltAndOffsetCommitted() throws Exception {
        String nextEvent = unique("evt");

        RecordMetadata invalid = sendEvent("   ");
        RecordMetadata next = sendEvent(nextEvent);

        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(next.offset() + 1));
        assertThat(storedEvents(nextEvent)).isEqualTo(1);
        ConsumerRecord<String, byte[]> dead = deadLetter(invalid.offset()).orElseThrow();
        try (var avro = new KafkaAvroDeserializer()) {
            avro.configure(Map.of("schema.registry.url", schemaRegistryUrl(), "specific.avro.reader", true), false);
            DeviceEvent parked = (DeviceEvent) avro.deserialize(DLT, dead.value());
            assertThat(parked.getEventId()).isEqualTo("   ");
        }
    }

    private RecordMetadata sendEvent(String eventId) throws Exception {
        DeviceEvent event = DeviceEvent.newBuilder()
                .setEventId(eventId)
                .setDeviceId("dev-errors")
                .setTimestampMs(Instant.now().toEpochMilli())
                .setEventType(EVENT_TYPE)
                .build();
        try (var producer = new KafkaProducer<String, DeviceEvent>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                "schema.registry.url", schemaRegistryUrl()))) {
            return producer.send(new ProducerRecord<>(TOPIC, "dev-errors", event)).get();
        }
    }

    private RecordMetadata sendRaw(byte[] garbage) throws Exception {
        try (var producer = new KafkaProducer<String, byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            return producer.send(new ProducerRecord<>(TOPIC, "dev-errors", garbage)).get();
        }
    }

    // Committed offset consumer group сервиса в единственном разделе темы (-1, пока ничего не подтверждено)
    private long committedOffset() throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            OffsetAndMetadata committed = admin.listConsumerGroupOffsets(GROUP)
                    .partitionsToOffsetAndMetadata().get().get(PARTITION);
            return committed == null ? -1 : committed.offset();
        }
    }

    private long storedEvents(String eventId) {
        return jdbcClient.sql("SELECT count() FROM device_events FINAL WHERE event_id = :id")
                .param("id", eventId).query(Long.class).single();
    }

    // Запись DLT, отложенная из исходного offset темы (заголовок kafka_dlt-original-offset)
    private Optional<ConsumerRecord<String, byte[]>> deadLetter(long offset) {
        try (var consumer = new KafkaConsumer<String, byte[]>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            TopicPartition dlt = new TopicPartition(DLT, 0);
            consumer.assign(List.of(dlt));
            consumer.seekToBeginning(List.of(dlt));
            long end = consumer.endOffsets(List.of(dlt)).get(dlt);
            List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
            while (consumer.position(dlt) < end) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
            return records.stream()
                    .filter(record -> ByteBuffer.wrap(record.headers().lastHeader("kafka_dlt-original-offset").value())
                            .getLong() == offset)
                    .findFirst();

        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}