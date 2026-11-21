package com.github.gluhov.iotplatform.eventscollector.it;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.DeviceEventRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.github.gluhov.iotplatform.eventscollector.config.SchemaRegistryTestContainerConfig.schemaRegistryUrl;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.EVENT_TYPE;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

// Ошибки приема на отдельной теме с одним разделом и своей consumer group: проверяется committed offset
// после ошибки и следующего корректного сообщения - ни одно сообщение не должно быть молча пропущено.
// Записи failed-events проверяются без MinIO и без процессора (он поднимается во второй волне).
@TestPropertySource(properties = {
        "app.kafka.events-topic=" + ItKafkaErrorHandlingTest.TOPIC,
        "app.processing.max-attempts=" + ItKafkaErrorHandlingTest.MAX_ATTEMPTS,
        "app.processing.retry-interval=100ms",
        "spring.kafka.consumer.group-id=" + ItKafkaErrorHandlingTest.GROUP,
        "spring.kafka.consumer.properties[raw.value.delegate.class]="
            + "com.github.gluhov.iotplatform.eventscollector.it.FlakySchemaRegistryDeserializer"
})
class ItKafkaErrorHandlingTest extends AbstractItTest {
    static final String TOPIC = "events-errors-it";
    static final String FAILED_TOPIC = "failed-events";
    static final String GROUP = "events-collector-errors-it";
    static final int MAX_ATTEMPTS = 3;
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Duration WAIT = Duration.ofSeconds(30);

    @Autowired
    private JdbcClient jdbcClient;

    // Запись в ClickHouse ломается для одного eventId; остальные сообщения идут в настоящий репозиторий
    @MockitoSpyBean
    private DeviceEventRepository eventRepository;

    // Тема ошибок - с одним разделом и своим именем; failed-events создаётся, если init-topics.sh её не создал
    static {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            for (NewTopic topic : List.of(new NewTopic(TOPIC, 1, (short) 1), new NewTopic(FAILED_TOPIC, 1, (short) 1))) {
                try {
                    admin.createTopics(List.of(topic)).all().get();
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw e;
                    }
                }
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
        assertThat(failedEvent(first.offset())).as("transient error is not a failed event").isEmpty();
    }

    @Test
    @DisplayName("Valid Avro of another type (DeviceInfo) then failed-events with original bytes, offset committed, next event stored")
    void givenAnotherAvroType_whenConsume_thenFailedEventAsIsAndOffsetCommitted() throws Exception {
        String nextEvent = unique("evt");
        byte[] foreign;
        try (var serializer = new KafkaAvroSerializer()) {
            serializer.configure(Map.of("schema.registry.url", schemaRegistryUrl()), false);
            foreign = serializer.serialize("devices", DeviceInfo.newBuilder()
                    .setDeviceId("dev-errors").setEmittedAtMs(Instant.now().toEpochMilli()).build());
        }

        RecordMetadata alien = sendRaw(foreign);
        RecordMetadata next = sendEvent(nextEvent);

        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(next.offset() + 1));
        assertThat(storedEvents(nextEvent)).isEqualTo(1);
        FailedEvent failed = failedEvent(alien.offset()).orElseThrow();
        assertThat(failed.getErrorCode()).isEqualTo(ErrorCode.DESERIALIZATION_FAILED);
        assertThat(failed.getErrorType()).isEqualTo(ErrorType.PERMANENT);
        assertThat(bytes(failed.getValue())).as("original bytes, not a re-serialized record").isEqualTo(foreign);
        assertThat(failed.getHeaders().keySet()).as("no Spring or raw-byte helper headers")
                .noneMatch(name -> name.startsWith("spring") || name.startsWith("kafka_") || name.startsWith("x-raw"));
    }

    @Test
    @DisplayName("Message is not Avro then failed-events with original key and value before its offset is committed")
    void givenUndeserializableMessage_whenConsume_thenFailedEventAndOffsetCommitted() throws Exception {
        byte[] garbage = "not-an-avro-message".getBytes(StandardCharsets.UTF_8);
        String nextEvent = unique("evt");

        RecordMetadata poison = sendRaw(garbage);

        // Offset "ядовитого" сообщения подтвержден сам, после записи в failed-events, а не перекрыт следующим сообщением
        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(poison.offset() + 1));
        FailedEvent failed = failedEvent(poison.offset()).orElseThrow();

        RecordMetadata next = sendEvent(nextEvent);

        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(next.offset() + 1));
        assertThat(storedEvents(nextEvent)).isEqualTo(1);
        assertThat(failed.getErrorCode()).isEqualTo(ErrorCode.DESERIALIZATION_FAILED);
        assertThat(failed.getSourceTopic()).isEqualTo(TOPIC);
        assertThat(bytes(failed.getValue())).as("original bytes kept").isEqualTo(garbage);
        assertThat(bytes(failed.getKey())).as("original key kept").isEqualTo("dev-errors".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Event fails validation then failed-events as Avro before its offset is committed")
    void givenInvalidEvent_whenConsume_thenFailedEventAndOffsetCommitted() throws Exception {
        String nextEvent = unique("evt");

        RecordMetadata invalid = sendEvent("   ");
        RecordMetadata next = sendEvent(nextEvent);

        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(next.offset() + 1));
        assertThat(storedEvents(nextEvent)).isEqualTo(1);
        FailedEvent failed = failedEvent(invalid.offset()).orElseThrow();
        assertThat(failed.getErrorCode()).isEqualTo(ErrorCode.VALIDATION_FAILED);
        assertThat(failed.getErrorType()).isEqualTo(ErrorType.PERMANENT);
        assertThat(failed.getAttempts()).isEqualTo(1);
        assertThat(failed.getDeviceId()).isEqualTo("dev-errors");
        try (var avro = new KafkaAvroDeserializer()) {
            avro.configure(Map.of("schema.registry.url", schemaRegistryUrl(), "specific.avro.reader", true), false);
            DeviceEvent parked = (DeviceEvent) avro.deserialize(TOPIC, bytes(failed.getValue()));
            assertThat(parked.getEventId()).isEqualTo("   ");
        }
    }

    @Test
    @DisplayName("ClickHouse write fails on every attempt then after max-attempts failed-events, offset committed, next event stored")
    void givenStorageFailure_whenConsume_thenRetriedMaxAttemptsThenFailedEvent() throws Exception {
        String failing = unique("evt");
        String nextEvent = unique("evt");
        doThrow(new IllegalStateException("clickhouse is down"))
                .when(eventRepository).insert(argThat(record -> record != null && failing.equals(record.eventId())));

        RecordMetadata failed = sendEvent(failing);
        RecordMetadata next = sendEvent(nextEvent);

        // Offset неудачного сообщения подтвержден после failed-events, и раздел пошел дальше: следующее событие записано
        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isEqualTo(next.offset() + 1));
        verify(eventRepository, times(MAX_ATTEMPTS)).insert(argThat(record -> record != null && failing.equals(record.eventId())));
        assertThat(storedEvents(failing)).as("failed event is not stored").isZero();
        assertThat(storedEvents(nextEvent)).isEqualTo(1);
        FailedEvent event = failedEvent(failed.offset()).orElseThrow();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.STORAGE_UNAVAILABLE);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.TRANSIENT);
        assertThat(event.getAttempts()).isEqualTo(MAX_ATTEMPTS);
        assertThat(event.getDescription()).contains("IllegalStateException: clickhouse is down");
        try (var avro = new KafkaAvroDeserializer()) {
            avro.configure(Map.of("schema.registry.url", schemaRegistryUrl(), "specific.avro.reader", true), false);
            assertThat(((DeviceEvent) avro.deserialize(TOPIC, bytes(event.getValue()))).getEventId()).isEqualTo(failing);
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

    // Запись failed-events для исходного offset темы TOPIC (ключ записи - TOPIC-partition-offset)
    private Optional<FailedEvent> failedEvent(long sourceOffset) {
        try (var consumer = new KafkaConsumer<String, Object>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                "schema.registry.url", schemaRegistryUrl(),
                "specific.avro.reader", true))) {
            List<TopicPartition> partitions = consumer.partitionsFor(FAILED_TOPIC).stream()
                    .map(info -> new TopicPartition(FAILED_TOPIC, info.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            List<FailedEvent> events = new ArrayList<>();
            while (partitions.stream().anyMatch(partition -> consumer.position(partition) < end.get(partition))) {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> events.add((FailedEvent) record.value()));
            }
            return events.stream()
                    .filter(event -> TOPIC.equals(event.getSourceTopic()) && event.getSourceOffset() == sourceOffset)
                    .findFirst();
        }
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }
}
