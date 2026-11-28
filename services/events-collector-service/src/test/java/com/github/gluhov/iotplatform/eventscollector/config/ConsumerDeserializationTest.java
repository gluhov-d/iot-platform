package com.github.gluhov.iotplatform.eventscollector.config;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.SerializationUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.github.gluhov.iotplatform.eventscollector.config.RawKeepingDeserializer.RAW_KEY_HEADER;
import static com.github.gluhov.iotplatform.eventscollector.config.RawKeepingDeserializer.RAW_VALUE_HEADER;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Настройки Avro-десериализатора из настоящего application.yaml (без Spring-контекста и Docker): сервис читает только
 * DeviceEvent, запись другого типа даже с существующим schemaId не десериализуется. ErrorHandlingDeserializer превращает
 * эту ошибку в DeserializationException с исходными байтами - это проверяют интеграционные тесты.
 */
class ConsumerDeserializationTest {
    private static final String TOPIC = "events";
    private static final String REGISTRY = "mock://consumer-deserialization-test";
    private static final String CONSUMER_PROPERTIES = "spring.kafka.consumer.properties.";

    private KafkaAvroDeserializer deserializer;
    private KafkaAvroSerializer serializer;

    @BeforeEach
    void setUp() {
        deserializer = new KafkaAvroDeserializer();
        deserializer.configure(consumerConfig(), false);
        serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of("schema.registry.url", REGISTRY), false);
    }

    @AfterEach
    void tearDown() {
        deserializer.close();
        serializer.close();
    }

    @Test
    @DisplayName("DeviceEvent is read as the generated class")
    void givenExpectedType_whenDeserialize_thenSpecificRecord() {
        DeviceEvent sent = event("evt-1", "dev-1", 1_000L);

        Object read = deserializer.deserialize(TOPIC, serializer.serialize(TOPIC, sent));

        assertThat(read).isEqualTo(sent);
    }

    @Test
    @DisplayName("Valid Avro of another type with a registered schema id is not read as DeviceEvent")
    void givenAnotherAvroType_whenDeserialize_thenSerializationException() {
        byte[] foreign = serializer.serialize("other-topic",
                DeviceInfo.newBuilder().setDeviceId("dev-1").setEmittedAtMs(1_000L).build());

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, foreign))
                .isInstanceOf(SerializationException.class);
    }

    @Test
    @DisplayName("Key is not strict UTF-8: raw key kept in the header, DeserializationException holds the same bytes")
    void givenInvalidUtf8Key_whenDeserialize_thenRawKeyKeptAndDeserializationFailed() {
        byte[] key = {(byte) 0xFF, (byte) 0xFE, 0x41};
        ErrorHandlingDeserializer<Object> keys = new ErrorHandlingDeserializer<>();
        keys.configure(consumerConfig(), true);
        Headers headers = new RecordHeaders();

        assertThat(keys.deserialize(TOPIC, headers, key)).isNull();

        assertThat(headers.lastHeader(RAW_KEY_HEADER).value()).isEqualTo(key);
        DeserializationException failure = exceptionFrom(headers, SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER);
        assertThat(failure.isKey()).isTrue();
        assertThat(failure.getData()).isEqualTo(key);
    }

    @Test
    @DisplayName("Garbage value: raw value kept in the header before Avro decoding, DeserializationException holds the same bytes")
    void givenGarbageValue_whenDeserialize_thenRawValueKeptAndDeserializationFailed() {
        byte[] garbage = {1, 2, 3, 0x7F};
        ErrorHandlingDeserializer<Object> values = new ErrorHandlingDeserializer<>();
        values.configure(consumerConfig(), false);
        Headers headers = new RecordHeaders();

        assertThat(values.deserialize(TOPIC, headers, garbage)).isNull();

        assertThat(headers.lastHeader(RAW_VALUE_HEADER).value()).isEqualTo(garbage);
        DeserializationException failure = exceptionFrom(headers, SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
        assertThat(failure.isKey()).isFalse();
        assertThat(failure.getData()).isEqualTo(garbage);
    }

    // Исключение, которое ErrorHandlingDeserializer положил в заголовок записи
    private static DeserializationException exceptionFrom(Headers headers, String header) {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(TOPIC, 0, 0L, 0L, TimestampType.CREATE_TIME,
                -1, -1, null, null, headers, Optional.empty());
        return SerializationUtils.getExceptionFromHeader(record, header, new LogAccessor(ConsumerDeserializationTest.class));
    }

    // Свойства потребителя ровно как в application.yaml (spring.kafka.consumer.properties), адрес реестра - тестовый
    private static Map<String, Object> consumerConfig() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application.yaml"));
        Properties all = yaml.getObject();
        Map<String, Object> config = new HashMap<>();
        // Не stringPropertyNames()/getProperty(): значения true и чисел в нём не строки, и они бы пропали
        all.forEach((key, value) -> {
            if (key.toString().startsWith(CONSUMER_PROPERTIES)) {
                config.put(key.toString().substring(CONSUMER_PROPERTIES.length()), value);
            }
        });
        config.put("schema.registry.url", REGISTRY);
        return config;
    }
}