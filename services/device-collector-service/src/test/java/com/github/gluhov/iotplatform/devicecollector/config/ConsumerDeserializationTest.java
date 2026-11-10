package com.github.gluhov.iotplatform.devicecollector.config;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Настройки Avro-десериализатора из настоящего application.yaml (без Spring-контекста и Docker): сервис читает только
 * DeviceInfo, запись другого типа даже с существующим schemaId не десериализуется. ErrorHandlingDeserializer превращает
 * эту ошибку в DeserializationException с исходными байтами - это проверяют интеграционные тесты.
 */
class ConsumerDeserializationTest {
    private static final String TOPIC = "devices";
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
    @DisplayName("DeviceInfo is read as the generated class")
    void givenExpectedType_whenDeserialize_thenSpecificRecord() {
        DeviceInfo sent = info("dev-1", "evt-1", DeviceUpsertEventType.CREATED, T0).build();

        Object read = deserializer.deserialize(TOPIC, serializer.serialize(TOPIC, sent));

        assertThat(read).isEqualTo(sent);
    }

    @Test
    @DisplayName("DeviceInfo of contracts 1.0.0 (two fields) is still read as DeviceInfo")
    void givenVersion100_whenDeserialize_thenDeviceInfoWithDefaults() {
        Schema v100 = new Schema.Parser().parse("""
                {"type": "record", "name": "DeviceInfo", "namespace": "com.github.gluhov.iotplatform.events.avro",
                 "fields": [{"name": "deviceId", "type": "string"}, {"name": "emittedAtMs", "type": "long"}]}""");
        GenericRecord old = new GenericData.Record(v100);
        old.put("deviceId", "dev-1");
        old.put("emittedAtMs", 1_000L);

        Object read = deserializer.deserialize(TOPIC, serializer.serialize(TOPIC, old));

        assertThat(read).isInstanceOf(DeviceInfo.class);
        assertThat(((DeviceInfo) read).getDeviceId()).isEqualTo("dev-1");
        assertThat(((DeviceInfo) read).getEventType()).isEqualTo(DeviceUpsertEventType.SNAPSHOT);
    }

    @Test
    @DisplayName("Valid Avro of another type with a registered schema id is not read as DeviceInfo")
    void givenAnotherAvroType_whenDeserialize_thenSerializationException() {
        byte[] foreign = serializer.serialize("other-topic", DeviceEvent.newBuilder()
                .setEventId("evt-x").setDeviceId("dev-1").setTimestampMs(1_000L).setEventType("OTHER").build());

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, foreign))
                .isInstanceOf(SerializationException.class);
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