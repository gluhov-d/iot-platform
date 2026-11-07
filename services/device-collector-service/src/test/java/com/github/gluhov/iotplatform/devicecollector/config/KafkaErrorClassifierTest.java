package com.github.gluhov.iotplatform.devicecollector.config;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.DeserializationException;

import com.github.gluhov.iotplatform.devicecollector.data.TestData;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class KafkaErrorClassifierTest {

    @Test
    @DisplayName("Not Avro or unknown schema: poison pill, retrying will not help")
    void givenUndeserializableMessage_whenClassify_thenPoisonPill() {
        assertThat(KafkaErrorClassifier.isPoisonPill(deserialization(new SerializationException("Unknown magic byte!"))))
                .isTrue();
        assertThat(KafkaErrorClassifier.isPoisonPill(deserialization(new RestClientException("Schema not found", 404, 40403))))
                .isTrue();
    }

    @Test
    @DisplayName("Schema Registry down or answering 5xx, Kafka timeout: transient, retried")
    void givenInfrastructureFailure_whenClassify_thenTransient() {
        assertThat(KafkaErrorClassifier.isTransient(deserialization(new ConnectException("Connection refused")))).isTrue();
        assertThat(KafkaErrorClassifier.isTransient(deserialization(new RestClientException("Internal", 500, 50001)))).isTrue();
        assertThat(KafkaErrorClassifier.isTransient(new TimeoutException("timeout"))).isTrue();
        assertThat(KafkaErrorClassifier.isPoisonPill(deserialization(new ConnectException("Connection refused")))).isFalse();
    }

    @Test
    @DisplayName("Avro with a valid schema id but a truncated body: poison pill, not an endless retry")
    void givenTruncatedAvroBody_whenDeserialize_thenPoisonPill() {
        MockSchemaRegistryClient registry = new MockSchemaRegistryClient();
        Map<String, Object> config = Map.of("schema.registry.url", "mock://classifier", "specific.avro.reader", true);
        byte[] valid;
        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer(registry, config)) {
            valid = serializer.serialize("devices", TestData.info("dev-1", "evt-1", DeviceUpsertEventType.CREATED, TestData.T0).build());
        }
        // Magic byte и id схемы на месте, тело обрезано - так выглядит повреждённое сообщение
        byte[] truncated = Arrays.copyOf(valid, valid.length - 8);
        Throwable error;
        try (KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(registry, config)) {
            error = catchThrowable(() -> deserializer.deserialize("devices", truncated));
        }

        assertThat(error).isNotNull();
        // Причина - ошибка декодера (IOException), которую раньше принимали за сбой сети
        assertThat(causes(error)).anyMatch(IOException.class::isInstance);
        assertThat(KafkaErrorClassifier.isPoisonPill(deserialization(error))).isTrue();
    }

    @Test
    @DisplayName("Schema Registry timeout: transient")
    void givenSocketTimeout_whenClassify_thenTransient() {
        assertThat(KafkaErrorClassifier.isTransient(deserialization(new SocketTimeoutException("Read timed out")))).isTrue();
    }

    @Test
    @DisplayName("Not a deserialization error: neither poison pill nor deserialization failure")
    void givenOtherError_whenClassify_thenNotDeserialization() {
        assertThat(KafkaErrorClassifier.isDeserializationFailure(new IllegalStateException("x"))).isFalse();
        assertThat(KafkaErrorClassifier.isPoisonPill(new IllegalStateException("x"))).isFalse();
    }

    private static java.util.List<Throwable> causes(Throwable error) {
        java.util.List<Throwable> all = new java.util.ArrayList<>();
        for (Throwable e = error; e != null && !all.contains(e); e = e.getCause()) {
            all.add(e);
        }
        return all;
    }

    private static DeserializationException deserialization(Throwable cause) {
        return new DeserializationException("failed", new byte[]{0}, false, cause);
    }
}