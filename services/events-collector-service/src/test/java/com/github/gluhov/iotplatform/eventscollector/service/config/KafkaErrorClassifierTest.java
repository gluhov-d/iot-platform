package com.github.gluhov.iotplatform.eventscollector.service.config;

import com.github.gluhov.iotplatform.eventscollector.config.KafkaErrorClassifier;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.EventPersistenceException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.net.SocketTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaErrorClassifierTest {

    @Test
    @DisplayName("Schema Registry read timeout during deserialization then transient, retried")
    void givenSchemaRegistryTimeout_whenClassify_thenNotPoison() {
        Exception error = deserialization(new SerializationException("Error retrieving Avro schema",
                new SocketTimeoutException("Read timed out")));

        assertThat(KafkaErrorClassifier.isTransient(error)).isTrue();
        assertThat(KafkaErrorClassifier.isPoisonPill(error)).isFalse();
    }

    @Test
    @DisplayName("Schema Registry answers 5xx then transient, retried")
    void givenSchemaRegistryServerError_whenClassify_thenNotPoison() {
        Exception error = deserialization(new RestClientException("Internal Server Error", 503, 50301));

        assertThat(KafkaErrorClassifier.isPoisonPill(error)).isFalse();
    }

    @Test
    @DisplayName("Message is not Avro or schema not found then poison pill, moved to DLT")
    void givenBrokenMessage_whenClassify_thenPoison() {
        assertThat(KafkaErrorClassifier.isPoisonPill(deserialization(new IllegalArgumentException("Unknown magic byte!"))))
                .isTrue();
        assertThat(KafkaErrorClassifier.isPoisonPill(deserialization(new RestClientException("Schema not found", 404, 40403))))
                .isTrue();
    }

    @Test
    @DisplayName("Storage error in listener then not a poison pill, retried by default")
    void givenStorageError_whenClassify_thenNotPoison() {
        Exception error = new ListenerExecutionFailedException("listener failed",
                new EventPersistenceException("insert device_events failed", new IllegalStateException("clickhouse down")));

        assertThat(KafkaErrorClassifier.isPoisonPill(error)).isFalse();
    }

    private static DeserializationException deserialization(Throwable cause) {
        return new DeserializationException("failed to deserialize", new byte[]{1, 2, 3}, false, cause);
    }
}