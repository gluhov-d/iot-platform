package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

import java.io.IOException;
import java.net.SocketTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Решение о повторе (R6, R7): нечитаемая запись - без повторов (объект unreadable), временная ошибка
 * Schema Registry и недоступный архив - без предела. Классификация - KafkaConfig.backOffFor.
 */
class KafkaConfigTest {

    private static final BackOff UNLIMITED = new ExponentialBackOff(500L, 2.0);

    @Test
    void givenUnparseableRecord_whenBackOff_thenNoRetries() {
        DeserializationException ex = new DeserializationException("не разобрано", new byte[]{1}, false,
                new SerializationException("Unknown magic byte!"));

        assertThat(KafkaConfig.backOffFor(ex, UNLIMITED)).isSameAs(KafkaConfig.NO_RETRIES);
    }

    @Test
    void givenSchemaRegistryNotFound_whenBackOff_thenNoRetriesBecauseRecordIsUnreadable() {
        DeserializationException ex = deserialization(new RestClientException("Schema 99999 not found", 404, 40403));

        assertThat(KafkaConfig.backOffFor(ex, UNLIMITED)).isSameAs(KafkaConfig.NO_RETRIES);
    }

    @Test
    void givenSchemaRegistryTimeout_whenBackOff_thenUnlimitedRetriesWithoutUnreadable() {
        DeserializationException ex = deserialization(new SocketTimeoutException("Read timed out"));

        assertThat(KafkaConfig.backOffFor(ex, UNLIMITED)).isSameAs(UNLIMITED);
    }

    @Test
    void givenSchemaRegistryServerError_whenBackOff_thenUnlimitedRetries() {
        DeserializationException ex = deserialization(new RestClientException("Internal error", 500, 50001));

        assertThat(KafkaConfig.backOffFor(ex, UNLIMITED)).isSameAs(UNLIMITED);
    }

    @Test
    void givenArchiveUnavailable_whenBackOff_thenUnlimitedRetries() {
        ArchiveUnavailableException ex = new ArchiveUnavailableException("timeout", new IOException());

        assertThat(KafkaConfig.backOffFor(ex, UNLIMITED)).isSameAs(UNLIMITED);
    }

    /** Отказ в доступе раньше пропускался (без восстановителя запись уходила с потерей) - теперь повтор, как недоступность. */
    @Test
    void givenArchiveDenied_whenBackOff_thenUnlimitedRetriesNotSkipped() {
        ArchiveDeniedException ex = new ArchiveDeniedException("Access Denied", null);

        assertThat(KafkaConfig.backOffFor(ex, UNLIMITED)).isSameAs(UNLIMITED);
    }

    private static DeserializationException deserialization(Throwable cause) {
        return new DeserializationException("не разобрано", new byte[]{1}, false,
                new SerializationException("Error deserializing Avro message", cause));
    }
}
