package com.github.gluhov.iotplatform.eventscollector.config;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.io.IOException;

// Какие ошибки приема из Kafka повторять, а какие - откладывать в DLT
public final class KafkaErrorClassifier {

    private KafkaErrorClassifier() {
    }

    // "Ядовитое" сообщение: не десериализуется по существу (не Avro, нет схемы) - повтор не поможет
    public static boolean isPoisonPill(Throwable exception) {
        return hasCause(exception, DeserializationException.class) && !isTransient(exception);
    }

    // Временный сбой: сеть или таймаут (IOException, в т. ч. SocketTimeoutException), retriable-ошибка Kafka,
    // ответ Schema Registry 5xx - после восстановления то же сообщение обработается
    public static boolean isTransient(Throwable exception) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (e instanceof IOException || e instanceof RetriableException) {
                return true;
            }
            if (e instanceof RestClientException rest && rest.getStatus() >= 500) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasCause(Throwable exception, Class<? extends Throwable> type) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (type.isInstance(e)) {
                return true;
            }
        }
        return false;
    }
}