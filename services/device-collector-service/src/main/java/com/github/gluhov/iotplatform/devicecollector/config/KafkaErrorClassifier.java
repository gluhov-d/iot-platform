package com.github.gluhov.iotplatform.devicecollector.config;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;

/** Какие ошибки приёма из Kafka повторять, а какие - сразу отправлять в devices-dlt (как в events-collector-service). */
public final class KafkaErrorClassifier {

    private KafkaErrorClassifier() {
    }

    /** Сообщение не десериализуется по существу (не Avro, нет схемы, повреждённое тело) - повтор не поможет. */
    public static boolean isPoisonPill(Throwable exception) {
        return isDeserializationFailure(exception) && !isTransient(exception);
    }

    public static boolean isDeserializationFailure(Throwable exception) {
        return hasCause(exception, DeserializationException.class);
    }

    /**
     * Временный сбой - после восстановления то же сообщение обработается: сеть до Schema Registry (нет соединения,
     * таймаут, имя не разрешается), retriable-ошибка Kafka, ответ Schema Registry 5xx.
     * Не любой IOException: обрезанное или повреждённое тело Avro даёт EOFException/IOException декодера -
     * это свойство самого сообщения, иначе раздел повторял бы его бесконечно.
     */
    public static boolean isTransient(Throwable exception) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (e instanceof SocketException || e instanceof SocketTimeoutException
                    || e instanceof UnknownHostException || e instanceof HttpTimeoutException
                    || e instanceof RetriableException) {
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