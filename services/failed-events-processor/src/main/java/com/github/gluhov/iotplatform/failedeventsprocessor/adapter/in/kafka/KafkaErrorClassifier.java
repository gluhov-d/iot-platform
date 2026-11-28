package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;

/**
 * Какие ошибки приёма повторять, а какие считать нечитаемой записью (как в device-collector-service).
 * Временная ошибка сети до Schema Registry или её 5xx - не свойство сообщения, повторяем; мусор и 404 схемы - нечитаемо.
 */
public final class KafkaErrorClassifier {

    private KafkaErrorClassifier() {
    }

    /** Запись не разобрана как FailedEvent (ErrorHandlingDeserializer передал DeserializationException). */
    public static boolean isUnreadable(Throwable exception) {
        return hasCause(exception, DeserializationException.class);
    }

    /**
     * Временный сбой: нет соединения с Schema Registry, таймаут, имя не разрешается, retriable-ошибка Kafka,
     * ответ Schema Registry 5xx. Обрезанное или повреждённое тело Avro (EOFException декодера) - не временное.
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

    static boolean hasCause(Throwable exception, Class<? extends Throwable> type) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (type.isInstance(e)) {
                return true;
            }
        }
        return false;
    }
}
