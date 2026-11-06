package com.github.gluhov.iotplatform.eventscollector.domain.exception;

/**
 * Публикация не удалась из-за временной недоступности Kafka или Schema Registry (сеть, таймаут, 5xx). Запись outbox
 * не виновата: попытка не засчитывается, запись будет выбрана снова - иначе простой брокера дольше
 * OUTBOX_MAX_ATTEMPTS итераций навсегда оставил бы её в FAILED.
 */
public class PublisherUnavailableException extends RuntimeException {
    public PublisherUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}