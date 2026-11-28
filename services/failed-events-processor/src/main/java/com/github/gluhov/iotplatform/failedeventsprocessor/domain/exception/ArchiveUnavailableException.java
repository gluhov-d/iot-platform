package com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception;

/** Архив недоступен или медленнее тайм-аута: временная ошибка, сообщение повторяется. */
public class ArchiveUnavailableException extends RuntimeException {

    public ArchiveUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
