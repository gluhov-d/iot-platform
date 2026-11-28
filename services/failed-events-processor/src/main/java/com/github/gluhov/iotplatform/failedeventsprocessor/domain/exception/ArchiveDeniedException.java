package com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception;

/** Архив отказал в доступе (учётка без права Put): повтор не поможет, нужна настройка прав. */
public class ArchiveDeniedException extends RuntimeException {

    public ArchiveDeniedException(String message, Throwable cause) {
        super(message, cause);
    }
}
