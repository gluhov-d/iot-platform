package com.github.gluhov.iotplatform.devicecollector.domain.model;

/** Итог обработки сообщения и шард, на котором лежит устройство (current_database() шарда). */
public record ProcessingResult(ProcessingOutcome outcome, String shard) {

    public static final String UNKNOWN_SHARD = "unknown";
}