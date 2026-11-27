package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

/**
 * Заголовок исходного сообщения: имя и байты значения без изменений; {@code value == null} - у заголовка не было значения.
 * Имя может повторяться: порядок и повторы сохраняются как в исходной записи (R26).
 */
public record HeaderEntry(String name, byte[] value) {
}
