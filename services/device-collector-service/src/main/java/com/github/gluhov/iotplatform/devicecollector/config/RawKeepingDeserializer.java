package com.github.gluhov.iotplatform.devicecollector.config;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Сохраняет исходные байты ключа или значения до разбора: кладёт ссылку на тот же массив в заголовок записи
 * (x-raw-key или x-raw-value) и только потом вызывает настоящий десериализатор. Копии и пересериализации нет,
 * поэтому FailedEvent получает сообщение побайтно, даже если оно разобралось, но не прошло проверку.
 * Делегат задаётся свойством x-raw.key.delegate или x-raw.value.delegate (application.yaml).
 * Заголовки обёртки FailedEvent не копирует (FailedEventRecoverer).
 */
public final class RawKeepingDeserializer implements Deserializer<Object> {
    public static final String RAW_KEY_HEADER = "x-raw-key";
    public static final String RAW_VALUE_HEADER = "x-raw-value";

    private Deserializer<Object> delegate;
    private String rawHeader;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        String property = isKey ? "x-raw.key.delegate" : "x-raw.value.delegate";
        String name = (String) configs.get(property);
        try {
            delegate = (Deserializer<Object>) Class.forName(name).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Не создан делегат " + property + " = " + name, e);
        }
        delegate.configure(configs, isKey);
        rawHeader = isKey ? RAW_KEY_HEADER : RAW_VALUE_HEADER;
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return delegate.deserialize(topic, data);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        if (headers != null && data != null) {
            headers.add(rawHeader, data);
        }
        return delegate.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
