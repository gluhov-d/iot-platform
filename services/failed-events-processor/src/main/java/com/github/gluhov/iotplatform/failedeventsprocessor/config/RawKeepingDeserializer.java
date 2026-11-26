package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Сохраняет исходные байты ключа или значения записи failed-events до разбора: ссылка на массив, пришедший из сети,
 * кладётся в заголовок записи до вызова настоящего десериализатора. Копии и пересериализации нет, поэтому восстановитель
 * получает байты как есть, даже если разбор упал по причине, которую Spring Kafka не оборачивает в
 * DeserializationException (например, ClassCastException). Заголовки {@link #RAW_KEY_HEADER} и
 * {@link #RAW_VALUE_HEADER} исключаются из заголовков объекта в UnreadableRecorder.
 *
 * <p>Делегат задаётся свойствами {@link #KEY_DELEGATE} и {@link #VALUE_DELEGATE}; обёртку подключает
 * ErrorHandlingDeserializer через spring.deserializer.*.delegate.class.
 */
public final class RawKeepingDeserializer implements Deserializer<Object> {
    public static final String RAW_KEY_HEADER = "x-raw-key";
    public static final String RAW_VALUE_HEADER = "x-raw-value";
    public static final String KEY_DELEGATE = "raw.key.delegate.class";
    public static final String VALUE_DELEGATE = "raw.value.delegate.class";

    private Deserializer<Object> delegate;
    private String rawHeader;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        String property = isKey ? KEY_DELEGATE : VALUE_DELEGATE;
        String className = String.valueOf(configs.get(property));
        try {
            delegate = (Deserializer<Object>) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Не создан делегат " + className + " (" + property + ")", e);
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
