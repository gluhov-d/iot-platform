package com.github.gluhov.iotplatform.devicecollector.config;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Строка в UTF-8 без замены недопустимых байтов. StringDeserializer молча заменяет их на U+FFFD и теряет исходные
 * байты, поэтому ключ декодируется строго: невалидный ключ - ошибка десериализации с исходными байтами (x-raw-key).
 */
public class StrictUtf8StringDeserializer implements Deserializer<String> {

    @Override
    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(data)).toString();
        } catch (CharacterCodingException e) {
            throw new SerializationException("невалидный UTF-8 в ключе", e);
        }
    }
}
