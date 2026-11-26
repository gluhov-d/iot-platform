package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Строковый ключ в строгом UTF-8: недопустимые байты дают ошибку десериализации, а не замену на U+FFFD.
 * Иначе StringDeserializer молча теряет исходные байты ключа, и восстановитель не может сохранить их.
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
