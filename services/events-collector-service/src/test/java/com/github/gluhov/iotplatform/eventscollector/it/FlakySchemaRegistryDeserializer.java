package com.github.gluhov.iotplatform.eventscollector.it;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

// Avro-десериализатор для ItKafkaErrorHandlingTest: следующие FAILURES вызовов падают так,
// будто Schema Registry не ответил вовремя (SerializationException с причиной SocketTimeoutException)
public class FlakySchemaRegistryDeserializer  extends KafkaAvroDeserializer {
    public static final AtomicInteger FAILURES = new AtomicInteger();

    @Override
    public Object deserialize(String topic, Headers headers, byte[] bytes) {
        failIfRequested();
        return super.deserialize(topic, headers, bytes);
    }

    @Override
    public Object deserialize(String topic, Headers headers, ByteBuffer data) {
        failIfRequested();
        return super.deserialize(topic, headers, data);
    }

    private static void failIfRequested() {
        if (FAILURES.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            throw new SerializationException("Error retrieving Avro schema", new SocketTimeoutException("Read timed out"));
        }
    }
}
