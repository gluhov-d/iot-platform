package com.github.gluhov.iotplatform.devicecollector.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Обёртка над Consumer для devices (R18, ADR-0011). При auto-offset-reset = none раздел без смещения группы (новая
 * группа или раздел, добавленный к теме у работающей группы) даёт NoOffsetForPartitionException в position(...) или
 * poll(...): адаптер @KafkaListener - ConsumerSeekAware, Spring Kafka вызывает position() при назначении раздела, и
 * контейнер считает ошибку фатальной. Здесь только эти два метода: такие разделы переводятся в начало темы
 * (seekToBeginning), вызов повторяется один раз. Всё выполняется в потоке потребителя для его разделов, поэтому другие
 * участники группы не мешают. OffsetOutOfRangeException не перехватывается: обработчик останавливается (KafkaConfig).
 */
@Slf4j
final class NoOffsetRecoveringConsumer {

    private NoOffsetRecoveringConsumer() {
    }

    @SuppressWarnings("unchecked")
    static <K, V> Consumer<K, V> wrap(Consumer<K, V> delegate) {
        return (Consumer<K, V>) Proxy.newProxyInstance(Consumer.class.getClassLoader(), new Class<?>[]{Consumer.class},
                (proxy, method, args) -> {
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        if (isReading(method) && e.getCause() instanceof NoOffsetForPartitionException noOffset) {
                            delegate.seekToBeginning(noOffset.partitions());
                            log.info("No committed offset of the group for {}: reading from the beginning of the topic",
                                    noOffset.partitions());
                            return invokeOnce(delegate, method, args);
                        }
                        throw e.getCause();
                    }
                });
    }

    private static boolean isReading(Method method) {
        return "position".equals(method.getName()) || "poll".equals(method.getName());
    }

    private static Object invokeOnce(Object delegate, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
