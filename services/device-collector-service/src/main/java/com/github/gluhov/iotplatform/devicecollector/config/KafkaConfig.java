package com.github.gluhov.iotplatform.devicecollector.config;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Обработка ошибок приёма из devices:
 * <ul>
 *   <li>невалидное или недесериализуемое сообщение - сразу в failed-events;</li>
 *   <li>ошибка записи в PostgreSQL (шард или Proxy недоступны) и непредвиденная ошибка - app.processing.max-attempts
 *       попыток с растущей паузой, затем failed-events: сообщения остальных устройств раздела продолжают обрабатываться;</li>
 *   <li>Schema Registry недоступен - повторять без ограничения: это сбой не сообщения, а инфраструктуры,
 *       иначе в failed-events ушли бы все сообщения подряд;</li>
 *   <li>смещение группы вне диапазона темы (сообщения удалены по сроку хранения до обработки) - обработчик
 *       останавливается, health DOWN (DevicesConsumerOffsets, ADR-0011);</li>
 *   <li>раздел без смещения у работающей группы (к теме добавили раздел) - читается с начала без остановки и перезапуска
 *       (NoOffsetRecoveringConsumer, R18).</li>
 * </ul>
 * Запись в failed-events делает {@code FailedEventRecoverer}. Offset сообщения подтверждается только после успешной
 * публикации; не удалось опубликовать - сообщение не подтверждено и будет обработано снова.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaConfig {
    private static final long MAX_BACKOFF_MS = 10_000L;
    private static final BackOff NO_RETRIES = new FixedBackOff(0L, 0L);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(ConsumerRecordRecoverer failedEvents, ProcessingProperties processing,
                                          DeviceMetrics metrics, DevicesConsumerOffsets offsets) {
        DefaultErrorHandler handler = new DevicesErrorHandler(offsets, (record, exception) -> {
            FailureReason reason = reasonOf(exception);
            // Не опубликовано в failed-events - исключение, сообщение будет обработано снова: метрики ниже не увеличены
            failedEvents.accept(record, exception);
            if (reason == FailureReason.DESERIALIZATION) {
                // До обработчика сообщение не дошло - получение и ошибочную попытку учитываем здесь
                metrics.received();
                metrics.processingError(reason);
            }
            metrics.failed(reason);
        }, storageBackOff(processing));
        handler.addNotRetryableExceptions(InvalidDeviceInfoException.class);
        // Повторять ли ошибку десериализации, решает функция ниже: временный сбой - да, "ядовитое" сообщение - нет
        handler.removeClassification(DeserializationException.class);
        BackOff schemaRegistryBackOff = unlimitedBackOff();
        handler.setBackOffFunction((record, exception) -> {
            if (KafkaErrorClassifier.isPoisonPill(exception)) {
                return NO_RETRIES;
            }
            return KafkaErrorClassifier.isDeserializationFailure(exception) ? schemaRegistryBackOff : null;
        });
        handler.setCommitRecovered(true);
        return handler;
    }

    /** R18: раздел без смещения читается с начала в потоке потребителя, без остановки обработчика. */
    @Bean
    DefaultKafkaConsumerFactoryCustomizer noOffsetRecovery() {
        return factory -> addNoOffsetRecovery(factory);
    }

    private static <K, V> void addNoOffsetRecovery(DefaultKafkaConsumerFactory<K, V> factory) {
        factory.addPostProcessor(NoOffsetRecoveringConsumer::wrap);
    }

    /** Номер доставки в заголовке kafka_deliveryAttempt: повтор после ошибки не считается новым сообщением. */
    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> deliveryAttemptHeader() {
        return container -> container.getContainerProperties().setDeliveryAttemptHeader(true);
    }

    /**
     * Ошибки вне обработки сообщения: смещение группы вне диапазона темы (auto-offset-reset = none) останавливает
     * обработчик, как CommonContainerStoppingErrorHandler: тихий переход на начало темы применил бы событие роутера
     * после потерянного DELETE администратора. Остальное - как у DefaultErrorHandler.
     */
    static final class DevicesErrorHandler extends DefaultErrorHandler {
        private static final long STOP_WAIT_MS = 10_000L;

        private final DevicesConsumerOffsets offsets;
        private final SimpleAsyncTaskExecutor stopper = new SimpleAsyncTaskExecutor("devices-listener-stop-");

        DevicesErrorHandler(DevicesConsumerOffsets offsets, ConsumerRecordRecoverer recoverer, BackOff backOff) {
            super(recoverer, backOff);
            this.offsets = offsets;
        }

        @Override
        public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
                                         MessageListenerContainer container, boolean batchListener) {
            OffsetOutOfRangeException outOfRange = outOfRangeCause(thrownException);
            if (outOfRange == null) {
                super.handleOtherException(thrownException, consumer, container, batchListener);
                return;
            }
            offsets.offsetOutOfRange(outOfRange.offsetOutOfRangePartitions());
            // Остановка - из другого потока: stop() ждёт завершения потока обработчика, а это он и есть
            stopper.execute(() -> container.stopAbnormally(() -> {
            }));
            try {
                ListenerUtils.stoppableSleep(container, STOP_WAIT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new KafkaException("devices listener stopped: committed offset out of range", thrownException);
        }

        private static OffsetOutOfRangeException outOfRangeCause(Throwable exception) {
            for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
                if (e instanceof OffsetOutOfRangeException outOfRange) {
                    return outOfRange;
                }
            }
            return null;
        }
    }

    // max-attempts попыток всего: первая и max-attempts - 1 повторов (паузы 1 с, 2 с, ...)
    static BackOff storageBackOff(ProcessingProperties processing) {
        if (processing.maxAttempts() == 1) {
            // ExponentialBackOffWithMaxRetries(0) не останавливается: одна попытка - сразу в failed-events
            return NO_RETRIES;
        }
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(processing.maxAttempts() - 1);
        backOff.setInitialInterval(processing.retryInterval().toMillis());
        backOff.setMultiplier(processing.retryMultiplier());
        backOff.setMaxInterval(MAX_BACKOFF_MS);
        return backOff;
    }

    private static BackOff unlimitedBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(MAX_BACKOFF_MS);
        return backOff;
    }

    static FailureReason reasonOf(Exception exception) {
        return KafkaErrorClassifier.isDeserializationFailure(exception) ? FailureReason.DESERIALIZATION
                : FailureReason.of(exception);
    }
}
