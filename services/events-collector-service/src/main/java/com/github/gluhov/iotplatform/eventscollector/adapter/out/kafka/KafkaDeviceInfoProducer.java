package com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka;

import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.eventscollector.config.KafkaErrorClassifier;
import com.github.gluhov.iotplatform.eventscollector.config.KafkaProperties;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.PublisherUnavailableException;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.DeviceInfoPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@RequiredArgsConstructor
public class KafkaDeviceInfoProducer implements DeviceInfoPublisher {
    private final KafkaTemplate<String, DeviceInfo> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    // Отправка синхронная: версия SENT пишется только после подтверждения брокером.
    // Временная недоступность (сеть, таймаут, retriable-ошибка Kafka, 5xx Schema Registry) - PublisherUnavailableException:
    // попытка не засчитывается. Остальное (сериализация, слишком большое сообщение) - DeviceInfoPublishException
    @Override
    public void publish(DeviceInfo deviceInfo) {
        Duration timeout = kafkaProperties.sendTimeout();
        try {
            // Ключ - deviceId: все события устройства в одном разделе, по порядку
            kafkaTemplate.send(kafkaProperties.devicesTopic(), deviceInfo.getDeviceId(), deviceInfo)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublisherUnavailableException("Interrupted while sending DeviceInfo", e);
        } catch (ExecutionException e) {
            throw classify("Kafka rejected DeviceInfo", e.getCause());
        } catch (TimeoutException e) {
            throw new PublisherUnavailableException("No Kafka acknowledgement within " + timeout, e);
        } catch (RuntimeException e) {
            // Ошибки сериализации и Schema Registry выбрасываются синхронно из send()
            throw classify("Failed to send DeviceInfo", e);
        }
    }

    private static RuntimeException classify(String message, Throwable cause) {
        return KafkaErrorClassifier.isTransient(cause)
                ? new PublisherUnavailableException(message, cause)
                : new DeviceInfoPublishException(message, cause);
    }
}