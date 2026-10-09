package com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka;

import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.eventscollector.config.KafkaProperties;
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

    // Отправка синхронная: версия SENT пишется только после подтверждения брокером
    @Override
    public void publish(String deviceId, long emittedAtMs) {
        DeviceInfo deviceInfo = DeviceInfo.newBuilder()
                .setDeviceId(deviceId)
                .setEmittedAtMs(emittedAtMs)
                .build();
        Duration timeout = kafkaProperties.sendTimeout();
        try {
            kafkaTemplate.send(kafkaProperties.devicesTopic(), deviceId, deviceInfo)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeviceInfoPublishException("Interrupted while sending DeviceInfo", e);
        } catch (ExecutionException e) {
            throw new DeviceInfoPublishException("Kafka rejected DeviceInfo", e.getCause());
        } catch (TimeoutException e) {
            throw new DeviceInfoPublishException("No Kafka acknowledgement within " + timeout, e);
        } catch (RuntimeException e) {
            // Ошибки сериализации и Schema Registry выбрасываются синхронно из send()
            throw new DeviceInfoPublishException("Failed to send DeviceInfo", e);
        }
    }
}
