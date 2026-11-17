package com.github.gluhov.iotplatform.deviceservice.adapter.out.kafka;

import com.github.gluhov.iotplatform.deviceservice.config.CommandProperties;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.CommandNotAcceptedException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceCommandPublisher;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Команда → тема devices: ключ - deviceId (порядок команд одного устройства), подтверждение брокера ждём не дольше
 * sendTimeout от начала отправки. Любой сбой (брокер, срок, Schema Registry) - CommandNotAcceptedException.
 */
@Component
public class KafkaDeviceCommandPublisher implements DeviceCommandPublisher {

    private final KafkaTemplate<String, DeviceInfo> template;
    private final DeviceInfoFactory factory;
    private final String topic;
    private final Duration sendTimeout;

    public KafkaDeviceCommandPublisher(KafkaTemplate<String, DeviceInfo> template, DeviceInfoFactory factory,
                                       CommandProperties properties) {
        this.template = template;
        this.factory = factory;
        this.topic = properties.devicesTopic();
        this.sendTimeout = properties.sendTimeout();
    }

    @Override
    public void publish(DeviceCommand command) {
        DeviceInfo message = factory.toMessage(command);
        long started = System.nanoTime();
        try {
            var acknowledgement = template.send(topic, command.deviceId(), message);
            // Срок - от начала отправки: время, которое send() ждал метаданные, вычитается из ожидания подтверждения
            long remainingMs = Math.max(1L, sendTimeout.toMillis() - elapsedMs(started));
            acknowledgement.get(remainingMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandNotAcceptedException(command.deviceId(), e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new CommandNotAcceptedException(command.deviceId(), e);
        }
    }

    private static long elapsedMs(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}
