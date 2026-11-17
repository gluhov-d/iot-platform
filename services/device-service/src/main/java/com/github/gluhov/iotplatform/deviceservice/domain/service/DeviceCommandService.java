package com.github.gluhov.iotplatform.deviceservice.domain.service;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CommandNotAcceptedException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceAlreadyExistsException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceNotFoundException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand.Operation;
import com.github.gluhov.iotplatform.deviceservice.domain.port.in.ManageDeviceUseCase;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceCommandPublisher;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceReadRepository;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceServiceMetrics;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;

/**
 * Команды администратора. Предварительная проверка - по модели чтения: устройство «есть», если карточка есть и оно не
 * удалено. Публикация - через порт; ответ клиенту - только после подтверждения Kafka (исключение - при отказе).
 */
@Service
public class DeviceCommandService implements ManageDeviceUseCase {

    static final String CODE_DEVICE_NOT_FOUND = "DEVICE_NOT_FOUND";
    static final String CODE_DEVICE_ALREADY_EXISTS = "DEVICE_ALREADY_EXISTS";
    static final String CODE_COMMAND_NOT_ACCEPTED = "COMMAND_NOT_ACCEPTED";

    private final DeviceReadRepository readRepository;
    private final DeviceCommandPublisher publisher;
    private final DeviceServiceMetrics metrics;
    private final Clock clock;

    public DeviceCommandService(DeviceReadRepository readRepository, DeviceCommandPublisher publisher,
                                DeviceServiceMetrics metrics, Clock clock) {
        this.readRepository = readRepository;
        this.publisher = publisher;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public DeviceCommand update(String deviceId, DeviceCommand.Fields fields) {
        requireExisting(Operation.UPDATE, deviceId);
        return publish(Operation.UPDATE, deviceId, fields);
    }

    @Override
    public DeviceCommand create(String deviceId, DeviceCommand.Fields fields) {
        if (readRepository.findById(deviceId).isPresent()) {
            metrics.commandRejected(Operation.CREATE, CODE_DEVICE_ALREADY_EXISTS);
            throw new DeviceAlreadyExistsException(deviceId);
        }
        return publish(Operation.CREATE, deviceId, fields);
    }

    /** Мягкое удаление: команда без полей администратора; событие роутера после неё устройство не возвращает. */
    @Override
    public DeviceCommand delete(String deviceId) {
        requireExisting(Operation.DELETE, deviceId);
        return publish(Operation.DELETE, deviceId, null);
    }

    private void requireExisting(Operation operation, String deviceId) {
        if (readRepository.findById(deviceId).isEmpty()) {
            metrics.commandRejected(operation, CODE_DEVICE_NOT_FOUND);
            throw new DeviceNotFoundException(deviceId);
        }
    }

    private DeviceCommand publish(Operation operation, String deviceId, DeviceCommand.Fields fields) {
        DeviceCommand command = new DeviceCommand(UUID.randomUUID(), operation, deviceId, fields, clock.instant());
        long started = System.nanoTime();
        try {
            publisher.publish(command);
        } catch (CommandNotAcceptedException e) {
            metrics.commandRejected(operation, CODE_COMMAND_NOT_ACCEPTED);
            throw e;
        } finally {
            metrics.commandPublished(operation, Duration.ofNanos(System.nanoTime() - started));
        }
        metrics.commandAccepted(operation);
        return command;
    }
}
