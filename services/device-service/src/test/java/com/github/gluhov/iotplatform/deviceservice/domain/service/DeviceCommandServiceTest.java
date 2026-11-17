package com.github.gluhov.iotplatform.deviceservice.domain.service;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CommandNotAcceptedException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceAlreadyExistsException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceNotFoundException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand.Operation;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceCommandPublisher;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceReadRepository;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceServiceMetrics;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeviceCommandServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-14T10:00:00Z");

    private final DeviceReadRepository readRepository = mock(DeviceReadRepository.class);
    private final DeviceCommandPublisher publisher = mock(DeviceCommandPublisher.class);
    private final DeviceServiceMetrics metrics = mock(DeviceServiceMetrics.class);
    private final DeviceCommandService service =
            new DeviceCommandService(readRepository, publisher, metrics, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void givenDeviceExists_whenUpdate_thenUpdateCommandPublishedWithRequestFields() {
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(device("dev-1")));
        DeviceCommand.Fields fields = fields("ACTIVE");

        DeviceCommand returned = service.update("dev-1", fields);

        ArgumentCaptor<DeviceCommand> captor = ArgumentCaptor.forClass(DeviceCommand.class);
        verify(publisher).publish(captor.capture());
        DeviceCommand published = captor.getValue();
        assertThat(published).isEqualTo(returned);
        assertThat(published.operation()).isEqualTo(Operation.UPDATE);
        assertThat(published.deviceId()).isEqualTo("dev-1");
        assertThat(published.fields()).isEqualTo(fields);
        assertThat(published.acceptedAt()).isEqualTo(NOW);
        assertThat(published.commandId()).isNotNull();
    }

    @Test
    void givenDeviceExists_whenUpdateTwice_thenEachCommandHasOwnEventId() {
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(device("dev-1")));

        DeviceCommand first = service.update("dev-1", fields("ACTIVE"));
        DeviceCommand second = service.update("dev-1", fields("ACTIVE"));

        assertThat(first.commandId()).isNotEqualTo(second.commandId());
    }

    @Test
    void givenNoDevice_whenUpdate_thenNotFoundAndNothingPublished() {
        when(readRepository.findById("dev-1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.update("dev-1", fields("ACTIVE")))
                .isInstanceOf(DeviceNotFoundException.class);

        verify(publisher, never()).publish(any());
        verify(metrics).commandRejected(Operation.UPDATE, "DEVICE_NOT_FOUND");
    }

    @Test
    void givenDeletedDevice_whenUpdate_thenNotFoundAndNothingPublished() {
        // Модель чтения отдаёт пусто и для удалённого устройства, и для устройства без карточки
        when(readRepository.findById("dev-1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.update("dev-1", fields("ACTIVE")))
                .isInstanceOf(DeviceNotFoundException.class);

        verify(publisher, never()).publish(any());
    }

    @Test
    void givenPublisherFails_whenUpdate_thenCommandNotAcceptedAndRejectionCounted() {
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(device("dev-1")));
        org.mockito.Mockito.doThrow(new CommandNotAcceptedException("dev-1", null))
                .when(publisher).publish(any());

        assertThatThrownBy(() -> service.update("dev-1", fields("ACTIVE")))
                .isInstanceOf(CommandNotAcceptedException.class);

        verify(metrics).commandRejected(Operation.UPDATE, "COMMAND_NOT_ACCEPTED");
        verify(metrics, never()).commandAccepted(any());
    }

    @Test
    void givenPublished_whenUpdate_thenAcceptedCountedAndPublishTimeRecorded() {
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(device("dev-1")));

        service.update("dev-1", fields("ACTIVE"));

        verify(metrics).commandAccepted(Operation.UPDATE);
        verify(metrics).commandPublished(eq(Operation.UPDATE), any(Duration.class));
    }

    @Test
    void givenNoDevice_whenCreate_thenCreateCommandPublished() {
        when(readRepository.findById("dev-1")).thenReturn(Optional.empty());
        DeviceCommand.Fields fields = fields("NEW");

        DeviceCommand returned = service.create("dev-1", fields);

        ArgumentCaptor<DeviceCommand> captor = ArgumentCaptor.forClass(DeviceCommand.class);
        verify(publisher).publish(captor.capture());
        assertThat(captor.getValue()).isEqualTo(returned);
        assertThat(returned.operation()).isEqualTo(Operation.CREATE);
        assertThat(returned.deviceId()).isEqualTo("dev-1");
        assertThat(returned.fields()).isEqualTo(fields);
        verify(metrics).commandAccepted(Operation.CREATE);
    }

    @Test
    void givenDeletedDevice_whenCreate_thenCreateCommandPublished() {
        // Удалённое устройство в модели чтения не видно: создание снова разрешено (CREATE восстанавливает запись)
        when(readRepository.findById("dev-1")).thenReturn(Optional.empty());

        DeviceCommand command = service.create("dev-1", fields("NEW"));

        assertThat(command.operation()).isEqualTo(Operation.CREATE);
        verify(publisher).publish(command);
    }

    @Test
    void givenDeviceExists_whenCreate_thenAlreadyExistsAndNothingPublished() {
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(device("dev-1")));

        assertThatThrownBy(() -> service.create("dev-1", fields("NEW")))
                .isInstanceOf(DeviceAlreadyExistsException.class);

        verify(publisher, never()).publish(any());
        verify(metrics).commandRejected(Operation.CREATE, "DEVICE_ALREADY_EXISTS");
    }

    @Test
    void givenDeviceExists_whenDelete_thenDeleteCommandPublishedWithoutFields() {
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(device("dev-1")));

        DeviceCommand returned = service.delete("dev-1");

        ArgumentCaptor<DeviceCommand> captor = ArgumentCaptor.forClass(DeviceCommand.class);
        verify(publisher).publish(captor.capture());
        assertThat(captor.getValue()).isEqualTo(returned);
        assertThat(returned.operation()).isEqualTo(Operation.DELETE);
        assertThat(returned.deviceId()).isEqualTo("dev-1");
        assertThat(returned.fields()).isNull();
        verify(metrics).commandAccepted(Operation.DELETE);
    }

    @Test
    void givenNoDevice_whenDelete_thenNotFoundAndNothingPublished() {
        when(readRepository.findById("dev-1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.delete("dev-1")).isInstanceOf(DeviceNotFoundException.class);

        verify(publisher, never()).publish(any());
        verify(metrics).commandRejected(Operation.DELETE, "DEVICE_NOT_FOUND");
    }

    @Test
    void givenDeletedDevice_whenDelete_thenNotFoundAndNothingPublished() {
        // Удалённое устройство в модели чтения не видно: повторное удаление - 404, как отсутствующее
        when(readRepository.findById("dev-1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.delete("dev-1")).isInstanceOf(DeviceNotFoundException.class);

        verify(publisher, never()).publish(any());
    }

    static Device device(String id) {
        return new Device(id, "name", "t1", "owner", "sensor", "SN-1", "ACME", "T100", "ACTIVE",
                Map.of("mode", "eco"), Map.of("zone", "A"), "1.0.0", "Linux", "6.1", List.of(),
                NOW, NOW, 1L);
    }

    static DeviceCommand.Fields fields(String status) {
        return new DeviceCommand.Fields("name", "t1", "owner", "sensor", "SN-1", "ACME", "T100", status,
                Map.of("mode", "eco"), Map.of("zone", "A"));
    }
}
