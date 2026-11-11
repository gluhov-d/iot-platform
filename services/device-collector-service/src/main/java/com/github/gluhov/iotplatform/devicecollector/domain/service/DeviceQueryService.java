package com.github.gluhov.iotplatform.devicecollector.domain.service;

import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceView;
import com.github.gluhov.iotplatform.devicecollector.domain.port.in.FindDeviceUseCase;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class DeviceQueryService implements FindDeviceUseCase {
    private final DeviceRepository repository;

    @Override
    public Optional<DeviceView> find(String deviceId) {
        return repository.findById(deviceId);
    }
}