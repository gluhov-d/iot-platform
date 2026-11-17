package com.github.gluhov.iotplatform.deviceservice.domain.service;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CacheUnavailableException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceNotFoundException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.CacheResult;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;
import com.github.gluhov.iotplatform.deviceservice.domain.port.in.GetDeviceUseCase;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceCache;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceReadRepository;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceServiceMetrics;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Чтение по Cache-Aside: кэш, при промахе - модель чтения, затем запись в кэш, если версия не ниже floor.
 * Недоступный кэш не ломает чтение: ответ дают из модели чтения (FR-021).
 */
@Service
public class DeviceQueryService implements GetDeviceUseCase {

    private final DeviceReadRepository readRepository;
    private final DeviceCache cache;
    private final DeviceServiceMetrics metrics;

    public DeviceQueryService(DeviceReadRepository readRepository, DeviceCache cache, DeviceServiceMetrics metrics) {
        this.readRepository = readRepository;
        this.cache = cache;
        this.metrics = metrics;
    }

    @Override
    public Device getById(String deviceId) {
        Optional<Device> cached;
        try {
            cached = cache.find(deviceId);
        } catch (CacheUnavailableException e) {
            metrics.cacheResult(CacheResult.ERROR);
            return readOrNotFound(deviceId);
        }
        if (cached.isPresent()) {
            metrics.cacheResult(CacheResult.HIT);
            return cached.get();
        }
        Optional<Device> stored = readRepository.findById(deviceId);
        if (stored.isEmpty()) {
            // Отсутствующее устройство в кэш не пишем
            metrics.cacheResult(CacheResult.MISS);
            throw new DeviceNotFoundException(deviceId);
        }
        Device device = stored.get();
        metrics.cacheResult(store(device));
        return device;
    }

    private Device readOrNotFound(String deviceId) {
        return readRepository.findById(deviceId).orElseThrow(() -> new DeviceNotFoundException(deviceId));
    }

    private CacheResult store(Device device) {
        try {
            return cache.putIfFresh(device) ? CacheResult.MISS : CacheResult.STALE_REPLICA;
        } catch (CacheUnavailableException e) {
            return CacheResult.ERROR;
        }
    }
}
