package com.github.gluhov.iotplatform.deviceservice.domain.service;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CacheUnavailableException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceNotFoundException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.ReadModelUnavailableException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.CacheResult;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceCache;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceReadRepository;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceServiceMetrics;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeviceQueryServiceTest {

    private final DeviceReadRepository readRepository = mock(DeviceReadRepository.class);
    private final DeviceCache cache = mock(DeviceCache.class);
    private final DeviceServiceMetrics metrics = mock(DeviceServiceMetrics.class);
    private final DeviceQueryService service = new DeviceQueryService(readRepository, cache, metrics);

    @Test
    void givenCachedDevice_whenGet_thenHitWithoutReadModel() {
        Device cached = device("dev-1", 3);
        when(cache.find("dev-1")).thenReturn(Optional.of(cached));

        Device result = service.getById("dev-1");

        assertThat(result).isEqualTo(cached);
        verify(readRepository, never()).findById(anyString());
        verify(metrics).cacheResult(CacheResult.HIT);
    }

    @Test
    void givenMiss_whenGet_thenReadFromReadModelAndCached() {
        Device stored = device("dev-1", 3);
        when(cache.find("dev-1")).thenReturn(Optional.empty());
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(stored));
        when(cache.putIfFresh(stored)).thenReturn(true);

        Device result = service.getById("dev-1");

        assertThat(result).isEqualTo(stored);
        verify(cache).putIfFresh(stored);
        verify(metrics).cacheResult(CacheResult.MISS);
    }

    @Test
    void givenReplicaVersionBelowFloor_whenGet_thenReturnedButNotCached() {
        Device stale = device("dev-1", 2);
        when(cache.find("dev-1")).thenReturn(Optional.empty());
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(stale));
        when(cache.putIfFresh(stale)).thenReturn(false);

        Device result = service.getById("dev-1");

        assertThat(result).isEqualTo(stale);
        verify(metrics).cacheResult(CacheResult.STALE_REPLICA);
    }

    @Test
    void givenCacheReadError_whenGet_thenReadFromReadModelWithoutCaching() {
        Device stored = device("dev-1", 3);
        when(cache.find("dev-1")).thenThrow(new CacheUnavailableException("redis down", null));
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(stored));

        Device result = service.getById("dev-1");

        assertThat(result).isEqualTo(stored);
        verify(cache, never()).putIfFresh(any());
        verify(metrics).cacheResult(CacheResult.ERROR);
    }

    @Test
    void givenCacheWriteError_whenGet_thenDeviceReturned() {
        Device stored = device("dev-1", 3);
        when(cache.find("dev-1")).thenReturn(Optional.empty());
        when(readRepository.findById("dev-1")).thenReturn(Optional.of(stored));
        when(cache.putIfFresh(stored)).thenThrow(new CacheUnavailableException("redis down", null));

        Device result = service.getById("dev-1");

        assertThat(result).isEqualTo(stored);
        verify(metrics).cacheResult(CacheResult.ERROR);
    }

    @Test
    void givenNoRowDeletedOrWithoutCard_whenGet_thenNotFound() {
        // Правило «карточка есть» применяет модель чтения: для удалённого или без карточки она отдаёт пусто
        when(cache.find("dev-1")).thenReturn(Optional.empty());
        when(readRepository.findById("dev-1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getById("dev-1"))
                .isInstanceOf(DeviceNotFoundException.class);
        verify(cache, never()).putIfFresh(any());
        verify(metrics).cacheResult(CacheResult.MISS);
    }

    @Test
    void givenCacheErrorAndNoRow_whenGet_thenNotFound() {
        // Замечание ревью: при недоступном Redis отсутствующее (удалённое, без карточки) устройство - тот же 404
        when(cache.find("dev-1")).thenThrow(new CacheUnavailableException("redis down", null));
        when(readRepository.findById("dev-1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getById("dev-1"))
                .isInstanceOf(DeviceNotFoundException.class);
        verify(cache, never()).putIfFresh(any());
        verify(metrics).cacheResult(CacheResult.ERROR);
    }

    @Test
    void givenCacheErrorAndReadModelUnavailable_whenGet_thenServiceUnavailablePropagates() {
        // И при недоступном Redis отказ модели чтения остаётся 503, а не 200
        when(cache.find("dev-1")).thenThrow(new CacheUnavailableException("redis down", null));
        when(readRepository.findById("dev-1")).thenThrow(new ReadModelUnavailableException("proxy down", null));

        assertThatThrownBy(() -> service.getById("dev-1"))
                .isInstanceOf(ReadModelUnavailableException.class);
    }

    @Test
    void givenReadModelUnavailable_whenGet_thenExceptionPropagates() {
        when(cache.find("dev-1")).thenReturn(Optional.empty());
        when(readRepository.findById("dev-1")).thenThrow(new ReadModelUnavailableException("proxy down", null));

        assertThatThrownBy(() -> service.getById("dev-1"))
                .isInstanceOf(ReadModelUnavailableException.class);
    }

    private static Device device(String id, long version) {
        return new Device(id, "name", "t1", "o1", "sensor", "SN-1", "acme", "m1", "ACTIVE",
                Map.of(), Map.of(), "1.0", "linux", "6", List.of(),
                Instant.parse("2026-01-01T00:00:00Z"), null, version);
    }
}
