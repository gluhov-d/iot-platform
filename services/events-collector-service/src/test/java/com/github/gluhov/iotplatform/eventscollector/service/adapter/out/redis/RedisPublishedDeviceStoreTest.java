package com.github.gluhov.iotplatform.eventscollector.service.adapter.out.redis;

import com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisPublishedDeviceStore;
import com.github.gluhov.iotplatform.eventscollector.domain.model.PublicationMarkResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.OptionalLong;

import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService.LEASE_KEY;
import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisPublishedDeviceStore.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Сам Lua-скрипт на настоящем Redis проверяет ItRedisPublishedDeviceStoreTest: мок скрипт не выполняет
@ExtendWith(MockitoExtension.class)
class RedisPublishedDeviceStoreTest {
    private static final String TOKEN = "owner-token";
    private static final String DEVICE_ID = "dev-1";
    private static final List<String> KEYS = List.of(LEASE_KEY, PUBLISHED_KEY, SNAPSHOT_AT_KEY, FINGERPRINT_KEY);

    @Mock
    private StringRedisTemplate redisTemplate;
    @InjectMocks
    private RedisPublishedDeviceStore store;

    @Test
    @DisplayName("Marks with snapshot and fingerprint then one script call with the owner token and all four keys")
    void givenAllMarks_whenMarkPublication_thenOneScriptCall() {
        when(redisTemplate.execute(MARK_PUBLICATION_SCRIPT, KEYS, TOKEN, DEVICE_ID, "1", "1000", "fp")).thenReturn(1L);

        PublicationMarkResult result = store.markPublication(TOKEN, DEVICE_ID, true, OptionalLong.of(1_000L), "fp");

        assertThat(result).isEqualTo(PublicationMarkResult.WRITTEN);
        verify(redisTemplate).execute(MARK_PUBLICATION_SCRIPT, KEYS, TOKEN, DEVICE_ID, "1", "1000", "fp");
    }

    @Test
    @DisplayName("Only the published mark then empty snapshot time and fingerprint in the script arguments")
    void givenOnlyPublishedMark_whenMarkPublication_thenEmptyArguments() {
        when(redisTemplate.execute(MARK_PUBLICATION_SCRIPT, KEYS, TOKEN, DEVICE_ID, "1", "", "")).thenReturn(1L);

        assertThat(store.markPublication(TOKEN, DEVICE_ID, true, OptionalLong.empty(), null))
                .isEqualTo(PublicationMarkResult.WRITTEN);
    }

    @Test
    @DisplayName("Only the snapshot time then published flag 0 in the script arguments")
    void givenOnlySnapshotTime_whenMarkPublication_thenPublishedFlagZero() {
        when(redisTemplate.execute(MARK_PUBLICATION_SCRIPT, KEYS, TOKEN, DEVICE_ID, "0", "1000", "")).thenReturn(1L);

        assertThat(store.markPublication(TOKEN, DEVICE_ID, false, OptionalLong.of(1_000L), null))
                .isEqualTo(PublicationMarkResult.WRITTEN);
    }

    @Test
    @DisplayName("Script results -1 and 0 then LEASE_LOST and STALE")
    void givenScriptResults_whenMarkPublication_thenMapped() {
        when(redisTemplate.execute(MARK_PUBLICATION_SCRIPT, KEYS, TOKEN, DEVICE_ID, "1", "1000", "fp"))
                .thenReturn(-1L, 0L);

        assertThat(store.markPublication(TOKEN, DEVICE_ID, true, OptionalLong.of(1_000L), "fp"))
                .isEqualTo(PublicationMarkResult.LEASE_LOST);
        assertThat(store.markPublication(TOKEN, DEVICE_ID, true, OptionalLong.of(1_000L), "fp"))
                .isEqualTo(PublicationMarkResult.STALE);
    }

    @Test
    @DisplayName("Script returns nothing or an unknown code then error, not a silent success")
    void givenUnexpectedScriptResult_whenMarkPublication_thenError() {
        when(redisTemplate.execute(MARK_PUBLICATION_SCRIPT, KEYS, TOKEN, DEVICE_ID, "1", "", ""))
                .thenReturn(null, 7L);

        assertThatThrownBy(() -> store.markPublication(TOKEN, DEVICE_ID, true, OptionalLong.empty(), null))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> store.markPublication(TOKEN, DEVICE_ID, true, OptionalLong.empty(), null))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("7");
    }
}