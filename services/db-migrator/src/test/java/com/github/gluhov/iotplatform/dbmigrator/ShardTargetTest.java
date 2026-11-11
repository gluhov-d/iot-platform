package com.github.gluhov.iotplatform.dbmigrator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardTargetTest {

    @Test
    @DisplayName("Shards from environment then one target per name with shared credentials")
    void givenEnvironment_whenFromEnvironment_thenTargetsInOrder() {
        Map<String, String> env = Map.of(
                "DB_SHARDS", "shard_0, shard_1",
                "DB_SHARD_0_URL", "jdbc:postgresql://pg0:5432/shard_0",
                "DB_SHARD_1_URL", "jdbc:postgresql://pg1:5432/shard_1",
                "DB_USER", "devices",
                "DB_PASSWORD", "p'a\"s#s");

        List<ShardTarget> targets = ShardTarget.fromEnvironment(env);

        assertThat(targets).extracting(ShardTarget::name).containsExactly("shard_0", "shard_1");
        assertThat(targets.get(1).url()).isEqualTo("jdbc:postgresql://pg1:5432/shard_1");
        assertThat(targets.getFirst().password()).isEqualTo("p'a\"s#s");
        assertThat(targets.getFirst().toString()).doesNotContain("p'a");
    }

    @Test
    @DisplayName("Shard without URL then error names the missing variable")
    void givenMissingUrl_whenFromEnvironment_thenError() {
        Map<String, String> env = Map.of("DB_SHARDS", "shard_0", "DB_USER", "u", "DB_PASSWORD", "p");

        assertThatThrownBy(() -> ShardTarget.fromEnvironment(env))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("DB_SHARD_0_URL is not set");
    }
}