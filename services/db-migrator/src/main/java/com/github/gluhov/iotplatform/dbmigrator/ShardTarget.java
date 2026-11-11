package com.github.gluhov.iotplatform.dbmigrator;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Шард, к которому применяются миграции. Пароль не попадает в toString(): объект пишется в журнал.
 */
public record ShardTarget(String name, String url, String user, String password) {

    /**
     * Цели из окружения:
     * <pre>
     * DB_SHARDS=shard_0,shard_1                                    имена шардов
     * DB_SHARD_0_URL=jdbc:postgresql://postgres-shard-0:5432/shard_0  адрес каждого (DB_&lt;ИМЯ&gt;_URL)
     * DB_USER, DB_PASSWORD                                         учётная запись, одна на все шарды
     * </pre>
     */
    public static List<ShardTarget> fromEnvironment(Map<String, String> env) {
        String shards = require(env, "DB_SHARDS");
        String user = require(env, "DB_USER");
        String password = require(env, "DB_PASSWORD");
        List<ShardTarget> targets = new ArrayList<>();
        for (String raw : shards.split(",")) {
            String name = raw.strip();
            if (name.isEmpty()) {
                continue;
            }
            String urlVariable = "DB_" + name.toUpperCase(Locale.ROOT) + "_URL";
            targets.add(new ShardTarget(name, require(env, urlVariable), user, password));
        }
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("DB_SHARDS is empty");
        }
        return List.copyOf(targets);
    }

    private static String require(Map<String, String> env, String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is not set");
        }
        return value;
    }

    @Override
    public String toString() {
        return name + " (" + url + ")";
    }
}