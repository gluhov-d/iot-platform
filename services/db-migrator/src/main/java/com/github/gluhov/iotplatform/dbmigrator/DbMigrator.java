package com.github.gluhov.iotplatform.dbmigrator;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.output.MigrateResult;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Одноразовая задача стенда: одни и те же миграции db/migration/devices применяются к каждому шарду напрямую
 * (не через ShardingSphere-Proxy: DDL должен попасть на все узлы, а Proxy маршрутизирует по ключу шардирования),
 * затем проверяется, что версии схемы на шардах совпадают. Код выхода 0 - схема одинакова и актуальна на всех
 * шардах; иначе 1, и стенд дальше не поднимается (Proxy и сервис ждут успешного завершения в depends_on).
 */
public final class DbMigrator {
    public static final String LOCATION = "classpath:db/migration/devices";

    private DbMigrator() {
    }

    public static void main(String[] args) {
        try {
            Map<String, String> versions = migrateAll(ShardTarget.fromEnvironment(System.getenv()));
            System.out.println("==> db-migrator: schema version " + versions.values().iterator().next()
                    + " on all shards " + versions.keySet());
        } catch (RuntimeException e) {
            System.err.println("!!! db-migrator failed: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Применяет миграции ко всем шардам и сверяет версии. Используется и в интеграционных тестах сервисов.
     *
     * @return версия схемы по имени шарда
     */
    public static Map<String, String> migrateAll(List<ShardTarget> targets) {
        Map<String, String> versions = new LinkedHashMap<>();
        for (ShardTarget target : targets) {
            versions.put(target.name(), migrate(target));
        }
        if (versions.values().stream().distinct().count() != 1) {
            throw new IllegalStateException("schema versions differ between shards: " + versions);
        }
        return versions;
    }

    static String migrate(ShardTarget target) {
        Flyway flyway = Flyway.configure()
                .dataSource(target.url(), target.user(), target.password())
                .locations(LOCATION)
                // Шард мог только что стартовать: повторяем подключение до ~20 с, а не падаем сразу.
                // Неверный пароль Flyway тоже повторяет - отказ придёт через то же время
                .connectRetries(10)
                .connectRetriesInterval(2)
                .load();
        System.out.println("==> " + target + ": migrate");
        MigrateResult result = flyway.migrate();
        System.out.println("    applied " + result.migrationsExecuted + " migration(s)");
        // validate: применённые миграции совпадают с файлами (никто не правил V*.sql задним числом)
        flyway.validate();
        MigrationInfo current = flyway.info().current();
        if (current == null) {
            throw new IllegalStateException(target.name() + ": no migrations applied");
        }
        return current.getVersion().getVersion();
    }
}