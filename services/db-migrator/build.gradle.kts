plugins {
    java
    application
}

group = "com.github.gluhov.iotplatform"
version = "0.1.0"
description = "db-migrator: миграции Flyway на каждый шард PostgreSQL реестра устройств"

val flywayVersion = "12.11.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Интеграционные тесты device-collector-service готовят схему шардов этим же модулем (DbMigrator.migrateAll)
    implementation("org.flywaydb:flyway-core:$flywayVersion")
    runtimeOnly("org.flywaydb:flyway-database-postgresql:$flywayVersion")
    runtimeOnly("org.postgresql:postgresql:42.7.8")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")

    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.27.6")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "com.github.gluhov.iotplatform.dbmigrator.DbMigrator"
    applicationName = "db-migrator"
}

tasks.test {
    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }
}