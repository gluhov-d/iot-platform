plugins {
    java
    jacoco
    id("org.springframework.boot") version "4.0.3"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.github.gluhov.iotplatform"
version = "0.1.0"
description = "device-collector-service"

val confluentVersion = "8.2.4"
// Версия Avro-схем (артефакт avros в Nexus); Avro приходит вместе с ними транзитивно
val avrosVersion = "1.2.0"
// Flyway в тестах (через db-migrator) - той же версии, что и в самом db-migrator, а не из BOM Spring Boot
extra["flyway.version"] = "12.11.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    // Контракты платформы - только из Nexus (адрес - свойство nexusUrl, см. gradle.properties в корне)
    exclusiveContent {
        forRepository {
            maven {
                name = "nexus"
                url = uri("${providers.gradleProperty("nexusUrl").get()}/repository/maven-public/")
                // Локальный Nexus работает по HTTP
                isAllowInsecureProtocol = true
            }
        }
        filter { includeGroup("com.github.gluhov.iotplatform") }
    }
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    // Чтение устройства по ключу (GET /api/v1/devices/{deviceId}) и /actuator/*
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-opentelemetry")
    implementation("io.micrometer:micrometer-registry-prometheus")
    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")

    implementation("com.github.gluhov.iotplatform:avros:$avrosVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    // ShardingSphere-Proxy говорит по протоколу PostgreSQL: обычный драйвер, без библиотек ShardingSphere
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.awaitility:awaitility")
    testImplementation("com.tngtech.archunit:archunit-junit5:1.5.1")
    // Схему шардов в интеграционных тестах готовит тот же db-migrator, что и на стенде.
    // Его slf4j-simple (журнал отдельного контейнера) здесь не нужен: второй провайдер SLF4J вытеснил бы Logback
    testImplementation(project(":services:db-migrator")) {
        exclude(group = "org.slf4j", module = "slf4j-simple")
    }
    testCompileOnly("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// ---------------------------------------------------------------- Тесты
// *Test — модульные (без Docker), It*Test — интеграционные (Testcontainers).
tasks.test {
    useJUnitPlatform()
    exclude("**/It*Test.class")
}

val integrationTest by tasks.registering(Test::class) {
    group = "verification"
    description = "Интеграционные тесты: Kafka, Schema Registry, ShardingSphere-Proxy и два шарда PostgreSQL в Testcontainers"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform()
    include("**/It*Test.class")
    shouldRunAfter(tasks.test)
}

tasks.withType<Test>().configureEach {
    testLogging { events("passed", "skipped", "failed") }
}

// ---------------------------------------------------------------- Покрытие
jacoco { toolVersion = "0.8.14" }

// Точка входа не является логикой сервиса; Avro-классы приходят из avros и в отчёт не попадают
val coverageExcludes = listOf(
    "com/github/gluhov/iotplatform/devicecollector/DeviceCollectorApplication*",
)

fun JacocoReportBase.configureCoverageInputs() {
    executionData.setFrom(fileTree(layout.buildDirectory) { include("jacoco/*.exec") })
    sourceDirectories.setFrom(files("src/main/java"))
    classDirectories.setFrom(sourceSets["main"].output.classesDirs.asFileTree.matching {
        exclude(coverageExcludes)
    })
}

tasks.jacocoTestReport {
    dependsOn(tasks.test, integrationTest)
    configureCoverageInputs()
    reports {
        xml.required = true
        html.required = true
    }
}

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.test, integrationTest)
    configureCoverageInputs()
    violationRules {
        rule {
            limit {
                counter = "LINE"
                value = "COVEREDRATIO"
                minimum = "0.70".toBigDecimal()
            }
        }
    }
}

tasks.check {
    dependsOn(integrationTest, tasks.jacocoTestCoverageVerification)
    finalizedBy(tasks.jacocoTestReport)
}

tasks.bootJar { archiveFileName = "device-collector-service.jar" }
tasks.jar { enabled = false }

// ---------------------------------------------------------------- Быстрая проверка стенда
// Нужен поднятый стенд (make up), поэтому задача не входит в check и build
val quickCheck by tasks.registering(Exec::class) {
    group = "verification"
    description = "Быстрая проверка поднятого локального стенда (scripts/quick-check.sh)"
    commandLine(layout.projectDirectory.file("scripts/quick-check.sh").asFile.absolutePath)
}