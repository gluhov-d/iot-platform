plugins {
    java
    jacoco
    id("org.springframework.boot") version "4.0.3"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.github.gluhov.iotplatform"
version = "0.1.0"
description = "events-collector-service"

val confluentVersion = "8.2.4"
// Версия Avro-контрактов (contracts-avro в Nexus); Avro приходит вместе с ними транзитивно
val contractsVersion = "1.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    // Контракты платформы - только из Nexus: группу com.github.gluhov.iotplatform Gradle не ищет
    // в других репозиториях, а остальные зависимости не ищет в Nexus.
    // Адрес - свойство nexusUrl (gradle.properties в корне, переопределяется -PnexusUrl)
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
    // Только встроенный сервер для /actuator/*
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.boot:spring-boot-starter-opentelemetry")
    implementation("io.micrometer:micrometer-registry-prometheus")
    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")

    implementation("com.github.gluhov.iotplatform:contracts-avro:$contractsVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    runtimeOnly("com.clickhouse:clickhouse-jdbc:0.9.9")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.testcontainers:testcontainers-clickhouse")
    testImplementation("org.awaitility:awaitility")
    testImplementation("com.tngtech.archunit:archunit-junit5:1.5.1")
    testCompileOnly("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// clickhouse-jdbc тянет at.yawk.lz4 (форк с исправлением CVE-2025-12183), kafka-clients -
// org.lz4:lz4-java 1.8.0. Оба объявляют одну capability; берем форк - API net.jpountz тот же.
configurations.configureEach {
    resolutionStrategy.capabilitiesResolution.withCapability("org.lz4:lz4-java") {
        selectHighestVersion()
    }
}

// ---------------------------------------------------------------- Тесты
// *Test — модульные (без Docker), It*Test — интеграционные (Testcontainers).
tasks.test {
    useJUnitPlatform()
    exclude("**/It*Test.class")
}

val integrationTest by tasks.registering(Test::class) {
    group = "verification"
    description = "Интеграционные тесты: Kafka, Schema Registry, ClickHouse и Redis в Testcontainers"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform()
    include("**/It*Test.class")
    shouldRunAfter(tasks.test)
}

tasks.withType<Test>().configureEach {
    testLogging { events("passed", "skipped", "failed")}
}

// ---------------------------------------------------------------- Покрытие
jacoco { toolVersion = "0.8.14" }

// Точка входа не является логикой сервиса; Avro-классы приходят из contracts-avro и в отчёт не попадают
val coverageExcludes = listOf(
    "com/github/gluhov/iotplatform/eventscollector/EventsCollectorApplication*",
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
                minimum = "0.65".toBigDecimal()
            }
        }
    }
}

tasks.check {
    dependsOn(integrationTest, tasks.jacocoTestCoverageVerification)
    finalizedBy(tasks.jacocoTestReport)
}

tasks.bootJar { archiveFileName = "events-collector-service.jar" }
tasks.jar { enabled = false }

// ---------------------------------------------------------------- Быстрая проверка стенда
// Нужен поднятый стенд (make up), поэтому задача не входит в check и build
// Exec без объявленных выходов Gradle выполняет при каждом запуске - кешировать нечего.
val checkLocal by tasks.registering(Exec::class) {
    group = "verification"
    description = "Быстрая проверка поднятого локального стенда (scripts/check-local.sh)"
    commandLine(layout.projectDirectory.file("scripts/check-local.sh").asFile.absolutePath)
}
