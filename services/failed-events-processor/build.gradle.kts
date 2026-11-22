plugins {
    java
    jacoco
    id("org.springframework.boot") version "4.0.3"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.github.gluhov.iotplatform"
version = "0.1.0"
description = "failed-events-processor"

val confluentVersion = "8.2.4"
// Версия Avro-схем (артефакт avros в Nexus): FailedEvent берётся из неё
val avrosVersion = "1.4.0"
// Версия AWS SDK for Java v2: одна на весь модуль, через BOM
val awsSdkVersion = "2.59.7"
// Testcontainers-модуль MinIO (R19): версия задана явно
val testcontainersMinioVersion = "2.0.3"

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

dependencyManagement {
    imports {
        mavenBom("software.amazon.awssdk:bom:$awsSdkVersion")
    }
}

dependencies {
    // Веб-сервер нужен только для /actuator (health liveness для Compose, prometheus для Alloy); REST API у процессора нет
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-opentelemetry")
    implementation("org.springframework.boot:spring-boot-starter-json")
    implementation("io.micrometer:micrometer-registry-prometheus")

    implementation("com.github.gluhov.iotplatform:avros:$avrosVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    implementation("software.amazon.awssdk:s3")
    // HTTP-клиент SDK с тайм-аутом соединения из FAILED_EVENTS_S3_CONNECT_TIMEOUT (версия - из BOM)
    implementation("software.amazon.awssdk:apache-client")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.testcontainers:testcontainers-minio:$testcontainersMinioVersion")
    testImplementation("org.awaitility:awaitility")
    testImplementation("io.micrometer:micrometer-observation-test")
    // Спаны в памяти для проверки родителя спана s3.put-object (T039)
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing")
    testImplementation("com.tngtech.archunit:archunit-junit5:1.5.1")
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
    description = "Интеграционные тесты: Kafka, Schema Registry и MinIO в Testcontainers"
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
    "com/github/gluhov/iotplatform/failedeventsprocessor/FailedEventsProcessorApplication*",
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

tasks.bootJar { archiveFileName = "failed-events-processor.jar" }
tasks.jar { enabled = false }
