plugins {
    java
    jacoco
    id("org.springframework.boot") version "4.0.3"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.github.gluhov.iotplatform"
version = "0.1.0"
description = "device-service: управление устройствами - REST API, команды в Kafka, чтение из модели чтения"

val confluentVersion = "8.2.4"
val mapstructVersion = "1.6.3"
val avrosVersion = "1.3.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    // Контракты платформы (avros) - только из Nexus (адрес - свойство nexusUrl, см. gradle.properties в корне)
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
    // DTO и интерфейс DevicesApi - из контракта, в Git не попадают (services/device-service-client)
    implementation(project(":services:device-service-client"))

    // Вход: REST API (webmvc), проверка сервисных токенов Keycloak, проверки полей
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    // Выход: команды в Kafka, кэш в Redis через Sentinel, чтение из реплик через ShardingSphere-Proxy
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    // Наблюдаемость: /actuator/*, трассировка, метрики Prometheus
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-opentelemetry")
    implementation("io.micrometer:micrometer-registry-prometheus")

    // MapStruct: маппер Device -> DeviceResponse (незаполненное поле - ошибка сборки)
    implementation("org.mapstruct:mapstruct:$mapstructVersion")
    annotationProcessor("org.mapstruct:mapstruct-processor:$mapstructVersion")

    // ShardingSphere-Proxy говорит по протоколу PostgreSQL: обычный драйвер, без библиотек ShardingSphere
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    // Срезы MVC (@WebMvcTest) в Spring Boot 4 вынесены в отдельный модуль
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.awaitility:awaitility")
    testImplementation("com.tngtech.archunit:archunit-junit5:1.5.1")
    // Схему шардов в интеграционных тестах готовит тот же db-migrator, что и на стенде.
    // Его slf4j-simple здесь не нужен: второй провайдер SLF4J вытеснил бы Logback
    testImplementation(project(":services:db-migrator")) {
        exclude(group = "org.slf4j", module = "slf4j-simple")
    }
    // Сообщение DeviceInfo 1.3.0 (тема devices) собирается в продакшн-коде; схема регистрируется в Schema Registry
    // стенда задачей schema-register, в интеграционных тестах - AbstractItTest
    implementation("com.github.gluhov.iotplatform:avros:$avrosVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// ---------------------------------------------------------------- Тесты
// *Test - модульные (без Docker), It*Test - интеграционные (Testcontainers).
tasks.test {
    useJUnitPlatform()
    exclude("**/It*Test.class")
}

val integrationTest by tasks.registering(Test::class) {
    group = "verification"
    description = "Интеграционные тесты: Kafka, Schema Registry, Redis, PostgreSQL, Keycloak в Testcontainers"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    include("**/It*Test.class")
    // Сквозной тест на полном стенде (тег e2e) - только в задаче e2eTest
    useJUnitPlatform {
        excludeTags("e2e")
    }
    shouldRunAfter(tasks.test)
}

// Сквозной тест на полном стенде платформы (ItEndToEndTest): весь infrastructure/docker-compose.yaml через
// ComposeContainer. Нужны Docker и docker compose на хосте, образы стенда (make up). В check не входит
val e2eTest by tasks.registering(Test::class) {
    group = "verification"
    description = "Сквозной тест на полном стенде: PUT -> Kafka -> device-collector-service -> реплика -> GET за 5 с"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    include("**/It*Test.class")
    useJUnitPlatform {
        includeTags("e2e")
    }
    shouldRunAfter(tasks.test, integrationTest)
}

tasks.withType<Test>().configureEach {
    testLogging { events("passed", "skipped", "failed") }
}

// ---------------------------------------------------------------- Покрытие
jacoco { toolVersion = "0.8.14" }

// Точка входа не является логикой сервиса
val coverageExcludes = listOf(
    "com/github/gluhov/iotplatform/deviceservice/DeviceServiceApplication*",
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

tasks.bootJar { archiveFileName = "device-service.jar" }
tasks.jar { enabled = false }
