import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.generic.GenericData

buildscript {
    repositories {mavenCentral()}
    dependencies {
        // Версия совпадает с Avro, которую тянет kafka-avro-serializer 8.2.x
        classpath("org.apache.avro:avro-compiler:1.12.1")
    }
}

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
val avroVersion = "1.12.1"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
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


    implementation("org.apache.avro:avro:$avroVersion")
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

// ---------------------------------------------------------------- Avro
// Классы SpecificRecord генерируются из общих контрактов contracts/avro -
// файлы схем копируются в сервис, источник правды один.
// rootDir - корень многомодульной сборки (корень репозитория)
val avroContractsDir = rootDir.resolve("contracts/avro")
val avroOutputDir = layout.buildDirectory.dir("generated-sources/avro")

val generateAvro by tasks.registering {
    group = "build"
    description = "Генерирует Java-классы SpecificRecord из contracts/avro/*.avsc"
    inputs.dir(avroContractsDir)
    outputs.dir(avroOutputDir)
    doLast {
        val out = avroOutputDir.get().asFile
        out.deleteRecursively()
        val parser = org.apache.avro.Schema.Parser()
        avroContractsDir.listFiles { f -> f.extension == "avsc" }!!.sorted().forEach { file ->
            val compiler = SpecificCompiler(parser.parse(file))
            // String, а не CharSequence: иначе при чтении приходит Utf8 и equals ломается
            compiler.setStringType(GenericData.StringType.String)
            compiler.compileToDestination(file, out)
        }
    }
}

sourceSets["main"].java.srcDir(avroOutputDir)
tasks.compileJava {dependsOn(generateAvro)}

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

// Сгенерированные Avro-классы и точка входа не являются логикой сервиса
val coverageExcludes = listOf(
    "com/github/gluhov/iotplatform/events/avro/**",
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
