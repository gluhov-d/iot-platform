import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

plugins {
    `java-library`
    `maven-publish`
    id("org.openapi.generator") version "7.17.0"
}

group = "com.github.gluhov.iotplatform"
// Версия библиотеки = info.version контракта; расхождение проверяет задача verifyContractVersion (сборка падает)
version = "1.0.0"
description = "device-service-client: DevicesApi и DTO из OpenAPI-контракта device-service"

val springBootVersion = "4.0.3"
// Адрес Nexus - свойство nexusUrl из gradle.properties в корне (переопределяется -PnexusUrl)
val nexusUrl = providers.gradleProperty("nexusUrl")
val contractFile = layout.projectDirectory.file("src/main/openapi/device-service.yaml")
val contractPath = contractFile.asFile.absolutePath
val modelsDir = layout.buildDirectory.dir("generated/models")
val apiDir = layout.buildDirectory.dir("generated/api")

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
    withSourcesJar()
}

repositories {
    mavenCentral()
}

dependencies {
    // Версии зависимостей задаёт BOM Spring Boot; плагин dependency-management здесь не используется:
    // он не проставляет версии в POM публикации, и публикация падает с «dependencies without a version»
    api(platform("org.springframework.boot:spring-boot-dependencies:$springBootVersion"))
    // DevicesApi (@HttpExchange) и HTTP Service Client; @DateTimeFormat - из spring-context
    api("org.springframework:spring-web")
    api("org.springframework:spring-context")
    // Аннотации, которые генератор ставит в DTO и интерфейсу: @Schema (swagger) и @Generated (jakarta)
    api("io.swagger.core.v3:swagger-annotations-jakarta:2.2.30")
    api("jakarta.annotation:jakarta.annotation-api")
    // Проверки полей в DTO и параметрах интерфейса
    api("jakarta.validation:jakarta.validation-api")
    // Jackson-аннотации в DTO
    api("com.fasterxml.jackson.core:jackson-annotations")
}

// ---------------------------------------------------------------- Контракт
// Два прогона генератора 7.17.0: DTO с Bean Validation и отдельно интерфейс без моделей.
// Один прогон с library=spring-http-interface отключает useBeanValidation, поэтому прогонов два.
val generateModels by tasks.registering(GenerateTask::class) {
    group = "build"
    description = "DTO из контракта device-service (spring, Bean Validation, Jakarta EE)"
    generatorName.set("spring")
    inputSpec.set(contractPath)
    outputDir.set(modelsDir.map { it.asFile.absolutePath })
    apiPackage.set("com.github.gluhov.iotplatform.deviceservice.api")
    modelPackage.set("com.github.gluhov.iotplatform.deviceservice.api.model")
    // Глобальное свойство models: генерируются только модели (значение пусто - «все»)
    globalProperties.set(mapOf("models" to ""))
    generateApiTests.set(false)
    generateApiDocumentation.set(false)
    generateModelTests.set(false)
    generateModelDocumentation.set(false)
    configOptions.set(
        mapOf(
            "useBeanValidation" to "true",
            "useJakartaEe" to "true",
            "openApiNullable" to "false",
        )
    )
}

val generateApi by tasks.registering(GenerateTask::class) {
    group = "build"
    description = "Интерфейс DevicesApi из контракта device-service (spring-http-interface, без моделей)"
    generatorName.set("spring")
    inputSpec.set(contractPath)
    outputDir.set(apiDir.map { it.asFile.absolutePath })
    apiPackage.set("com.github.gluhov.iotplatform.deviceservice.api")
    modelPackage.set("com.github.gluhov.iotplatform.deviceservice.api.model")
    // Глобальное свойство apis: генерируется только интерфейс, модели - из первого прогона
    globalProperties.set(mapOf("apis" to ""))
    generateApiTests.set(false)
    generateApiDocumentation.set(false)
    configOptions.set(
        mapOf(
            "library" to "spring-http-interface",
            "useJakartaEe" to "true",
            // Шаблон интерфейса ставит @Parameter без импорта (7.17.0): без аннотаций swagger в интерфейсе
            "annotationLibrary" to "none",
            "documentationProvider" to "none",
        )
    )
}

sourceSets {
    main {
        java {
            srcDir(modelsDir.map { it.dir("src/main/java") })
            srcDir(apiDir.map { it.dir("src/main/java") })
        }
    }
}

// Версия проекта должна совпадать с info.version контракта. Проверка в задаче, а не при конфигурации:
// Dockerfile других подпроектов копирует только build.gradle.kts, и чтение контракта при конфигурации там не нужно
val verifyContractVersion by tasks.registering {
    group = "verification"
    description = "Версия проекта совпадает с info.version контракта device-service"
    inputs.file(contractFile)
    val projectVersion = project.version.toString()
    doLast {
        val text = contractFile.asFile.readText()
        // info.version - единственный ключ с отступом в две позиции (остальные вложены глубже)
        val contractVersion = Regex("(?m)^  version:\\s*['\"]?([^'\"\\s]+)").find(text)?.groupValues?.get(1)
            ?: throw GradleException("В ${contractFile.asFile.name} не найден info.version")
        if (contractVersion != projectVersion) {
            throw GradleException(
                "Версия библиотеки $projectVersion расходится с info.version контракта $contractVersion: " +
                    "поправьте version в build.gradle.kts или контракт"
            )
        }
    }
}

tasks.compileJava {
    dependsOn(generateModels, generateApi, verifyContractVersion)
}

tasks.named("sourcesJar") {
    dependsOn(generateModels, generateApi)
}

// ---------------------------------------------------------------- Nexus
// Учётная запись приходит снаружи: ORG_GRADLE_PROJECT_nexusUsername / ORG_GRADLE_PROJECT_nexusPassword
publishing {
    publications {
        create<MavenPublication>("deviceServiceClient") {
            artifactId = "device-service-client"
            from(components["java"])
            pom {
                name.set("device-service-client")
                description.set(project.description)
            }
        }
    }
    repositories {
        maven {
            // Имя nexus -> Gradle ищет свойства nexusUsername и nexusPassword
            name = "nexus"
            val repository = if (version.toString().endsWith("SNAPSHOT")) "maven-snapshots" else "maven-releases"
            url = uri("${nexusUrl.get()}/repository/$repository/")
            // Локальный Nexus работает по HTTP
            isAllowInsecureProtocol = true
            credentials(PasswordCredentials::class)
        }
    }
}

// Выпущенную версию maven-releases перезаписать нельзя: публикация проверяет наличие POM версии до загрузки
val checkVersionNotReleased by tasks.registering {
    group = "verification"
    description = "Версия клиента ещё не выпущена в maven-releases (выпущенную версию не перезаписать)"
    outputs.upToDateWhen { false }
    val releaseVersion = version.toString()
    val base = nexusUrl.get()
    doLast {
        if (releaseVersion.endsWith("-SNAPSHOT")) return@doLast
        val pom = "$base/repository/maven-releases/com/github/gluhov/iotplatform/device-service-client/" +
            "$releaseVersion/device-service-client-$releaseVersion.pom"
        val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
        val request = HttpRequest.newBuilder(URI.create(pom)).timeout(Duration.ofSeconds(10))
            .method("HEAD", HttpRequest.BodyPublishers.noBody()).build()
        val status = try {
            client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode()
        } catch (e: IOException) {
            throw GradleException("Nexus недоступен ($base): $e. Поднимите его: make nexus-up", e)
        }
        when (status) {
            200 -> throw GradleException(
                "Версия $releaseVersion уже опубликована: выпущенную версию не перезаписать, поднимите version " +
                    "в services/device-service-client/build.gradle.kts"
            )
            404 -> {}
            else -> throw GradleException("$pom: HTTP $status")
        }
    }
}

tasks.withType<PublishToMavenRepository>().configureEach {
    dependsOn(checkVersionNotReleased)
}
