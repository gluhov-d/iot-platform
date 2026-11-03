import org.apache.avro.Schema
import org.apache.avro.SchemaCompatibility
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.SchemaParser
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.generic.GenericData
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.zip.ZipInputStream

buildscript {
    repositories { mavenCentral() }
    dependencies {
        // Версия совпадает с Avro, которую тянет kafka-avro-serializer 8.2.x
        classpath("org.apache.avro:avro-compiler:1.12.1")
    }
}

plugins {
    `java-library`
    `maven-publish`
}

// Версия контрактов: несовместимое изменение схем - новая старшая версия.
// Выпущенную версию maven-releases перезаписать не даст.
group = "com.github.gluhov.iotplatform"
version = "1.0.0"
description = "Avro-контракты IoT Platform: схемы и сгенерированные классы SpecificRecord"

val avroVersion = "1.12.1"

// Имя jar совпадает с artifactId публикации
base.archivesName = "contracts-avro"

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
    // api: сгенерированные классы наследуют SpecificRecordBase, Avro нужен потребителю при компиляции
    api("org.apache.avro:avro:$avroVersion")
}

// ---------------------------------------------------------------- Avro
val avroSchemasDir = layout.projectDirectory.dir("avro")
val avroOutputDir = layout.buildDirectory.dir("generated-sources/avro")

val generateAvro by tasks.registering {
    group = "build"
    description = "Генерирует Java-классы SpecificRecord из contracts/avro/*.avsc"
    inputs.dir(avroSchemasDir)
    outputs.dir(avroOutputDir)
    doLast {
        val out = avroOutputDir.get().asFile
        out.deleteRecursively()
        val files = avroSchemasDir.asFile.listFiles { f -> f.extension == "avsc" }!!.sorted()
        // Один SchemaParser на все файлы: схема может ссылаться на тип из другого файла по имени,
        // порядок файлов не важен. Повторное объявление одного полного имени - ошибка сборки.
        val parser = SchemaParser()
        val parsed = files.map { it to parser.parse(it) }
        parsed.forEach { (file, result) ->
            val compiler = SpecificCompiler(parser.resolve(result))
            // String, а не CharSequence: иначе при чтении приходит Utf8 и equals ломается
            compiler.setStringType(GenericData.StringType.String)
            compiler.compileToDestination(file, out)
        }
    }
}

sourceSets["main"].java.srcDir(avroOutputDir)
tasks.compileJava { dependsOn(generateAvro) }
tasks.named("sourcesJar") { dependsOn(generateAvro) }

// Сами схемы - в jar рядом с классами (avro/*.avsc): по ним проверяется совместимость новой версии
tasks.processResources {
    from(avroSchemasDir) {
        include("*.avsc")
        into("avro")
    }
}

// ---------------------------------------------------------------- Nexus
// Адрес Nexus и учётная запись приходят снаружи (конвейер контрактов), в репозитории их нет:
//   nexusUrl - gradle.properties в корне (переопределяется -PnexusUrl)
//   ORG_GRADLE_PROJECT_nexusUsername / ORG_GRADLE_PROJECT_nexusPassword
val nexusUrl = providers.gradleProperty("nexusUrl")

// ---------------------------------------------------------------- Совместимость
// Новая версия сверяется с последней выпущенной в maven-releases. Уровень - -PcompatibilityLevel:
//   FULL (по умолчанию) - новая схема читает старые данные, а старая - новые. Сервисы переходят
//                         на новую версию контрактов независимо, поэтому нужны оба направления;
//   BACKWARD            - только новая читает старые (режим Schema Registry);
//   FORWARD             - только старая читает новые.
val compatibilityLevel = providers.gradleProperty("compatibilityLevel").orElse("FULL")
val artifactPath = "${project.group.toString().replace('.', '/')}/contracts-avro"

data class SemVer(val major: Int, val minor: Int, val patch: Int, val snapshot: Boolean) : Comparable<SemVer> {
    override fun compareTo(other: SemVer) = compareValuesBy(this, other, { it.major }, { it.minor }, { it.patch })
    override fun toString() = "$major.$minor.$patch" + if (snapshot) "-SNAPSHOT" else ""
}

fun parseSemVer(text: String): SemVer? =
    Regex("""(\d+)\.(\d+)\.(\d+)(-SNAPSHOT)?""").matchEntire(text)?.destructured?.let { (major, minor, patch, snapshot) ->
        SemVer(major.toInt(), minor.toInt(), patch.toInt(), snapshot.isNotEmpty())
    }

// Все схемы одним парсером, как при генерации классов; ключ - полное имя главной записи файла
fun parseSchemas(texts: Map<String, String>): Map<String, Schema> {
    val parser = SchemaParser()
    val results = texts.map { (file, text) ->
        try {
            parser.parse(text)
        } catch (e: Exception) {
            throw GradleException("$file: ${e.message}", e)
        }
    }
    return results.map { parser.resolve(it) }.associateBy { it.fullName }
}

fun httpGet(client: HttpClient, url: String): HttpResponse<ByteArray> = try {
    val request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build()
    client.send(request, HttpResponse.BodyHandlers.ofByteArray())
} catch (e: IOException) {
    throw GradleException("Nexus недоступен ($url): $e. Поднимите его: make nexus-up", e)
}

val checkContractsCompatibility by tasks.registering {
    group = "verification"
    description = "Сверяет схемы contracts/avro с последней выпущенной версией contracts-avro в Nexus"
    // Результат зависит от содержимого Nexus, поэтому задача выполняется при каждом запуске
    outputs.upToDateWhen { false }
    val current = version.toString()
    doLast {
        val level = compatibilityLevel.get().uppercase()
        if (level !in setOf("FULL", "BACKWARD", "FORWARD")) {
            throw GradleException("compatibilityLevel = '$level': ожидается FULL, BACKWARD или FORWARD")
        }
        val currentVersion = parseSemVer(current)
            ?: throw GradleException("version = '$current': ожидается X.Y.Z или X.Y.Z-SNAPSHOT")

        val files = avroSchemasDir.asFile.listFiles { f -> f.extension == "avsc" }!!.sorted()
        val newSchemas = parseSchemas(files.associate { it.name to it.readText() })
        logger.lifecycle("Схемы разобраны: ${newSchemas.keys.sorted().joinToString()}")

        val client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build()
        val base = "${nexusUrl.get()}/repository/maven-releases/$artifactPath"

        val metadata = httpGet(client, "$base/maven-metadata.xml")
        val released = when (metadata.statusCode()) {
            200 -> Regex("<version>([^<]+)</version>").findAll(String(metadata.body()))
                .mapNotNull { parseSemVer(it.groupValues[1]) }
                .filter { !it.snapshot }
                .toList()
            404 -> emptyList()
            else -> throw GradleException("$base/maven-metadata.xml: HTTP ${metadata.statusCode()}")
        }
        val latest = released.maxOrNull()
        if (latest == null) {
            logger.lifecycle("Выпущенных версий contracts-avro нет: первая публикация, сравнивать не с чем")
            return@doLast
        }
        if (!currentVersion.snapshot && currentVersion in released) {
            throw GradleException(
                "Версия $current уже опубликована: выпущенную версию не перезаписать, " +
                    "поднимите version в contracts/build.gradle.kts"
            )
        }
        if (currentVersion <= latest) {
            throw GradleException("Версия $current должна быть больше последней выпущенной $latest")
        }

        val jar = httpGet(client, "$base/$latest/contracts-avro-$latest.jar")
        if (jar.statusCode() != 200) {
            throw GradleException("contracts-avro-$latest.jar: HTTP ${jar.statusCode()}")
        }
        val oldTexts = mutableMapOf<String, String>()
        ZipInputStream(jar.body().inputStream()).use { zip ->
            generateSequence { zip.nextEntry }
                .filter { it.name.startsWith("avro/") && it.name.endsWith(".avsc") }
                .forEach { oldTexts[it.name] = String(zip.readBytes()) }
        }
        if (oldTexts.isEmpty()) {
            throw GradleException("В contracts-avro-$latest.jar нет схем avro/*.avsc")
        }
        val oldSchemas = parseSchemas(oldTexts)

        val problems = mutableListOf<String>()
        oldSchemas.forEach { (name, old) ->
            val new = newSchemas[name]
            if (new == null) {
                problems += "$name: схема удалена"
                return@forEach
            }
            // BACKWARD: новая схема (reader) читает данные старой (writer); FORWARD - наоборот
            val checks = buildList {
                if (level != "FORWARD") add("BACKWARD" to SchemaCompatibility.checkReaderWriterCompatibility(new, old))
                if (level != "BACKWARD") add("FORWARD" to SchemaCompatibility.checkReaderWriterCompatibility(old, new))
            }
            checks.filter { it.second.type != SchemaCompatibilityType.COMPATIBLE }.forEach { (direction, result) ->
                result.result.incompatibilities.forEach {
                    problems += "$name [$direction] ${it.type} в ${it.location}: ${it.message}"
                }
            }
        }

        if (problems.isEmpty()) {
            logger.lifecycle("Совместимо с $latest ($level), проверено схем: ${oldSchemas.size}")
            return@doLast
        }
        val report = problems.joinToString("\n") { "  - $it" }
        if (currentVersion.major > latest.major) {
            logger.warn("Несовместимо с $latest ($level), но старшая версия поднята до $current - публикация разрешена:\n$report")
        } else {
            throw GradleException(
                "Схемы несовместимы с $latest ($level):\n$report\n" +
                    "Сделайте изменение совместимым (новые поля - с default) или поднимите старшую версию."
            )
        }
    }
}

// ---------------------------------------------------------------- Публикация
publishing {
    publications {
        create<MavenPublication>("contracts") {
            artifactId = "contracts-avro"
            from(components["java"])
            pom {
                name = "IoT Platform Avro contracts"
                description = project.description
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

// В Nexus попадает только версия, прошедшая проверку совместимости - даже при прямом вызове publish
tasks.withType<PublishToMavenRepository>().configureEach {
    dependsOn(checkContractsCompatibility)
}