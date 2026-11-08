import org.apache.avro.Schema
import org.apache.avro.SchemaCompatibility
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.SchemaParser
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.generic.GenericData
import java.io.IOException
import java.net.URI
import java.net.URLClassLoader
import java.net.URLEncoder
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

// Версия схем: несовместимое изменение схем - новая старшая версия.
// Выпущенную версию maven-releases перезаписать не даст.
group = "com.github.gluhov.iotplatform"
version = "1.1.0"
description = "Avro-схемы IoT Platform: .avsc всех топиков и сгенерированные классы SpecificRecord"

val avroVersion = "1.12.1"

// Имя jar совпадает с artifactId публикации
base.archivesName = "avros"

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
    description = "Генерирует Java-классы SpecificRecord из avros/avro/*.avsc"
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
// Адрес Nexus и учётная запись приходят снаружи (конвейер avros), в репозитории их нет:
//   nexusUrl - gradle.properties в корне (переопределяется -PnexusUrl)
//   ORG_GRADLE_PROJECT_nexusUsername / ORG_GRADLE_PROJECT_nexusPassword
val nexusUrl = providers.gradleProperty("nexusUrl")

// ---------------------------------------------------------------- Совместимость
// Новая версия сверяется с последней выпущенной в maven-releases. Уровень - -PcompatibilityLevel:
//   FULL (по умолчанию) - новая схема читает старые данные, а старая - новые. Сервисы переходят
//                         на новую версию схем независимо, поэтому нужны оба направления;
//   BACKWARD            - только новая читает старые (режим Schema Registry);
//   FORWARD             - только старая читает новые.
val compatibilityLevel = providers.gradleProperty("compatibilityLevel").orElse("FULL")
val artifactPath = "${project.group.toString().replace('.', '/')}/avros"

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

val checkAvrosCompatibility by tasks.registering {
    group = "verification"
    description = "Сверяет схемы avros/avro с последней выпущенной версией avros в Nexus"
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
            logger.lifecycle("Выпущенных версий avros нет: первая публикация, сравнивать не с чем")
            return@doLast
        }
        if (!currentVersion.snapshot && currentVersion in released) {
            throw GradleException(
                "Версия $current уже опубликована: выпущенную версию не перезаписать, " +
                    "поднимите version в avros/build.gradle.kts"
            )
        }
        if (currentVersion <= latest) {
            throw GradleException("Версия $current должна быть больше последней выпущенной $latest")
        }

        val jar = httpGet(client, "$base/$latest/avros-$latest.jar")
        if (jar.statusCode() != 200) {
            throw GradleException("avros-$latest.jar: HTTP ${jar.statusCode()}")
        }
        val oldTexts = mutableMapOf<String, String>()
        ZipInputStream(jar.body().inputStream()).use { zip ->
            generateSequence { zip.nextEntry }
                .filter { it.name.startsWith("avro/") && it.name.endsWith(".avsc") }
                .forEach { oldTexts[it.name] = String(zip.readBytes()) }
        }
        if (oldTexts.isEmpty()) {
            throw GradleException("В avros-$latest.jar нет схем avro/*.avsc")
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

// ---------------------------------------------------------------- Schema Registry
// Регистрация схем в Schema Registry - одноразовая задача compose schema-register (после публикации в Nexus):
//   -PschemaRegistryUrl=http://schema-registry:8081
//   -PschemaSubjects=events-value=DeviceEvent,devices-value=DeviceInfo   (тема-value=простое имя записи)
//   -PschemaCompatibility=FULL_TRANSITIVE                                (по умолчанию)
// Продюсеры сервисов работают с auto.register.schemas=false и сами схем не регистрируют: сериализатор ищет в
// subject ту же схему, что лежит в SCHEMA$ класса, - со вложенными типами и свойствами генерации. Поэтому
// регистрируется SCHEMA$ самих классов (DeviceSnapshot внутри DeviceInfo), а не отдельные subject со ссылками.
fun jsonString(text: String): String = buildString {
    append('"')
    text.forEach { c ->
        when {
            c == '"' -> append("\\\"")
            c == '\\' -> append("\\\\")
            c < ' ' -> append("\\u%04x".format(c.code))
            else -> append(c)
        }
    }
    append('"')
}

val registerSchemas by tasks.registering {
    group = "publishing"
    description = "Регистрирует схемы avros/avro в Schema Registry (FULL_TRANSITIVE); повторный запуск ничего не меняет"
    dependsOn(tasks.classes)
    outputs.upToDateWhen { false }
    doLast {
        val registryUrl = providers.gradleProperty("schemaRegistryUrl").orNull?.trimEnd('/')
            ?: throw GradleException("Не задан -PschemaRegistryUrl")
        val level = providers.gradleProperty("schemaCompatibility").orElse("FULL_TRANSITIVE").get().uppercase()
        if (level !in setOf("FULL_TRANSITIVE", "FULL")) {
            throw GradleException("schemaCompatibility = '$level': ожидается FULL_TRANSITIVE или FULL")
        }
        val subjects = (providers.gradleProperty("schemaSubjects").orNull
            ?: throw GradleException("Не задан -PschemaSubjects (тема-value=ИмяЗаписи через запятую)"))
            .split(',').map { it.trim() }.filter { it.isNotEmpty() }
            .map { entry ->
                val (subject, record) = entry.split('=', limit = 2).takeIf { it.size == 2 }
                    ?: throw GradleException("schemaSubjects: '$entry' - ожидается тема-value=ИмяЗаписи")
                subject to record
            }
        if (subjects.isEmpty()) throw GradleException("schemaSubjects пуст")

        val files = avroSchemasDir.asFile.listFiles { f -> f.extension == "avsc" }!!.sorted()
        val schemas = parseSchemas(files.associate { it.name to it.readText() })

        val classesDir = layout.buildDirectory.dir("classes/java/main").get().asFile
        val classLoader = URLClassLoader(arrayOf(classesDir.toURI().toURL()), Schema::class.java.classLoader)

        val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
        fun call(method: String, path: String, body: String? = null): Pair<Int, String> {
            val publisher = if (body == null) HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofString(body)
            val request = HttpRequest.newBuilder(URI.create("$registryUrl$path"))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .method(method, publisher)
                .build()
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            return response.statusCode() to response.body()
        }

        // Schema Registry может быть healthy чуть раньше, чем примет запись: ждём до 60 с
        var ready = false
        for (attempt in 1..30) {
            try {
                if (call("GET", "/subjects").first == 200) { ready = true; break }
            } catch (_: IOException) {
            }
            Thread.sleep(2000)
        }
        if (!ready) throw GradleException("Schema Registry недоступен: $registryUrl")

        val failures = mutableListOf<String>()
        subjects.forEach { (subject, record) ->
            val fullName = schemas.values.singleOrNull { it.name == record }?.fullName
                ?: throw GradleException("$subject: в avros нет записи '$record' (есть: ${schemas.keys.sorted().joinToString()})")
            // Схема - SCHEMA$ сгенерированного класса, а не .avsc: в ней есть свойства генерации (avro.java.string),
            // и Schema Registry находит схему сериализатора только при полном совпадении
            val schema = classLoader.loadClass(fullName).getMethod("getClassSchema").invoke(null) as Schema
            val body = """{"schemaType":"AVRO","schema":${jsonString(schema.toString())}}"""
            val path = "/subjects/" + URLEncoder.encode(subject, Charsets.UTF_8)

            // 1. Режим совместимости subject - явно: не зависит от глобального значения Schema Registry
            val config = call("PUT", "/config/" + URLEncoder.encode(subject, Charsets.UTF_8), """{"compatibility":"$level"}""")
            if (config.first != 200) throw GradleException("$subject: PUT /config HTTP ${config.first}: ${config.second}")

            // 2. Эта схема уже в subject (байт в байт как её находит сериализатор) - ничего не делаем
            val found = call("POST", path, body)
            if (found.first == 200) {
                val id = Regex(""""id"\s*:\s*(\d+)""").find(found.second)?.groupValues?.get(1)
                val version = Regex(""""version"\s*:\s*(\d+)""").find(found.second)?.groupValues?.get(1)
                println("$subject: ${schema.fullName} уже зарегистрирована (id=$id, version=$version), $level")
                return@forEach
            }

            // 3. Новая схема: сначала проверка совместимости со ВСЕМИ зарегистрированными версиями
            //    (404 - subject ещё нет, сравнивать не с чем)
            val check = call("POST", "/compatibility/subjects/" + URLEncoder.encode(subject, Charsets.UTF_8) + "/versions?verbose=true", body)
            if (check.first == 200 && check.second.contains(""""is_compatible":false""")) {
                failures += "$subject: ${schema.fullName} несовместима с зарегистрированными версиями ($level): ${check.second}"
                return@forEach
            }
            if (check.first != 200 && check.first != 404) {
                throw GradleException("$subject: проверка совместимости HTTP ${check.first}: ${check.second}")
            }

            // 4. Регистрация; Schema Registry проверяет совместимость ещё раз сам
            val created = call("POST", "$path/versions", body)
            if (created.first == 409) {
                failures += "$subject: ${schema.fullName} отклонена Schema Registry (409): ${created.second}"
                return@forEach
            }
            if (created.first != 200) throw GradleException("$subject: регистрация HTTP ${created.first}: ${created.second}")
            val id = Regex(""""id"\s*:\s*(\d+)""").find(created.second)?.groupValues?.get(1)
            println("$subject: ${schema.fullName} зарегистрирована (id=$id), $level")
        }
        if (failures.isNotEmpty()) {
            throw GradleException("Схемы не зарегистрированы:\n" + failures.joinToString("\n") { "  - $it" } +
                    "\nСделайте изменение совместимым (новые поля - с default) или поднимите старшую версию avros с новыми subject.")
        }
    }
}

// ---------------------------------------------------------------- Публикация
publishing {
    publications {
        create<MavenPublication>("avros") {
            artifactId = "avros"
            from(components["java"])
            pom {
                name = "IoT Platform Avro schemas"
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
    dependsOn(checkAvrosCompatibility)
}