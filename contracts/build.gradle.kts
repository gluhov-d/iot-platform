import org.apache.avro.SchemaParser
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.generic.GenericData

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

// ---------------------------------------------------------------- Публикация
// Адрес Nexus и учетная запись приходят снаружи (конвейер контрактов), в репозитории их нет:
//   -PnexusUrl=http://localhost:8091
//   ORG_GRADLE_PROJECT_nexusUsername / ORG_GRADLE_PROJECT_nexusPassword
val nexusUrl = providers.gradleProperty("nexusUrl").orElse("http://localhost:8091")

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