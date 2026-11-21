package com.github.gluhov.iotplatform.failedeventsprocessor.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.springframework.kafka.annotation.KafkaListener;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/** Правила слоёв (FR-022): домен не знает про Kafka и S3, слушатель живёт только в adapter.in.kafka. */
@AnalyzeClasses(packages = "com.github.gluhov.iotplatform.failedeventsprocessor",
        importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule domainDoesNotDependOnKafkaOrS3 = noClasses()
            .that().resideInAPackage("..domain..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "org.springframework.kafka..", "org.apache.kafka..", "io.confluent..",
                    "software.amazon.awssdk..");

    /** @KafkaListener стоит на методе: правило смотрит на методы, иначе оно ничего не проверяет. */
    @ArchTest
    static final ArchRule kafkaListenersOnlyInKafkaAdapter = methods()
            .that().areAnnotatedWith(KafkaListener.class)
            .should().beDeclaredInClassesThat().resideInAPackage("..adapter.in.kafka..");
}
