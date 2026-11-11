package com.github.gluhov.iotplatform.devicecollector.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RestController;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAnyPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

@AnalyzeClasses(packages = "com.github.gluhov.iotplatform.devicecollector",
        importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    private static final String ROOT = "com.github.gluhov.iotplatform.devicecollector";

    @ArchTest
    static final ArchRule layersAreRespected = layeredArchitecture().consideringOnlyDependenciesInLayers()
            .layer("Domain").definedBy(ROOT + ".domain..")
            .layer("InAdapters").definedBy(ROOT + ".adapter.in..")
            .layer("OutAdapters").definedBy(ROOT + ".adapter.out..")
            .layer("Support").definedBy(ROOT + ".support..")
            .layer("Config").definedBy(ROOT + ".config..")
            .whereLayer("InAdapters").mayNotBeAccessedByAnyLayer()
            .whereLayer("OutAdapters").mayNotBeAccessedByAnyLayer()
            .whereLayer("Support").mayNotBeAccessedByAnyLayer()
            .whereLayer("Config").mayOnlyBeAccessedByLayers("InAdapters", "OutAdapters")
            .because("domain must not know about adapters and configuration, adapters must not call each other");

    @ArchTest
    static final ArchRule domainDoesNotDependOnInfrastructure = noClasses()
            .that().resideInAPackage(ROOT + ".domain..")
            .should().dependOnClassesThat(resideInAnyPackage(
                    "org.apache.kafka..", "org.springframework.kafka..", "io.confluent..",
                    "java.sql..", "javax.sql..", "org.springframework.jdbc..", "org.springframework.transaction..",
                    "org.postgresql.."))
            .because("Kafka and PostgreSQL are reached only through ports");

    @ArchTest
    static final ArchRule domainUsesOnlySpringStereotypes = noClasses()
            .that().resideInAPackage(ROOT + ".domain..")
            .should().dependOnClassesThat(resideInAPackage("org.springframework..")
                    .and(not(resideInAPackage("org.springframework.stereotype.."))))
            .because("domain services are Spring beans (@Service, @Component), nothing more");

    @ArchTest
    static final ArchRule adaptersUseDomainOnlyThroughPorts = noClasses()
            .that().resideInAPackage(ROOT + ".adapter..")
            .should().dependOnClassesThat().resideInAPackage(ROOT + ".domain.service..")
            .because("adapters call the domain through port.in interfaces");

    @ArchTest
    static final ArchRule kafkaListenersAreInboundAdapters = methods()
            .that().areAnnotatedWith(KafkaListener.class)
            .should().beDeclaredInClassesThat().resideInAPackage(ROOT + ".adapter.in.kafka..")
            .because("receiving messages is the job of inbound adapters only");

    @ArchTest
    static final ArchRule controllersAreInboundAdapters = classes()
            .that().areAnnotatedWith(RestController.class)
            .should().resideInAPackage(ROOT + ".adapter.in.web..")
            .because("HTTP API is the job of inbound adapters only");

    // Задание: приложение не знает адресов физических шардов и библиотек ShardingSphere - только JDBC к Proxy
    @ArchTest
    static final ArchRule noShardingSphereLibraries = noClasses()
            .should().dependOnClassesThat().resideInAPackage("org.apache.shardingsphere..")
            .because("the service talks to ShardingSphere-Proxy as to a plain PostgreSQL");

    @ArchTest
    static final ArchRule outboundPortsAreImplementedByOutboundAdapters = classes()
            .that().implement(resideInAPackage(ROOT + ".domain.port.out.."))
            .should().resideInAnyPackage(ROOT + ".adapter.out..", ROOT + ".support..")
            .because("a class must not mix domain logic or inbound handling with outbound integration");

    @ArchTest
    static final ArchRule inboundPortsAreImplementedByDomainServices = classes()
            .that().implement(resideInAPackage(ROOT + ".domain.port.in.."))
            .should().resideInAPackage(ROOT + ".domain.service..")
            .because("use cases are implemented by domain services, not by adapters");

    @ArchTest
    static final ArchRule packagesAreFreeOfCycles = slices()
            .matching(ROOT + ".(*)..")
            .should().beFreeOfCycles();
}