package com.github.gluhov.iotplatform.failedeventsprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FailedEventsProcessorApplication {

    public static void main(String[] args) {
        configureConfigName();
        SpringApplication.run(FailedEventsProcessorApplication.class, args);
    }

    /**
     * Своё имя файла конфигурации: процессор можно поднять вторым контекстом рядом с сервисом,
     * у которого на classpath свой application.yaml (R23).
     */
    public static void configureConfigName() {
        System.setProperty("spring.config.name", "failed-events-processor");
    }
}
