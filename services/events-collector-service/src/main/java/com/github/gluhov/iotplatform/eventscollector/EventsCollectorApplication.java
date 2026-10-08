package com.github.gluhov.iotplatform.eventscollector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class EventsCollectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventsCollectorApplication.class, args);
    }
}