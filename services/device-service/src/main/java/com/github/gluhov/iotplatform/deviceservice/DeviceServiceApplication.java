package com.github.gluhov.iotplatform.deviceservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

@SpringBootApplication
public class DeviceServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeviceServiceApplication.class, args);
    }

    /** Время приёма команд (DeviceCommand.acceptedAt); в тестах подменяется фиксированными часами. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
