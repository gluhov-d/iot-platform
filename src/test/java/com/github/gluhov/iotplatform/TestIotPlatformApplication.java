package com.github.gluhov.iotplatform;

import org.springframework.boot.SpringApplication;

public class TestIotPlatformApplication {

    public static void main(String[] args) {
        SpringApplication.from(IotPlatformApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
