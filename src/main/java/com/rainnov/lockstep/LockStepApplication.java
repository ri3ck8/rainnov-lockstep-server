package com.rainnov.lockstep;

import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@ConfigurationPropertiesScan
public class LockStepApplication {

    public static void main(String[] args) {
        SpringApplication.run(LockStepApplication.class, args);
    }

}
