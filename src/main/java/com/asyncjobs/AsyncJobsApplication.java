package com.asyncjobs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AsyncJobsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AsyncJobsApplication.class, args);
    }
}
