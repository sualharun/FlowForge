package io.flowforge.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages="io.flowforge")
@EnableScheduling
public class ApiApplication {
    public static void main(String[] args) { SpringApplication.run(ApiApplication.class,args); }
}
