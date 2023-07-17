package com.idata.hhmdataconnector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class HhmDataConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(HhmDataConnectorApplication.class, args);
    }

}
