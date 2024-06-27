package com.example.kafka_batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaBatchApplication.class, args);
	}

}
