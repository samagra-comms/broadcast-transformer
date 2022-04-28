package com.uci.transformer.broadcast.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;

import lombok.extern.slf4j.Slf4j;

@EnableKafka
@EnableAsync
@EnableCaching
@ComponentScan(basePackages = {"com.uci.transformer.broadcast", "messagerosa", "com.uci.utils"})
@SpringBootApplication
@Slf4j
public class BroadcastTransformerApplication {

	public static void main(String[] args) {
		SpringApplication.run(BroadcastTransformerApplication.class, args);
	}

}
