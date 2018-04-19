package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.example.demo.service","com.example.demo.controller"})
public class RocketmqDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(RocketmqDemoApplication.class, args);
	}
}
