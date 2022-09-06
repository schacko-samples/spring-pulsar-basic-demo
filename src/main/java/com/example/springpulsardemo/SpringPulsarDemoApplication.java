package com.example.springpulsardemo;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class SpringPulsarDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringPulsarDemoApplication.class, args);
	}

	@Bean
	ApplicationRunner runner1(PulsarTemplate<String> pulsarTemplate) {

		String topic1 = "spring-pulsar-simple-demo";

		return args -> {
			for (int i = 0; i < 100; i++) {
				pulsarTemplate.send(topic1, "This is message " + (i + 1));
				Thread.sleep(100);
			}
		};
	}

	@PulsarListener(subscriptionName = "spring-pulsar-simple-demo-subscription", topics = "spring-pulsar-simple-demo")
	void process(String message) {
		System.out.println("Message Received: " + message);
	}

	record Foo(String foo, String bar) {
		@Override
		public String toString() {
			return "Foo{" + "foo='" + this.foo + '\'' + ", bar='" + this.bar + '\'' + '}';
		}
	}
}
