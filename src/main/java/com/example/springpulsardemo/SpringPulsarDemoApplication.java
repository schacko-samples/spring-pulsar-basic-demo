package com.example.springpulsardemo;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class SpringPulsarDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringPulsarDemoApplication.class, args);
	}

	@Bean
	ApplicationRunner runner1(
			PulsarTemplate<Foo> pulsarTemplate ) {

		String topic1 = "spring-pulsar-foo-demo-3";
		pulsarTemplate.setSchema(Schema.JSON(Foo.class));
		return args -> {
			for (int i = 0; i < 100; i++) {
				pulsarTemplate.sendAsync(topic1, new Foo("foo", "bar"));
			}
		};
	}

	@PulsarListener(subscriptionName = "spring-pulsar-foo-demo-subscription-3", topics = "spring-pulsar-foo-demo-3", schemaType = SchemaType.JSON)
	void process(Foo message) {
		System.out.println("Message Received: " + message);
	}

	record Foo(String foo, String bar) {
		@Override
		public String toString() {
			return "Foo{" + "foo='" + this.foo + '\'' + ", bar='" + this.bar + '\'' + '}';
		}
	}
}
