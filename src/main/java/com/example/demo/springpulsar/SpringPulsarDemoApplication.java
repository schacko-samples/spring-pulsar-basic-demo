package com.example.demo.springpulsar;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class SpringPulsarDemoApplication {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static void main(String[] args) {
		SpringApplication.run(SpringPulsarDemoApplication.class, args);
	}

	@Bean
	@ConditionalOnProperty(name = "timing.enabled", havingValue = "false")
	ApplicationRunner produceMessages(PulsarTemplate<Foo> pulsarTemplate) {
		return args -> {
			pulsarTemplate.setSchema(Schema.JSON(Foo.class));
			for (int i = 0; i < 1000; i++) {
				pulsarTemplate.sendAsync("demo-spring-pulsar", new Foo("foo", "bar"));
			}
		};
	}

	@Bean
	@ConditionalOnProperty(name = "timing.enabled", havingValue = "true", matchIfMissing = true)
	ApplicationRunner produceMessagesWithTiming(PulsarTemplate<Foo> pulsarTemplate) {
		return args -> {
			this.logger.info("BEGIN run");
			int numMsgs = 1000;
			Instant start = Instant.now();
			List<CompletableFuture<MessageId>> messageIds = new ArrayList<>();
			pulsarTemplate.setSchema(Schema.JSON(Foo.class));
			for (int i = 0; i < numMsgs; i++) {
				CompletableFuture<MessageId> messageId = pulsarTemplate.sendAsync("demo-spring-pulsar", new Foo("foo", "bar"));
				messageIds.add(messageId);
				messageId.whenComplete(this::logSendResult);
			}
			CompletableFuture.allOf(messageIds.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
			Duration total = Duration.between(start, Instant.now());
			this.logger.info("END run - sent {} msgs in {}ms ({} msg/ms)", numMsgs, total.toMillis(), (float)numMsgs/total.toMillis());
		};
	}

	@PulsarListener(subscriptionName = "demo-spring-pulsar-subscription", topics = "demo-spring-pulsar", schemaType = SchemaType.JSON)
	void process(Foo message) {
		this.logger.info("Message received: {}", message);
	}

	private void logSendResult(MessageId id, Throwable ex) {
		if (ex != null) {
			logger.error("Failed to send msg: " + ex.getMessage(), ex);
		}
		else {
			logger.debug("Sent msg: " + id);
		}
	}

	record Foo(String foo, String bar) {}
}
