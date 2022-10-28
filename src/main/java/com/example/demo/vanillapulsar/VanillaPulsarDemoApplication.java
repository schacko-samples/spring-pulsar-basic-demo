package com.example.demo.vanillapulsar;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class VanillaPulsarDemoApplication {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static void main(String[] args) {
		SpringApplication.run(VanillaPulsarDemoApplication.class, args);
	}

	@Bean
	@ConditionalOnProperty(name = "timing.enabled", havingValue = "false")
	ApplicationRunner produceAndConsumeMessages() {
		return args -> {
			try (PulsarClient client = PulsarClient.builder()
					.serviceUrl("pulsar://localhost:6650")
					.build()) {
				try (Producer<Foo> producer = client.newProducer(Schema.JSON(Foo.class))
						.topic("demo-vanilla-pulsar")
						.create()) {
					try (Consumer<Foo> consumer = client.newConsumer(Schema.JSON(Foo.class))
							.topic("demo-vanilla-pulsar")
							.subscriptionName("demo-vanilla-pulsar-subscription")
							.subscribe()) {
						for (int i = 0; i < 1000; i++) {
							producer.sendAsync(new Foo("foo", "bar"));
						}
						Messages<Foo> messages = consumer.batchReceive();
						messages.forEach((msg) -> this.logger.info("Message received: {}", msg.getValue()));
						consumer.acknowledge(messages);
					}
				}
			}
		};
	}

	@Bean
	@ConditionalOnProperty(name = "timing.enabled", havingValue = "true", matchIfMissing = true)
	ApplicationRunner produceAndConsumeMessagesWithTiming() {
		return args -> {
			logger.info("BEGIN run");
			int numMsgs = 1000;
			Instant start = Instant.now();
			List<CompletableFuture<MessageId>> messageIds = new ArrayList<>();
			try (PulsarClient client = PulsarClient.builder()
					.serviceUrl("pulsar://localhost:6650")
					.build()) {
				try (Producer<Foo> producer = client.newProducer(Schema.JSON(Foo.class))
						.topic("demo-vanilla-pulsar")
						.create()) {
					try (Consumer<Foo> consumer = client.newConsumer(Schema.JSON(Foo.class))
							.topic("demo-vanilla-pulsar")
							.subscriptionName("demo-vanilla-pulsar-subscription")
							.subscribe()) {
						for (int i = 0; i < numMsgs; i++) {
							CompletableFuture<MessageId> messageId = producer.sendAsync(new Foo("foo", "bar"));
							messageIds.add(messageId);
							messageId.whenComplete(this::logSendResult);
						}
						CompletableFuture.allOf(messageIds.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
						Duration total = Duration.between(start, Instant.now());
						logger.info("END run - sent {} msgs in {}ms ({} msg/ms)", numMsgs, total.toMillis(), (float)numMsgs/total.toMillis());

						Messages<Foo> messages = consumer.batchReceive();
						messages.forEach((msg) -> this.logger.info("Message received: {}", msg.getValue()));
						consumer.acknowledge(messages);
					}
				}
			}
		};
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
