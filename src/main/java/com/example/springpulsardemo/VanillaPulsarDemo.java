package com.example.springpulsardemo;

import org.apache.pulsar.client.api.*;

public class VanillaPulsarDemo {

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        Producer<Foo> producer = client.newProducer(Schema.JSON(Foo.class))
                .topic("my-topic-1")
                .create();

        for (int i = 0; i < 100; i ++) {
            producer.sendAsync(new Foo("foo", "bar"));
        }

        Consumer<Foo> consumer = client.newConsumer(Schema.JSON(Foo.class))
                .topic("my-topic-1")
                .subscriptionName("my-subscription-1-xyz")
                .subscribe();


        Messages<Foo> messages = consumer.batchReceive();
        for (Message<Foo> message : messages) {
            System.out.println("Foo: " + message.getValue());
        }
        consumer.acknowledge(messages);

        consumer.close();
        producer.close();
        client.close();

    }

    record Foo(String foo, String bar) {
        @Override
        public String toString() {
            return "Foo{" + "foo='" + this.foo + '\'' + ", bar='" + this.bar + '\'' + '}';
        }
    }



}
