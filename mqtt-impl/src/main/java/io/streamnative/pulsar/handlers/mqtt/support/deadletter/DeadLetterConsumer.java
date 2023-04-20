/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.support.deadletter;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DeadLetterConsumer<T> {

    private final PulsarClient client;
    private int maxRedeliverTimeMs;
    private String deadLetterTopic;
    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;

    public DeadLetterConsumer(PulsarClient client, int maxRedeliverTimeMs, String deadLetterTopic) {
        this.client = client;
        this.maxRedeliverTimeMs = maxRedeliverTimeMs;
        this.deadLetterTopic = deadLetterTopic;

        try {
            consumer = client.newConsumer()
                .topic(deadLetterTopic)
                .subscriptionName("commonSub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messageListener(this::sendMessages)
                .subscribe();

            producer = client.newProducer()
                .topic(deadLetterTopic)
                .create();
        } catch (PulsarClientException e) {
            log.error("Could not create DeadLetter consumer or producer.", e);
        }
    }

    public void negativeAcknowledge(Message<T> msg) {
       //msg.getPublishTime()
    }


    public boolean readyToBeDead(Message<T> msg) {
        return msg.getPublishTime() + maxRedeliverTimeMs < System.currentTimeMillis();
    }

    public void send(byte[] message) {
        //producer.send(message)
    }

    public void close() {
        try {
            consumer.close();
            producer.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close DeadLetter consumer/producer", e);
        }
    }



    private void sendMessages(Consumer<byte[]> consumer, Message<byte[]> msg) {

    }

}
