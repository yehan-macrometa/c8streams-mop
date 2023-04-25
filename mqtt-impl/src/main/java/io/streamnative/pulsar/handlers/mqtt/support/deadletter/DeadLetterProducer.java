/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.support.deadletter;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.TimeUnit;

@Slf4j
public class DeadLetterProducer {

    private int maxRedeliverTimeMs;
    private Producer<byte[]> producer;

    public DeadLetterProducer(PulsarClient client, String deadLetterTopic, int maxRedeliverTimeMs) {
        this.maxRedeliverTimeMs = maxRedeliverTimeMs;

        try {
            producer = client.newProducer()
                .topic(deadLetterTopic)
                .batchingMaxMessages(1000)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .create();
        } catch (PulsarClientException e) {
            log.error("Could not create DeadLetter consumer or producer.", e);
        }
    }

    public boolean readyToBeDead(Message<byte[]> msg) {
        if (log.isDebugEnabled()) {
            log.debug("[DLT readyToBeDead] res = {}, msg.getPublishTime() = {}, maxRedeliverTimeMs = {}, system time = {}",
                msg.getPublishTime() + maxRedeliverTimeMs < System.currentTimeMillis(), msg.getPublishTime(), maxRedeliverTimeMs, System.currentTimeMillis());
        }
        return msg.getPublishTime() + maxRedeliverTimeMs < System.currentTimeMillis();
    }

    public void send(Message<byte[]> msg) throws PulsarClientException {
        if (log.isDebugEnabled()) {
            log.debug("[DLT Producer] Sent message = {} for pulsar topic = {} virtual topic = {}",
                new String(msg.getData()), producer.getTopic(), msg.getProperty("virtualTopic"));
        }
        producer.newMessage().properties(msg.getProperties()).value(msg.getData()).send();
    }

    public void close() {
        try {
            producer.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close DeadLetter producer", e);
        }
    }

}
