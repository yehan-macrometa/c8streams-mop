/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.support.deadletter;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DeadLetterProducer {

    private int maxRedeliverTimeMs;
    private PulsarClient client;
    private String deadLetterTopic;
    private Producer<byte[]> producer;

    public DeadLetterProducer(PulsarClient client, String deadLetterTopic, int maxRedeliverTimeMs) {
        this.maxRedeliverTimeMs = maxRedeliverTimeMs;
            this.client = client;
            this.deadLetterTopic = deadLetterTopic;
    }

    public void start() {
        try {
            if (log.isDebugEnabled()) {
                log.info("[DLT Producer] for topic = {} creating...", deadLetterTopic);
            }
            this.producer = client.newProducer()
                .topic(deadLetterTopic)
                .enableBatching(true)
                .batchingMaxMessages(1000)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .create();
            if (log.isDebugEnabled()) {
                log.info("[DLT Producer] for topic = {} created", deadLetterTopic);
            }
        } catch (Exception e) {
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
        try {
            if (log.isDebugEnabled()) {
                log.debug("[DLT Producer] Sent message = {} for pulsar topic = {} virtual topic = {}",
                    new String(msg.getData()), producer.getTopic(), msg.getProperty("virtualTopic"));
            }

            if (producer != null) {
                producer.newMessage().properties(msg.getProperties()).value(msg.getData()).send();
            }
        } catch (Exception e) {
            log.error("Could not create DeadLetter consumer or producer.", e);
        }

    }

    public void close() {
        try {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close DeadLetter producer", e);
        }
    }

}
