/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.support;

import io.streamnative.pulsar.handlers.mqtt.support.deadletter.DeadLetterConsumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

@Slf4j
public class MQTTCommonConsumerGroup {

    private final int subscribersCount;
    @Getter
    private final List<MQTTCommonConsumer> consumers;
    private final String pulsarTopicName;
    //private final DeadLetterConsumer<byte[]> deadLetterConsumer;

    public MQTTCommonConsumerGroup(PulsarClient client, OrderedExecutor orderedSendExecutor, ExecutorService ackExecutor, String pulsarTopicName, int maxRedeliverTimeSec, int subscribersCount)
        throws PulsarClientException {
        this.subscribersCount = subscribersCount;
        this.pulsarTopicName = pulsarTopicName;
        if (subscribersCount < 1) {
            throw new IllegalArgumentException(String.format("Invalid value in subscribersCount: %d", subscribersCount));
        }
        this.consumers = new ArrayList<>();

        /*this.deadLetterConsumer =
            new DeadLetterConsumer(client, maxRedeliverTimeSec * 1000, pulsarTopicName + "-DLT");*/

        for (int i = 0; i < subscribersCount; i++) {
            try {
                MQTTCommonConsumer commonConsumer = new MQTTCommonConsumer(pulsarTopicName, "common_" + i, i,
                    orderedSendExecutor, ackExecutor, client);
                log.info("MqttVirtualTopics: Common consumer #{} for real topic {} initialized", i, pulsarTopicName);
                consumers.add(commonConsumer);
            } catch (PulsarClientException e) {
                log.error("Could not create common consumer", e);
                close();
                throw e;
            }
        }
    }

    public void add(String mqttTopicName, MQTTVirtualConsumer consumer) {
        consumers.forEach(c -> c.add(mqttTopicName, consumer));
    }

    public void remove(String mqttTopicName, MQTTVirtualConsumer consumer) {
        consumers.forEach(c -> c.remove(mqttTopicName, consumer));
    }

    public void acknowledgeMessage(MessageId messageId) {
        consumers.forEach(c -> c.acknowledgeMessage(messageId));
    }

    public void close() {
        for (MQTTCommonConsumer commonConsumer : consumers) {
            commonConsumer.close();
        }
        //deadLetterConsumer.close();
    }

}
