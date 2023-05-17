/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.support;

import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.support.deadletter.DeadLetterConsumer;
import lombok.Getter;
import io.streamnative.pulsar.handlers.mqtt.support.deadletter.DeadLetterProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

@Slf4j
public class MQTTCommonConsumerGroup {
    
    private final int subscribersCount;
    @Getter
    private final List<MQTTCommonConsumer> consumers;
    private final String pulsarTopicName;
    private final OrderedExecutor orderedSendExecutor;
    private final DeadLetterConsumer deadLetterConsumer;
    private final DeadLetterProducer deadLetterProducer;
    private final PacketIdGenerator packetIdGenerator;
    
    private final Map<String, List<MQTTVirtualConsumer>> virtualConsumersMap = new ConcurrentHashMap<>();
    
    public MQTTCommonConsumerGroup(PulsarClient client, OrderedExecutor orderedSendExecutor, ExecutorService ackExecutor,
                                   ExecutorService dltExecutor, String pulsarTopicName, MQTTServerConfiguration config) throws PulsarClientException {
        this.subscribersCount = config.getMqttRealTopicSubscribersCount();
        this.pulsarTopicName = pulsarTopicName;
        this.orderedSendExecutor = orderedSendExecutor;
        if (subscribersCount < 1) {
            throw new IllegalArgumentException(String.format("Invalid value in subscribersCount: %d", subscribersCount));
        }
        this.consumers = new ArrayList<>();
        this.packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
        String deadLetterTopicName = pulsarTopicName + "-DLT";
        this.deadLetterConsumer =
            new DeadLetterConsumer(client, dltExecutor, packetIdGenerator, virtualConsumersMap,
                deadLetterTopicName, config.getMqttDLTThrottlingRatePerTopicInMsg());
        this.deadLetterProducer =
            new DeadLetterProducer(client, deadLetterTopicName, config.getMqttMaxRedeliverTimeSec() * 1000);
        
        for (int i = 0; i < subscribersCount; i++) {
            try {
                MQTTCommonConsumer commonConsumer = new MQTTCommonConsumer(pulsarTopicName, "common_" + i, i,
                    orderedSendExecutor, ackExecutor, client, packetIdGenerator, virtualConsumersMap,
                    deadLetterConsumer, deadLetterProducer);
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
        virtualConsumersMap.computeIfAbsent(mqttTopicName, s -> new CopyOnWriteArrayList<>()).add(consumer);
        log.info("Add virtual consumer to common group for virtual topic = {}, real topic = {}. left consumers = {}",
            mqttTopicName, this.pulsarTopicName, virtualConsumersMap.get(mqttTopicName).size());
    }
    
    public void remove(String mqttTopicName, MQTTVirtualConsumer consumer) {
        if (virtualConsumersMap.containsKey(mqttTopicName)) {
            boolean result = virtualConsumersMap.get(mqttTopicName).remove(consumer);
            log.info("Try remove({}) virtual consumer from common group for virtual topic = {}, real topic = {}. left consumers = {}",
                result, mqttTopicName, this.pulsarTopicName, virtualConsumersMap.get(mqttTopicName).size());
        }
    }
    
    public void close() {
        deadLetterConsumer.close();
        for (MQTTCommonConsumer commonConsumer : consumers) {
            commonConsumer.close();
        }
        Set<Map.Entry<String, List<MQTTVirtualConsumer>>> consumersSet = virtualConsumersMap.entrySet();
        // close virtual consumers
        for (Map.Entry<String, List<MQTTVirtualConsumer>> entry : consumersSet) {
            for (MQTTVirtualConsumer virtualConsumer: entry.getValue()) {
                virtualConsumer.close();
            }
        }
        deadLetterProducer.close();
    }
    
}
