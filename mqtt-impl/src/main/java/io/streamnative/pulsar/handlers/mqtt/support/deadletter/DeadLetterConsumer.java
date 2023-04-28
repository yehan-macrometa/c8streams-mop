/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.support.deadletter;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.internal.StringUtil;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTVirtualConsumer;
import io.streamnative.pulsar.handlers.mqtt.support.MessageBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@Slf4j
public class DeadLetterConsumer {

    private Consumer<byte[]> consumer;
    private ExecutorService dltExecutor;
    private final RateLimiter rateLimiter;
    private final PacketIdGenerator packetIdGenerator;
    private final Map<String, List<MQTTVirtualConsumer>> virtualConsumersMap;

    public DeadLetterConsumer(PulsarClient client, ExecutorService dltExecutor, PacketIdGenerator packetIdGenerator,
                              Map<String, List<MQTTVirtualConsumer>> virtualConsumersMap,
                              String deadLetterTopic, int throttlingRate) {
        this.dltExecutor = dltExecutor;
        if (throttlingRate > 0) {
            this.rateLimiter = RateLimiter.create(throttlingRate);
        } else {
            this.rateLimiter = null;
        }
        this.packetIdGenerator = packetIdGenerator;
        this.virtualConsumersMap = virtualConsumersMap;

        try {
            consumer = client.newConsumer()
                .consumerName("commonDlt")
                .topic(deadLetterTopic)
                .subscriptionName("commonDltSub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10000).build())
                .subscribe();

            runThrottlingConsumer();
        } catch (PulsarClientException e) {
            log.error("Could not create DeadLetter consumer or producer.", e);
        }
    }

    private void runThrottlingConsumer() {
        dltExecutor.submit(() -> {
            while (true) {
                try {
                    Message<byte[]> msg = consumer.receive();
                    sendDeadLetterMessage(consumer, msg);
                } catch (PulsarClientException e) {
                    log.warn("Failure to receive a message from DLT", e);
                } finally {
                    if (rateLimiter != null) {
                        rateLimiter.acquire();
                    }
                }
            }
        });
    }

    private void sendDeadLetterMessage(Consumer<byte[]> consumer, Message<byte[]> msg) {
        String virtualTopic = msg.getProperty("virtualTopic");
        if (StringUtil.isNullOrEmpty(virtualTopic)) {
            log.warn("[{}] Virtual topic name is empty for message {}.", consumer.getConsumerName(), msg.getMessageId());
            return;
        }
        List<MQTTVirtualConsumer> topicConsumers = virtualConsumersMap.get(virtualTopic);
        if (topicConsumers != null && topicConsumers.size() > 0) {
            topicConsumers.forEach(mqttConsumer -> {
                try {
                    int packetId = packetIdGenerator.nextPacketId();
                    MqttPublishMessage message = MessageBuilder.publish()
                        .messageId(packetId)
                        .payload(Unpooled.copiedBuffer(msg.getData()))
                        .topicName(virtualTopic)
                        .qos(mqttConsumer.getQos())
                        .retained(false)
                        .build();

                    if (log.isDebugEnabled()) {
                        log.debug("[DLT Consumer] Consumer for pulsar topic {} received message: {}",
                            consumer.getTopic(), message.payload().toString(StandardCharsets.UTF_8));
                    }
                    mqttConsumer.sendMessage(message, consumer, packetId, msg.getMessageId());
                    if (mqttConsumer.getQos() == MqttQoS.AT_MOST_ONCE) {
                        consumer.acknowledge(msg);
                    }
                } catch (Exception e) {
                    log.warn("[{}] Could not send the message to consumer {}.",
                        consumer.getConsumerName(), mqttConsumer.getConsumerName(), e);
                }
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[DLT Consumer] Consumer for pulsar topic {} received message: {}",
                    consumer.getTopic(), new String(msg.getData()));
            }
        }
    }

    public void close() {
        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (PulsarClientException e) {
            log.warn("Failed to close DeadLetter consumer", e);
        }
    }

}
