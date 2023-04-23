/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.internal.StringUtil;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandAck;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * MQTT consumer.
 */
@Slf4j
public class MQTTCommonConsumer {
    /*@Getter
    private final Subscription subscription;*/
    @Getter
    private Map<String, List<MQTTVirtualConsumer>> consumers = new ConcurrentHashMap<>();
    private PacketIdGenerator packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
    @Getter
    private int index;
    private final OrderedExecutor orderedSendExecutor;
    private final ExecutorService ackExecutor;
    private Consumer<byte[]> consumer;

    public MQTTCommonConsumer(/*Subscription subscription, */String pulsarTopicName, String consumerName, int index,
                              OrderedExecutor orderedSendExecutor, ExecutorService ackExecutor, PulsarClient client) throws PulsarClientException {
        this.index = index;
        /*this.subscription = subscription;*/
        this.orderedSendExecutor = orderedSendExecutor;
        this.ackExecutor = ackExecutor;

        consumer = client.newConsumer()
                .consumerName(consumerName)
                .topic(pulsarTopicName)
                .subscriptionName("commonSub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messageListener(this::sendMessages)
                .receiverQueueSize(100_000)
                .subscribe();

        // TODO: Use ScheduledExecutor and clean this part.
//        new Thread(() -> {
//            while (true) {
//                log.info("[{}-{}] Redelivering unacked messages.", consumerName, consumerId());
//                try {
//                    subscription.getDispatcher().redeliverUnacknowledgedMessages(this);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                try {
//                    Thread.sleep(30000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }).start();
    }

    private void sendMessages(Consumer<byte[]> consumer, Message<byte[]> msg) {
        try {
            int packetId = packetIdGenerator.nextPacketId();
            String virtualTopic = msg.getProperty("virtualTopic");

            if (StringUtil.isNullOrEmpty(virtualTopic)) {
                log.warn("[{}] Virtual topic name is empty for message {}.", consumer.getConsumerName(), msg.getMessageId());
                log.info("ConsumerDebug: [{}-{}] Virtual topic name is empty.", consumer.getTopic(), consumer.getConsumerName());
                return;
            }

            List<MQTTVirtualConsumer> topicConsumers = consumers.get(virtualTopic);

            if (topicConsumers != null && topicConsumers.size() > 0) {
                log.info("ConsumerDebug: [{}-{}] Found {} virtual consumers for {}.", consumer.getTopic(),
                        consumer.getConsumerName(), topicConsumers.size(), virtualTopic);
                log.info("ConsumerDebug: [{}-{}] Managing virtual consumers for {} virtual topics.", consumer.getTopic(),
                        consumer.getConsumerName(), consumers.size());
                topicConsumers.forEach(mqttConsumer -> {
                    MqttPublishMessage message = MessageBuilder.publish()
                            .messageId(packetId)
                            .payload(Unpooled.copiedBuffer(msg.getData()))
                            .topicName(virtualTopic)
                            .qos(mqttConsumer.getQos())
                            .retained(false)
                            .build();

                    orderedSendExecutor.executeOrdered(virtualTopic, () -> {
                        try {
                            // TODO: Use the Promise returned by sendMessage
                            // It is better to wait until the previous write is completed to write to the same channel
                            // again. We need to use the Promise returned by sendMessage method and wait until it
                            // completes to send a new message to this same virtualTopic.
                            //
                            // Idea:
                            // We can keep this Promise and attach a list of message to it. When there is a new message
                            // to the same virtualTopic, if the previous Promise is not completed, we can queue this
                            // new message in that attached list and skip calling the sendMessage. Whenever this Promise
                            // completes, we can resubmit these messages via this.sendMessage method or
                            // orderedSendExecutor.executeOrdered method.
                            if (log.isDebugEnabled()) {
                                log.info("[Common consumer] Common consumer for pulsar topic {} received message: {}",
                                    consumer.getTopic(), message.payload().toString(StandardCharsets.UTF_8));
                            }
                            mqttConsumer.sendMessage(message, packetId, msg.getMessageId());
                        } catch (Exception e) {
                            // TODO: We need to fix each issue possible.
                            // But we cannot allow one consumer to stop sending messages to all other consumers.
                            // A crash here does that.
                            // So have to catch it.
                            log.warn("[{}] Could not send the message to consumer {}.", consumer.getConsumerName(), mqttConsumer.getConsumerName(), e);
                        }

                        try {
                            if (mqttConsumer.getQos() == MqttQoS.AT_MOST_ONCE) {
                                consumer.acknowledge(msg);
                            }
                        } catch (Exception e) {
                            log.error("Error when handling acknowledgement.", e);
                        }
                    });
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.info("[Common Consumer] Common consumer is not connected for virtual topic = {}" + virtualTopic);
                }
                log.info("ConsumerDebug: [{}-{}] No virtual consumer for {}.", consumer.getTopic(), consumer.getConsumerName(), virtualTopic);
            }
        } catch (Exception e) {
            log.warn("An error occurred while processing sendMessage. {}", e.getMessage());
        }
    }

    public void add(String mqttTopicName, MQTTVirtualConsumer consumer) {
        consumers.computeIfAbsent(mqttTopicName, s -> new CopyOnWriteArrayList<>()).add(consumer);
        log.info("Add virtual consumer to common #{} for virtual topic = {}, real topic = {}. left consumers = {}",
            index, mqttTopicName, this.consumer.getTopic(), consumers.get(mqttTopicName).size());
    }

    public void remove(String mqttTopicName, MQTTVirtualConsumer consumer) {
        if (consumers.containsKey(mqttTopicName)) {
            log.info("ConsumerDebug: [{}-{}] Removing virtual consumers for {}.", this.consumer.getTopic(),
                    this.consumer.getConsumerName(), mqttTopicName);
            log.info("ConsumerDebug: [{}-{}] Managing virtual consumers for {} virtual topics.", this.consumer.getTopic(),
                    consumer.getConsumerName(), consumers.size());
            boolean result = consumers.get(mqttTopicName).remove(consumer);
            log.info("Try remove({}) virtual consumer from common #{} for virtual topic = {}, real topic = {}. left consumers = {}",
                result, index, mqttTopicName, this.consumer.getTopic(), consumers.get(mqttTopicName).size());
        } else {
            log.info("ConsumerDebug: [{}-{}] No virtual consumers to remove for {}.", this.consumer.getTopic(),
                    this.consumer.getConsumerName(), mqttTopicName);
            log.info("ConsumerDebug: [{}-{}] Managing virtual consumers for {} virtual topics.", this.consumer.getTopic(),
                    consumer.getConsumerName(), consumers.size());
        }
    }

    public void acknowledgeMessage(/*long ledgerId, long entryId, */MessageId messageId) {
        ackExecutor.submit(() -> {
            try {
                /*if (messageId == null) {
                    ack(ledgerId, entryId);
                } else {*/
                    ack(messageId);
                //}
            } catch (Exception e) {
                log.warn("Could not acknowledge message. {}", e.getMessage());
            }
        });
    }

    private void ack(MessageId messageId) {
        try {
            consumer.acknowledge(messageId);
        } catch (PulsarClientException e) {
            log.error("Could not acknowledge the message {}.", messageId);
        }
    }

    /*private void ack(long ledgerId, long entryId) {
        getSubscription().acknowledgeMessage(
                Collections.singletonList(PositionImpl.get(ledgerId, entryId)),
                CommandAck.AckType.Individual, Collections.emptyMap());
    }*/

    public void close() {
        log.info("[Common Consumer] Close a common consumer # {} for pulsar topic = {}", index, consumer.getTopic());
        Set<Map.Entry<String, List<MQTTVirtualConsumer>>> consumersSet = consumers.entrySet();
        // close virtual consumers
        for (Map.Entry<String, List<MQTTVirtualConsumer>> entry : consumersSet) {
            for (MQTTVirtualConsumer virtualConsumer: entry.getValue()) {
                virtualConsumer.close();
            }
        }
        // close common consumer
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close common consumer for pulsar topic = {}", consumer.getTopic(), e);
        }
    }
}
