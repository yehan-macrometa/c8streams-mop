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
import org.apache.pulsar.common.api.proto.CommandAck;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * MQTT consumer.
 */
@Slf4j
public class MQTTCommonConsumer {
    @Getter
    private final Subscription subscription;
    private Map<String, List<MQTTVirtualConsumer>> consumers = new ConcurrentHashMap<>();
    private PacketIdGenerator packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
    @Getter
    private int index;
    private final OrderedExecutor orderedSendExecutor;
    private final ExecutorService ackExecutor;
    private Consumer<byte[]> consumer;

    public MQTTCommonConsumer(Subscription subscription, String pulsarTopicName, String consumerName, int index,
                              OrderedExecutor orderedSendExecutor, ExecutorService ackExecutor, PulsarClient client) {
        this.index = index;
        this.subscription = subscription;
        this.orderedSendExecutor = orderedSendExecutor;
        this.ackExecutor = ackExecutor;

        try {
            consumer = client.newConsumer()
                    .consumerName(consumerName)
                    .topic(pulsarTopicName)
                    .subscriptionName(subscription.getName())
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .messageListener(this::sendMessages)
                    .receiverQueueSize(100_000)
                    .subscribe();;
        } catch (PulsarClientException e) {
            log.error("Could not create common consumer.", e);
        }

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
                return;
            }

            List<MQTTVirtualConsumer> topicConsumers = consumers.get(virtualTopic);

            if (topicConsumers != null && topicConsumers.size() > 0) {
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
                            log.info("[test] Common consumer = " + consumer.getTopic() + ", message " +
                                (message.payload() != null ? message.payload().toString(StandardCharsets.UTF_8) : "<empty>"));
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
            }
        } catch (Exception e) {
            log.warn("An error occurred while processing sendMessage. {}", e.getMessage());
        }
    }

    public void add(String mqttTopicName, MQTTVirtualConsumer consumer) {
        consumers.computeIfAbsent(mqttTopicName, s -> new CopyOnWriteArrayList<>()).add(consumer);
        log.info("Add virtual consumer to common #{} for virtual topic = {}, real topic = {}. left consumers = {}",
            index, mqttTopicName, consumer.getTopicName(), consumers.get(mqttTopicName).size());
    }

    public void remove(String mqttTopicName, MQTTVirtualConsumer consumer) {
        if (consumers.containsKey(mqttTopicName)) {
            boolean result = consumers.get(mqttTopicName).remove(consumer);
            log.info("Try remove({}) virtual consumer from common #{} for virtual topic = {}, real topic = {}. left consumers = {}",
                result, index, mqttTopicName, consumer.getTopicName(), consumers.get(mqttTopicName).size());
        }
    }

    public void acknowledgeMessage(long ledgerId, long entryId, MessageId messageId) {
        ackExecutor.submit(() -> {
            try {
                if (messageId == null) {
                    ack(ledgerId, entryId);
                } else {
                    ack(messageId);
                }
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

    private void ack(long ledgerId, long entryId) {
        getSubscription().acknowledgeMessage(
                Collections.singletonList(PositionImpl.get(ledgerId, entryId)),
                CommandAck.AckType.Individual, Collections.emptyMap());
    }
}
