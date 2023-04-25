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
import io.streamnative.pulsar.handlers.mqtt.support.deadletter.DeadLetterConsumer;
import io.streamnative.pulsar.handlers.mqtt.support.deadletter.DeadLetterProducer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MQTT consumer.
 */
@Slf4j
public class MQTTCommonConsumer {

    @Getter
    private int index;
    private final OrderedExecutor orderedSendExecutor;
    private final ExecutorService ackExecutor;
    private Consumer<byte[]> consumer;
    private final DeadLetterConsumer deadLetterConsumer;
    private final DeadLetterProducer deadLetterProducer;
    private final PacketIdGenerator packetIdGenerator;
    private final Map<String, List<MQTTVirtualConsumer>> virtualConsumersMap;

    public MQTTCommonConsumer(String pulsarTopicName, String consumerName, int index,
                              OrderedExecutor orderedSendExecutor, ExecutorService ackExecutor, PulsarClient client,
                              PacketIdGenerator packetIdGenerator, Map<String, List<MQTTVirtualConsumer>> virtualConsumersMap,
                              DeadLetterConsumer deadLetterConsumer, DeadLetterProducer deadLetterProducer) throws PulsarClientException {
        this.index = index;
        this.orderedSendExecutor = orderedSendExecutor;
        this.ackExecutor = ackExecutor;
        this.packetIdGenerator = packetIdGenerator;
        this.deadLetterConsumer = deadLetterConsumer;
        this.deadLetterProducer = deadLetterProducer;
        this.virtualConsumersMap = virtualConsumersMap;

        consumer = client.newConsumer()
                .consumerName(consumerName)
                .topic(pulsarTopicName)
                .subscriptionName("commonSub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messageListener(this::sendMessage)
                .receiverQueueSize(100_000)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10000).build())
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

    private void sendMessage(Consumer<byte[]> consumer, Message<byte[]> msg) {
        String virtualTopic = msg.getProperty("virtualTopic");
        if (StringUtil.isNullOrEmpty(virtualTopic)) {
            return;
        }

        List<MQTTVirtualConsumer> topicConsumers = virtualConsumersMap.get(virtualTopic);

        if (topicConsumers != null && topicConsumers.size() > 0) {
            topicConsumers.forEach(mqttConsumer -> {
                orderedSendExecutor.executeOrdered(virtualTopic, () -> {
                    try {
                        int packetId = packetIdGenerator.nextPacketId();
                        MqttPublishMessage message = MessageBuilder.publish()
                            .messageId(packetId)
                            .payload(Unpooled.copiedBuffer(msg.getData()))
                            .topicName(virtualTopic)
                            .qos(mqttConsumer.getQos())
                            .retained(false)
                            .build();
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
                            log.debug("[Common consumer] Common consumer for pulsar topic {} received message: {}",
                                consumer.getTopic(), message.payload().toString(StandardCharsets.UTF_8));
                        }
                        mqttConsumer.sendMessage(message, consumer, packetId, msg.getMessageId());
                        if (mqttConsumer.getQos() == MqttQoS.AT_MOST_ONCE) {
                            consumer.acknowledge(msg);
                        }
                    } catch (Exception e) {
                        // TODO: We need to fix each issue possible.
                        // But we cannot allow one consumer to stop sending messages to all other consumers.
                        // A crash here does that.
                        // So have to catch it.
                        log.warn("[{}] Could not send the message to consumer {}.", consumer.getConsumerName(), mqttConsumer.getConsumerName(), e);
                    }
                });
            });
        } else if (log.isDebugEnabled()) {
            if (log.isDebugEnabled()) {
                log.info("[Common Consumer] Common consumer is not connected for virtual topic = {}" + virtualTopic);
            }
        }
        if (deadLetterProducer.readyToBeDead(msg)) {
            orderedSendExecutor.executeOrdered(virtualTopic, () -> {
                try {
                    deadLetterProducer.send(msg);
                    consumer.acknowledge(msg.getMessageId());
                } catch (Exception e) {
                    log.warn("An error occurred while processing sendMessage. {}", e.getMessage(), e);
                }
            });
        }
    }

    public void close() {
        log.info("[Common Consumer] Close a common consumer # {} for pulsar topic = {}", index, consumer.getTopic());
        // close common consumer
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close common consumer for pulsar topic = {}", consumer.getTopic(), e);
        }
    }
}
