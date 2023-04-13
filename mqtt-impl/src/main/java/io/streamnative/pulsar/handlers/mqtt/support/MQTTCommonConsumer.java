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

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.SucceededFuture;
import io.netty.util.internal.StringUtil;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.protocol.Commands;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * MQTT consumer.
 */
@Slf4j
public class MQTTCommonConsumer extends Consumer {
    private Map<String, List<MQTTVirtualConsumer>> consumers = new ConcurrentHashMap<>();
    private PacketIdGenerator packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
    @Getter
    private int index;

    public MQTTCommonConsumer(Subscription subscription, String pulsarTopicName, String consumerName, MQTTStubCnx cnx, int index) {
        super(subscription, CommandSubscribe.SubType.Shared, pulsarTopicName, index, 0, consumerName, 0, cnx,
                "", null, false, CommandSubscribe.InitialPosition.Latest, null, MessageId.latest);
        this.index = index;

        // TODO: Use ScheduledExecutor and clean this part.
        new Thread(() -> {
            while (true) {
                log.info("[{}-{}] Redelivering unacked messages.", consumerName, consumerId());
                try {
                    redeliverUnacknowledgedMessages();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public Future<Void> sendMessages(List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks, int totalMessages, long totalBytes, long totalChunkedMessages, RedeliveryTracker redeliveryTracker) {
        log.debug("[{}-{}] Sending messages of {} entries", consumerName(), consumerId(), entries.size());

        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            int packetId = packetIdGenerator.nextPacketId();
            List<MqttPublishMessage> messages = PulsarMessageConverter.toMqttMessages(null, entry,
                    packetId, MqttQoS.AT_LEAST_ONCE);

            String virtualTopic = null;
            if (messages.size() > 0) {
                virtualTopic = messages.get(0).variableHeader().topicName();
            }

            if (StringUtil.isNullOrEmpty(virtualTopic)) {
                log.debug("[{}-{}] Virtual topic name is empty for {} entry.", consumerName(), consumerId(), entry.getEntryId());
                continue;
            }

            List<MQTTVirtualConsumer> topicConsumers = consumers.get(virtualTopic);

            if (topicConsumers != null && topicConsumers.size() > 0) {
                log.debug("[{}-{}] Sending message to {} consumer(s) for virtualTopic {}.", consumerName(), consumerId(),
                        topicConsumers.size(), virtualTopic);

                for (MqttPublishMessage message : messages) {
                    int finalI = i;
                    topicConsumers.forEach(mqttConsumer -> {
                        try {
                            mqttConsumer.sendMessage(entry, message, packetId);

                            Subscription subscription = getSubscription();
                            if (mqttConsumer.getQos() == MqttQoS.AT_MOST_ONCE) {
                                subscription.acknowledgeMessage(
                                        Collections.singletonList(PositionImpl.get(entry.getLedgerId(), entry.getEntryId())),
                                        CommandAck.AckType.Individual, Collections.emptyMap());
                            } else {
                                ConcurrentLongLongPairHashMap pendingAcks = getPendingAcks();
                                if (pendingAcks != null) {
                                    int batchSize = batchSizes.getBatchSize(finalI);
                                    pendingAcks.put(entry.getLedgerId(), entry.getEntryId(), batchSize, 1);
                                    if (log.isDebugEnabled()){
                                        log.debug("[{}-{}] Added {}:{} ledger entry with batchSize of {} to pendingAcks in"
                                                        + " broker.service.Consumer",
                                                subscription.getTopicName(), subscription, entry.getLedgerId(), entry.getEntryId(), batchSize);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            // TODO: We need to fix each issue possible.
                            // But we cannot allow one consumer to stop sending messages to all other consumers.
                            // A crash here does that.
                            // So have to catch it.
                            log.debug("[{}-{}] Could not send the message to consumer {}.", consumerName(), consumerId(), mqttConsumer.getConsumerName(), e);
                        }
                    });
                }
            } else {
                log.debug("MqttVirtualTopics: No consumers for virtualTopic {}.", virtualTopic);

                // TODO: Introduce DeadLetterTopic functionality
                ConcurrentLongLongPairHashMap pendingAcks = getPendingAcks();
                Subscription subscription = getSubscription();
                if (pendingAcks != null) {
                    int batchSize = batchSizes.getBatchSize(i);
                    pendingAcks.put(entry.getLedgerId(), entry.getEntryId(), batchSize, 1);
                    if (log.isDebugEnabled()){
                        log.debug("[{}-{}] Added {}:{} ledger entry with batchSize of {} to pendingAcks in"
                                        + " broker.service.Consumer",
                                subscription.getTopicName(), subscription, entry.getLedgerId(), entry.getEntryId(), batchSize);
                    }
                }
            }
        }

        // TODO: VirtualMqttTopic: Figure out what to send
        return new SucceededFuture<>(ImmediateEventExecutor.INSTANCE, null);
    }

    public void add(String mqttTopicName, MQTTVirtualConsumer consumer) {
        consumers.computeIfAbsent(mqttTopicName, s -> new CopyOnWriteArrayList<>()).add(consumer);
        log.debug("Add virtual consumer to common #{} for topic {}. left consumers = {}",
            index, mqttTopicName, consumers.get(mqttTopicName).size());
    }

    public void remove(String mqttTopicName, MQTTVirtualConsumer consumer) {
        if (consumers.containsKey(mqttTopicName)) {
            boolean result = consumers.get(mqttTopicName).remove(consumer);
            log.debug("Try remove({}) virtual consumer from common #{} for topic {}. left consumers = {}",
                result, index, mqttTopicName, consumers.get(mqttTopicName).size());
        }
    }

    public void acknowledgeMessage(long ledgerId, long entryId) {
        try {
            getSubscription().acknowledgeMessage(
                    Collections.singletonList(PositionImpl.get(ledgerId, entryId)),
                    CommandAck.AckType.Individual, Collections.emptyMap());
            getPendingAcks().remove(ledgerId, entryId);
        } catch (Exception e) {
            log.warn("Could not acknowledge message. {}", e.getMessage());
        }
    }
}
