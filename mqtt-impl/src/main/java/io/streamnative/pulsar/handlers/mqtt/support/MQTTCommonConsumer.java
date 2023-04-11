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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;

import java.util.ArrayList;
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
    private Map<String, List<MQTTConsumer>> consumers = new ConcurrentHashMap<>();
    private final PacketIdGenerator packetIdGenerator;

    public MQTTCommonConsumer(Subscription subscription, String pulsarTopicName, String consumerName, MQTTServerCnx cnx, PacketIdGenerator packetIdGenerator) {
        super(subscription, CommandSubscribe.SubType.Shared, pulsarTopicName, 0, 0, consumerName, 0, cnx,

                "", null, false, CommandSubscribe.InitialPosition.Latest, null, MessageId.latest);
        this.packetIdGenerator = packetIdGenerator;
    }

    @Override
    public Future<Void> sendMessages(List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks, int totalMessages, long totalBytes, long totalChunkedMessages, RedeliveryTracker redeliveryTracker) {
        log.debug("MqttVirtualTopics: Sending messages");
        List<Future> futures = new ArrayList<>();

        for (Entry entry : entries) {
            // Temporary message just to read the topic name
            List<MqttPublishMessage> messages = PulsarMessageConverter.toMqttMessages(null, entry,
                    packetIdGenerator.nextPacketId(), MqttQoS.AT_LEAST_ONCE);
            log.debug("MqttVirtualTopics: Sending {} messages of entry {}.", messages.size(), entry.getEntryId());
            for (MqttPublishMessage message : messages) {
                MessageImpl<byte[]> pulsarMessage = PulsarMessageConverter.toPulsarMsg(message);

                String virtualTopic = pulsarMessage.getProperty("virtualTopic");
                if (StringUtil.isNullOrEmpty(virtualTopic)) {
                    log.warn("Virtual topic name is empty for {} message of {} entry.", message.refCnt(),
                            entry.getEntryId());
                    continue;
                }

                log.debug("MqttVirtualTopics: Sending message to virtualTopic {}.", virtualTopic);

                List<MQTTConsumer> topicConsumers = consumers.get(virtualTopic);
                if (topicConsumers != null) {
                    log.debug("MqttVirtualTopics: There are {} consumer(s) for virtualTopic {}.",
                            topicConsumers.size(), virtualTopic);
                    topicConsumers.forEach(mqttConsumer -> {
                        futures.add(mqttConsumer.sendMessage(entry, message));
                    });
                } else {
                    log.debug("MqttVirtualTopics: No consumers for virtualTopic {}.", virtualTopic);
                }
            }

            getSubscription().acknowledgeMessage(
                    Collections.singletonList(entries.get(entries.size() - 1).getPosition()),
                    CommandAck.AckType.Cumulative, Collections.emptyMap());
        }

        // TODO: VirtualMqttTopic: Figure out what to send
        return new SucceededFuture<>(ImmediateEventExecutor.INSTANCE, null);
    }

    public void add(String mqttTopicName, MQTTConsumer consumer) {
        consumers.computeIfAbsent(mqttTopicName, s -> new CopyOnWriteArrayList<>()).add(consumer);
    }

    public void remove(String mqttTopicName, MQTTConsumer consumer) {
        if (consumers.containsKey(mqttTopicName)) {
            consumers.get(mqttTopicName).remove(consumer);
        }
    }
}
