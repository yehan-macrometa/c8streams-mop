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
package io.streamnative.pulsar.handlers.mqtt;

import static io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter.toPulsarMsg;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Abstract class for publish handler.
 */
@Slf4j
public abstract class AbstractQosPublishHandler implements QosPublishHandler {

    protected final PulsarService pulsarService;
    protected final MQTTServerConfiguration configuration;
    private static PulsarClient client;

    static {
        try {
            // TODO: Make the params configurable
            client = PulsarClient.builder()
                    .serviceUrl("pulsar://localhost:6650")
                    .authentication(AuthenticationFactory.token("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.HC3JF9HhzUP1nnABqH0NL5Oj7_cs2buz9G5a_Vk710I"))
                    .operationTimeout(1, TimeUnit.MINUTES)
                    .connectionsPerBroker(50)
                    .ioThreads(50)
                    .listenerThreads(50)
                    .build();
        } catch (PulsarClientException e) {
            log.error("Could not create Pulsar Client.", e);
        }
    }

    private static final Map<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();

    protected AbstractQosPublishHandler(PulsarService pulsarService, MQTTServerConfiguration configuration) {
        this.pulsarService = pulsarService;
        this.configuration = configuration;
    }

    protected CompletableFuture<Optional<Topic>> getTopicReference(MqttPublishMessage msg) {
        log.debug("MqttVirtualTopics: Returning common topic");
        String virtualTopicName = msg.variableHeader().topicName();
        String realTopicName = configuration.getSharder().getShardId(virtualTopicName);
        log.debug("[publish] to real topic {}", realTopicName);
        return PulsarTopicUtils.getTopicReference(pulsarService, realTopicName,
                configuration.getDefaultTenant(), configuration.getDefaultNamespace(), true
                , configuration.getDefaultTopicDomain());
    }

    protected CompletableFuture<MessageId> writeToPulsarTopic(MqttPublishMessage msg) {
        return getTopicReference(msg).thenCompose(topicOp -> {
            MessageImpl<byte[]> message = toPulsarMsg(msg);
            CompletableFuture<MessageId> future = topicOp.map(topic -> {
                CompletableFuture<MessageId> f = getProducer(topic.getName()).newMessage()
                        .properties(message.getProperties())
                        .value(message.getValue())
                        .sendAsync();
                message.release();
                return f;
            }).orElseGet(() ->
                FutureUtil.failedFuture(
                        new BrokerServiceException.TopicNotFoundException(msg.variableHeader().topicName())));
            return future;
        });
    }

    private Producer<byte[]> getProducer(String topic) {
        return producers.computeIfAbsent(topic, t -> {
            try {
                return client.newProducer()
                        .topic(t)
                        .blockIfQueueFull(true)
                        .sendTimeout(1, TimeUnit.MINUTES)
                        .maxPendingMessages(10)
                        .batchingMaxMessages(1000)
                        .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                        .create();
            } catch (PulsarClientException e) {
                log.error("Could not create producer.", e);
                return null;
            }
        });
    }
}
