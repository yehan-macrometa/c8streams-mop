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

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTPublisherContext;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.util.FutureUtil;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter.toPulsarMsg;

/**
 * Abstract class for publish handler.
 */
@Slf4j
public abstract class AbstractQosPublishHandler implements QosPublishHandler {

    protected final PulsarService pulsarService;
    protected final MQTTServerConfiguration configuration;

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
            CompletableFuture<MessageId> future = topicOp.map(topic ->
                    MQTTPublisherContext.publishMessages(message, topic.getName()))
                    .orElseGet(() -> FutureUtil.failedFuture(
                            new BrokerServiceException.TopicNotFoundException(msg.variableHeader().topicName())));
            message.recycle();
            return future;
        });
    }
}
