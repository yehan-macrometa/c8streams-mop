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

import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.streamnative.pulsar.handlers.mqtt.support.MQTTCommonConsumer;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTCommonConsumerGroup;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTVirtualConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class TopicSubscriptionManager {

    private final Map<String, Pair<MQTTCommonConsumerGroup, MQTTVirtualConsumer>> topicSubscriptions = Maps.newConcurrentMap();

    public Pair<MQTTCommonConsumerGroup, MQTTVirtualConsumer> putIfAbsent(String topic, MQTTCommonConsumerGroup consumerGroup,
                                                               MQTTVirtualConsumer consumer) {
        return topicSubscriptions.putIfAbsent(topic, Pair.of(consumerGroup, consumer));
    }

    public void removeSubscriptionConsumers() {
        topicSubscriptions.forEach((k, v) -> {
            try {
                removeConsumerIfExist(k, v.getLeft(), v.getRight());
            } catch (BrokerServiceException ex) {
                log.warn("subscription [{}] remove consumer {} error",
                        v.getLeft(), v.getRight(), ex);
            }
        });
    }

    public CompletableFuture<Void> unsubscribe(String topic, boolean cleanSubscription) {
        Pair<MQTTCommonConsumerGroup, MQTTVirtualConsumer> subscriptionConsumerPair = topicSubscriptions.get(topic);
        if (subscriptionConsumerPair == null) {
            return FutureUtil.failedFuture(new MQTTNoSubscriptionExistedException(
                    String.format("Can not found subscription for topic %s when unSubscribe", topic)));
        }
        try {
            removeConsumerIfExist(topic, subscriptionConsumerPair.getLeft(), subscriptionConsumerPair.getValue());
        } catch (BrokerServiceException e) {
            log.error("[ Subscription ] Subscription for topic {} Remove consumer fail.", topic, e);
            FutureUtil.failedFuture(e);
        }
        return CompletableFuture.completedFuture(null);
        
    }

    public CompletableFuture<Void> removeSubscriptions() {
        List<CompletableFuture<Void>> futures = topicSubscriptions.keySet()
                .stream()
                .map(topic -> unsubscribe(topic, true))
                .collect(Collectors.toList());
        return FutureUtil.waitForAll(futures);
    }

    private void removeConsumerIfExist(String topic, MQTTCommonConsumerGroup consumerGroup, MQTTVirtualConsumer consumer) throws BrokerServiceException {
        consumerGroup.remove(topic, consumer);
    }
}
