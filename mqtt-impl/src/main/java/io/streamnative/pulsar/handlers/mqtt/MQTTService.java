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

import io.streamnative.pulsar.handlers.mqtt.support.MQTTCommonConsumer;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTMetricsCollector;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTMetricsProvider;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTPublisherContext;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.Notification;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.broker.cache.LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT;

/**
 * Main class for mqtt service.
 */
@Slf4j
public class MQTTService {

    @Getter
    private final BrokerService brokerService;

    @Getter
    private final MQTTServerConfiguration serverConfiguration;

    @Getter
    private final PulsarService pulsarService;

    @Getter
    private final MQTTAuthenticationService authenticationService;

    @Getter
    private final AuthorizationService authorizationService;

    @Getter
    private final MQTTMetricsProvider metricsProvider;

    @Getter
    private final MQTTMetricsCollector metricsCollector;

    @Getter
    private final MQTTConnectionManager connectionManager;

    @Getter
    private final ConcurrentHashMap<String, List<MQTTCommonConsumer>> commonConsumersMap;
    private final OrderedExecutor orderedSendExecutor;
    private final ExecutorService ackExecutor;
    private final PulsarClient client;
    private final ScheduledExecutorService scheduledExecutor;

    public MQTTService(BrokerService brokerService, MQTTServerConfiguration serverConfiguration) {
        this.brokerService = brokerService;
        this.pulsarService = brokerService.pulsar();
        this.serverConfiguration = serverConfiguration;
        this.authorizationService = brokerService.getAuthorizationService();
        this.metricsCollector = new MQTTMetricsCollector(serverConfiguration);
        this.metricsProvider = new MQTTMetricsProvider(metricsCollector);
        this.pulsarService.addPrometheusRawMetricsProvider(metricsProvider);
        this.authenticationService = serverConfiguration.isMqttAuthenticationEnabled()
            ? new MQTTAuthenticationService(brokerService.getAuthenticationService(),
                serverConfiguration.getMqttAuthenticationMethods()) : null;
        this.connectionManager = new MQTTConnectionManager();
        this.commonConsumersMap = new ConcurrentHashMap<>();
        this.scheduledExecutor = Executors.newScheduledThreadPool(1);

        int numThreads = serverConfiguration.getMqttNumConsumerThreads();
        orderedSendExecutor = OrderedExecutor.newBuilder()
                .name("mqtt-common-consumer-send")
                .numThreads(numThreads)
                .maxTasksInQueue(100_000)
                .build();
        ackExecutor = Executors.newWorkStealingPool(numThreads);

        pulsarService.getLocalMetadataStore().registerListener(this::handleMetadataStoreNotification);

        MQTTPublisherContext.init(brokerService, serverConfiguration);

        try {
            client = PulsarClient.builder()
                    .serviceUrl("pulsar://localhost:6650")
                    .authentication(
                            brokerService.getPulsar().getConfiguration().getBrokerClientAuthenticationPlugin(),
                            brokerService.getPulsar().getConfiguration().getBrokerClientAuthenticationParameters())
                    .operationTimeout(1, TimeUnit.MINUTES)
                    .connectionsPerBroker(numThreads)
                    .ioThreads(numThreads)
                    .listenerThreads(numThreads)
                    .build();

            initConsumers(brokerService, serverConfiguration);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private void initConsumers(BrokerService brokerService, MQTTServerConfiguration serverConfiguration) {
        List<String> topics = MQTTProtocolHandler.getRealTopics(serverConfiguration.getMqttRealTopicNamePrefix(),
                serverConfiguration.getMqttRealTopicCount());
        if (topics == null) {
            return;
        }
        topics.forEach(topic -> {
            try {
                Optional<Boolean> redirect = PulsarTopicUtils.isTopicRedirect(brokerService.getPulsar(), topic, serverConfiguration.getDefaultTenant(), serverConfiguration.getDefaultNamespace(), true
                        , serverConfiguration.getDefaultTopicDomain()).get();
                if (!redirect.orElse(true)) {
                    createCommonConsumers(topic).get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public CompletableFuture<List<MQTTCommonConsumer>> getCommonConsumers(String virtualTopicName) {
        String realTopicName = serverConfiguration.getSharder().getShardId(virtualTopicName);
        List<MQTTCommonConsumer> consumers = commonConsumersMap.get(realTopicName);

        if (consumers != null) {
            return CompletableFuture.completedFuture(consumers);
        } else {
            return createCommonConsumers(realTopicName);
        }
    }

    private synchronized CompletableFuture<List<MQTTCommonConsumer>> createCommonConsumers(String realTopicName) {
        CompletableFuture<List<MQTTCommonConsumer>> future = new CompletableFuture<>();
        List<MQTTCommonConsumer> consumers = commonConsumersMap.get(realTopicName);

        if (consumers != null) {
            future.complete(consumers);
        } else {
            consumers = new ArrayList<>();

            int subscribersCount = serverConfiguration.getMqttRealTopicSubscribersCount();
            if (subscribersCount < 1) {
                subscribersCount = 1;
            }

            Exception exception = null;
            for (int i = 0; i < subscribersCount; i++) {
                try {
                    MQTTCommonConsumer commonConsumer = new MQTTCommonConsumer(realTopicName, "common_" + i, i, orderedSendExecutor, ackExecutor, client);
                    log.info("MqttVirtualTopics: Common consumer #{} for real topic {} initialized", i, realTopicName);
                    consumers.add(commonConsumer);
                } catch (PulsarClientException e) {
                    log.error("Could not create common consumer", e);
                    exception = e;
                    break;
                }
            }

            if (exception == null) {
                commonConsumersMap.put(realTopicName, consumers);
                future.complete(consumers);
            } else {
                consumers.forEach(MQTTCommonConsumer::close);
                future.completeExceptionally(exception);
            }
        }

        return future;
    }

    private void handleMetadataStoreNotification(Notification n) {
        if (n.getPath().startsWith(LOCAL_POLICIES_ROOT)) {
            final NamespaceName namespace = NamespaceName.get(NamespaceBundleFactory.getNamespaceFromPoliciesPath(n.getPath()));
            log.info("Policy updated for namespace {}, refreshing the common consumers.", namespace);
            checkAndCloseCommonConsumers(namespace);
        }
    }

    private void checkAndCloseCommonConsumers(NamespaceName namespace) {
        Set<Map.Entry<String, List<MQTTCommonConsumer>>> entries = commonConsumersMap.entrySet();
        for (Map.Entry<String, List<MQTTCommonConsumer>> entry : entries) {

            try {
                TopicName topicName = TopicName.get(entry.getKey());
                if (namespace.toString().equals(topicName.getNamespace())) {
                    Optional<Boolean> redirectOp = PulsarTopicUtils.isTopicRedirect(pulsarService, entry.getKey(),
                        serverConfiguration.getDefaultTenant(), serverConfiguration.getDefaultNamespace(), true
                        , serverConfiguration.getDefaultTopicDomain()).get();
                    if (log.isDebugEnabled()) {
                        log.info("Checking common consumers it rebalanced to another broker for pulsar topic = {} with result = {}",
                            entry.getKey(), redirectOp.toString());
                    }
                    if (!redirectOp.isPresent() || redirectOp.get()) {
                        for (MQTTCommonConsumer commonConsumer : entry.getValue()) {
                            commonConsumer.close();
                        }
                        commonConsumersMap.remove(entry.getKey());
                    }
                }
            } catch (Exception e) {
                log.warn("Failed lookup a pulsar topic = {} or close common consumes", entry.getKey(), e);
            }
        }
    }
}
