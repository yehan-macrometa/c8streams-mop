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

import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTCommonConsumerGroup;
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
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
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
    private final ConcurrentHashMap<String, MQTTCommonConsumerGroup> commonConsumersMap;
    private final OrderedExecutor orderedSendExecutor;
    private final Semaphore throttlingSendExecutions;
    private final ExecutorService ackExecutor;
    private final ExecutorService dltExecutor;
    private final PulsarClient client;
    private final ScheduledExecutorService scheduledExecutor;
    private final Semaphore commonConsumerSemaphore;
    private static final String POLICY_ROOT = "/admin/policies/";

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
        this.commonConsumerSemaphore = new Semaphore(1);

        int numThreads = serverConfiguration.getMqttNumConsumerThreads();
        orderedSendExecutor = OrderedExecutor.newBuilder()
                .name("mqtt-common-consumer-send")
                .numThreads(numThreads)
                .maxTasksInQueue(serverConfiguration.getMqttCommonConsumersThrottling())
                .build();
        throttlingSendExecutions = new Semaphore(serverConfiguration.getMqttCommonConsumersThrottling() - 1);
        ackExecutor = Executors.newWorkStealingPool(numThreads);
        dltExecutor = Executors.newCachedThreadPool(
            new DefaultThreadFactory("mqtt-dlt-exec", false, 2));

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
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        if(serverConfiguration.isMqttInitCommonConsumers()) {
            // TODO: It works for single broker, because here doesn't check which topic to which broker belongs
            ackExecutor.execute(() -> {
                TopicName maskTopic = TopicName.get(serverConfiguration.getMqttRealTopicNamePrefix());
                Policies policies = null;
                while (policies == null) {
                    try {
                        Optional<Policies> policiesOp = pulsarService.getPulsarResources().getNamespaceResources()
                            .get(POLICY_ROOT + maskTopic.getNamespace());
                        if (policiesOp.isPresent()) {
                            policies = policiesOp.get();
                        }
                    } catch (MetadataStoreException e) {
                        log.warn("Failed to retrieve policies for namespace = {}", maskTopic.getNamespace());
                    }
                    log.warn("Unable to retrieve policies for namespace = {}. Wait 5 second for retry...", maskTopic.getNamespace());
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                    }
                }

                log.info("Successfully namespace = {} created! Initialising MQTT Common Consumer Groups", maskTopic.getNamespace());

                for (String pulsarTopic : serverConfiguration.getAllRealTopics()) {
                    MQTTCommonConsumerGroup consumerGroup = null;
                    try {
                        consumerGroup = new MQTTCommonConsumerGroup(client, orderedSendExecutor, throttlingSendExecutions,
                            ackExecutor, dltExecutor, pulsarTopic, serverConfiguration);
                        commonConsumersMap.put(pulsarTopic, consumerGroup);
                    } catch (PulsarClientException e) {
                        log.error("MQTT Consumer Group cannot be started for topic = {}", pulsarTopic, e);
                    }
                }

                log.info("Successfully Created All MQTT Common Consumer Groups!");

            });
        }

    }

    public CompletableFuture<MQTTCommonConsumerGroup> getCommonConsumers(String virtualTopicName) {
        CompletableFuture<MQTTCommonConsumerGroup> future = new CompletableFuture<>();
        String realTopicName = serverConfiguration.getSharder().getShardId(virtualTopicName);
        MQTTCommonConsumerGroup consumerGroup = commonConsumersMap.get(realTopicName);

        if (consumerGroup != null) {
            future.complete(consumerGroup);
        } else {
            try {
                commonConsumerSemaphore.acquire();
                consumerGroup = commonConsumersMap.get(realTopicName);
                if (consumerGroup != null) {
                    future.complete(consumerGroup);
                    commonConsumerSemaphore.release();
                } else {
                    ackExecutor.execute(() -> {
                        log.info("Create Common Consumer for topic = " + realTopicName);
                        try {
                            MQTTCommonConsumerGroup consumerGroup2 =
                                new MQTTCommonConsumerGroup(client, orderedSendExecutor, throttlingSendExecutions,
                                ackExecutor, dltExecutor, realTopicName, serverConfiguration);
                            commonConsumersMap.put(realTopicName, consumerGroup2);
                            log.info("Finish Create Common Consumer for topic = " + realTopicName);
                            future.complete(consumerGroup2);
                        } catch (PulsarClientException e) {
                            log.error("Could not create common consumer", e);
                            future.completeExceptionally(e);
                        } finally {
                            commonConsumerSemaphore.release();
                        }
                    });
                }

            } catch (InterruptedException e) {
                log.warn("Semaphore failed", e);
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
        Set<Map.Entry<String, MQTTCommonConsumerGroup>> entries = commonConsumersMap.entrySet();
        for (Map.Entry<String, MQTTCommonConsumerGroup> entry : entries) {

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
                        entry.getValue().close();
                        commonConsumersMap.remove(entry.getKey());
                    }
                }
            } catch (Exception e) {
                log.warn("Failed lookup a pulsar topic = {} or close common consumes", entry.getKey(), e);
            }
        }
    }

    public void close(){
        if (client != null) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close pulsar client", e);
            }
        }
    }
}
