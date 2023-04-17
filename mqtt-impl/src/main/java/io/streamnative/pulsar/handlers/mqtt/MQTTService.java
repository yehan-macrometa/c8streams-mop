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

import com.sun.tools.jdeprscan.scan.Scan;
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
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

        log.info("Start Pulsar topic scheduler");

        scheduledExecutor.scheduleWithFixedDelay(new ScanPulsarTopics(), 1, 1, TimeUnit.MINUTES);

    }

    public CompletableFuture<List<MQTTCommonConsumer>> getCommonConsumers(String virtualTopicName) {
        CompletableFuture<List<MQTTCommonConsumer>> future = new CompletableFuture<>();
        String realTopicName = serverConfiguration.getSharder().getShardId(virtualTopicName);
        List<MQTTCommonConsumer> consumers = commonConsumersMap.get(realTopicName);

        if (consumers != null) {
            future.complete(consumers);
        } else {
            synchronized (this) {
                consumers = commonConsumersMap.get(realTopicName);

                if (consumers != null) {
                    future.complete(consumers);
                } else {
                    Subscription sub = null;
                    try {
                        sub = PulsarTopicUtils
                                .getOrCreateSubscription(pulsarService, realTopicName, "commonSub",
                                        serverConfiguration.getDefaultTenant(), serverConfiguration.getDefaultNamespace(),
                                        serverConfiguration.getDefaultTopicDomain()).get();
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                        throw new RuntimeException("Failed to create `commonSub` subscription for real topic = " + realTopicName, e);
                    }
                    consumers = new ArrayList<>();

                    int subscribersCount = serverConfiguration.getMqttRealTopicSubscribersCount();
                    if (subscribersCount < 1) {
                        subscribersCount = 1;
                    }

                    for (int i = 0; i < subscribersCount; i++) {
                        try {
                            MQTTCommonConsumer commonConsumer = new MQTTCommonConsumer(sub, sub.getTopicName(), "common_" + i, i, orderedSendExecutor, ackExecutor, client);
                            log.info("MqttVirtualTopics: Common consumer #{} for real topic {} initialized", i, realTopicName);
                            consumers.add(commonConsumer);
                        } catch (Exception e) {
                            log.error("Could not create common consumer", e);
                        }
                    }
                    commonConsumersMap.put(realTopicName, consumers);
                    future.complete(consumers);
                }
            }
        }
        return future;
    }

    class ScanPulsarTopics implements Runnable {

        @Override
        public void run() {
            try {
                Set<Map.Entry<String, List<MQTTCommonConsumer>>> entries = commonConsumersMap.entrySet();
                for (Map.Entry<String, List<MQTTCommonConsumer>> entry : entries) {
                    Optional<Boolean> redirectOp;
                    try {
                        redirectOp = PulsarTopicUtils.isTopicRedirect(pulsarService, entry.getKey(),
                            serverConfiguration.getDefaultTenant(), serverConfiguration.getDefaultNamespace(), true
                            , serverConfiguration.getDefaultTopicDomain()).get();
                    } catch (Exception e) {
                        log.warn("Failed lookup for a pulsar topic = " + entry.getKey(), e);
                        continue;
                    }

                    if (!redirectOp.isPresent() || redirectOp.get() ||
                        entry.getValue().isEmpty() || entry.getValue().get(0).getConsumers().isEmpty()) {
                        log.info("[test] Remove a common consumers for pulsar topic = {}", entry.getKey());
                        for (MQTTCommonConsumer commonConsumer : entry.getValue()) {
                            commonConsumer.close();
                        }
                        commonConsumersMap.remove(entry.getKey());
                    } else {
                        log.info("[test] pulsar topic  = {} on this broker", entry.getKey());
                    }
                }
            } catch (Exception e) {
                log.warn("[test] ", e);
            }
        }
    }
}
