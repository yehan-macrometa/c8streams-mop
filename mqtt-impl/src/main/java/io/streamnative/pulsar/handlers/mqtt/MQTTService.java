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
import io.streamnative.pulsar.handlers.mqtt.support.MQTTStubCnx;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
    private final ConcurrentHashMap<String, MQTTCommonConsumer> commonConsumersMap;

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
    }

    public synchronized CompletableFuture<MQTTCommonConsumer> getCommonConsumer(String virtualTopicName) {
        CompletableFuture<MQTTCommonConsumer> future = new CompletableFuture<>();
        String realTopicName = serverConfiguration.getSharder().getShardId(virtualTopicName);
        //String topicName = "c8locals.LocalMqtt";
        MQTTCommonConsumer consumer = commonConsumersMap.get(realTopicName);
        if (consumer != null) {
            future.complete(consumer);
        } else {
            Subscription sub = null;
            try {
                sub = PulsarTopicUtils
                    .getOrCreateSubscription(pulsarService, realTopicName, "commonSub",
                        serverConfiguration.getDefaultTenant(), serverConfiguration.getDefaultNamespace(),
                        serverConfiguration.getDefaultTopicDomain()).get();
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            MQTTStubCnx cnx = new MQTTStubCnx(pulsarService);
            MQTTCommonConsumer commonConsumer = new MQTTCommonConsumer(sub, sub.getTopicName(), "common", cnx);
            sub.addConsumer(commonConsumer);
            commonConsumer.flowPermits(1000);
            log.info("MqttVirtualTopics: Common consumer initialized");
            commonConsumersMap.put(realTopicName, commonConsumer);
            future.complete(commonConsumer);
        }
        return future;
    }
}
