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

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.LISTENER_DEL;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.PLAINTEXT_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.PROTOCOL_NAME;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.SSL_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.SSL_PSK_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.getListenerPort;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyException;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import io.streamnative.pulsar.handlers.mqtt.proxy.PulsarServiceLookupHandler;
import io.streamnative.pulsar.handlers.mqtt.sharding.ConsistentHashSharder;
import io.streamnative.pulsar.handlers.mqtt.sharding.Sharder;
import io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
/**
 * MQTT Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class MQTTProtocolHandler implements ProtocolHandler {

    @Getter
    private MQTTServerConfiguration mqttConfig;

    @Getter
    private BrokerService brokerService;

    @Getter
    private String bindAddress;

    private MQTTProxyService proxyService;

    @Getter
    private MQTTService mqttService;

    private PulsarServiceLookupHandler lookupHandler;

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equals(protocol.toLowerCase());
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // init config
        if (conf instanceof MQTTServerConfiguration) {
            // in unit test, passed in conf will be MQTTServerConfiguration
            mqttConfig = (MQTTServerConfiguration) conf;
        } else {
            // when loaded with PulsarService as NAR, `conf` will be type of ServiceConfiguration
            mqttConfig = ConfigurationUtils.create(conf.getProperties(), MQTTServerConfiguration.class);
        }
        mqttConfig.setSharder(initSharder(mqttConfig.getAllRealTopics()));
        mqttConfig.setOrderedPublishExecutor(OrderedExecutor.newBuilder()
                .name("mqtt-pulsar-producer")
                .numThreads(50)
                .build());
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(mqttConfig.getBindAddress());
    }

    @Override
    public String getProtocolDataToAdvertise() {
        if (log.isDebugEnabled()) {
            log.debug("Get configured listener: {}", mqttConfig.getMqttListeners());
        }
        return mqttConfig.getMqttListeners();
    }

    @Override
    public void start(BrokerService brokerService) {
        this.brokerService = brokerService;
        mqttService = new MQTTService(brokerService, mqttConfig);

        PulsarServiceLookupHandler lookupHandler = null;


        if (mqttConfig.isMqttProxyEnabled() || mqttConfig.isMqttProxyEnable()) {
            Arrays.stream(mqttConfig.getMqttProxyPorts().split(",")).map(Integer::parseInt).forEach(port -> {
                MQTTProxyConfiguration proxyConfig = createProxyConf();
                proxyConfig.setMqttProxyPort(port);
                proxyConfig.setMqttProxyEnabled(true);
                try {
                    proxyService = new MQTTProxyService(mqttService, getLookupHandler(proxyConfig), proxyConfig);
                    proxyService.start();
                    log.info("Start MQTT proxy service at port: {}", proxyConfig.getMqttProxyPort());
                } catch (Exception ex) {
                    log.error("Failed to start MQTT proxy service.", ex);
                }
            });
        }

        if (mqttConfig.isMqttProxyTlsEnabled()) {
            Arrays.stream(mqttConfig.getMqttProxyTlsPorts().split(",")).map(Integer::parseInt).forEach(port -> {
                MQTTProxyConfiguration proxyConfig = createProxyConf();
                proxyConfig.setMqttProxyTlsPort(port);
                proxyConfig.setMqttProxyTlsEnabled(true);
                try {
                    proxyService = new MQTTProxyService(mqttService, getLookupHandler(proxyConfig), proxyConfig);
                    proxyService.start();
                    log.info("Start MQTT proxy service at port: {}", proxyConfig.getMqttProxyTlsPort());
                } catch (Exception ex) {
                    log.error("Failed to start MQTT proxy service.", ex);
                }
            });
        }

        if (mqttConfig.isMqttProxyTlsPskEnabled()) {
            Arrays.stream(mqttConfig.getMqttProxyTlsPorts().split(",")).map(Integer::parseInt).forEach(port -> {
                MQTTProxyConfiguration proxyConfig = createProxyConf();
                proxyConfig.setMqttProxyTlsPskPort(port);
                proxyConfig.setMqttProxyTlsEnabled(true);
                try {
                    proxyService = new MQTTProxyService(mqttService, getLookupHandler(proxyConfig), proxyConfig);
                    proxyService.start();
                    log.info("Start MQTT proxy service at port: {}", proxyConfig.getMqttProxyTlsPskPort());
                } catch (Exception ex) {
                    log.error("Failed to start MQTT proxy service.", ex);
                }
            });
        }

        log.info("Starting MqttProtocolHandler, MoP version is: '{}'", MopVersion.getVersion());
        log.info("Git Revision {}", MopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
                MopVersion.getBuildUser(),
                MopVersion.getBuildHost(),
                MopVersion.getBuildTime());
    }

    private PulsarServiceLookupHandler getLookupHandler( MQTTProxyConfiguration proxyConfig) throws MQTTProxyException {
        if (lookupHandler == null) {
            lookupHandler = new PulsarServiceLookupHandler(mqttService.getPulsarService(), proxyConfig);
        }
        return lookupHandler;
    }

    private MQTTProxyConfiguration createProxyConf() {
        MQTTProxyConfiguration proxyConfig = new MQTTProxyConfiguration();
        proxyConfig.setDefaultTenant(mqttConfig.getDefaultTenant());
        proxyConfig.setDefaultTopicDomain(mqttConfig.getDefaultTopicDomain());
        proxyConfig.setMaxNoOfChannels(mqttConfig.getMaxNoOfChannels());
        proxyConfig.setMaxFrameSize(mqttConfig.getMaxFrameSize());
        //proxyConfig.setMqttProxyPort(mqttConfig.getMqttProxyPort());
        //proxyConfig.setMqttProxyTlsPort(mqttConfig.getMqttProxyTlsPort());
        //proxyConfig.setMqttProxyTlsPskPort(mqttConfig.getMqttProxyTlsPskPort());
        //proxyConfig.setMqttProxyTlsEnabled(mqttConfig.isMqttProxyTlsEnabled());
        //proxyConfig.setMqttProxyTlsPskEnabled(mqttConfig.isMqttProxyTlsPskEnabled());
        proxyConfig.setBrokerServiceURL("pulsar://"
            + ServiceConfigurationUtils.getAppliedAdvertisedAddress(mqttConfig, true)
            + ":" + mqttConfig.getBrokerServicePort().get());
        proxyConfig.setMqttProxyNumAcceptorThreads(mqttConfig.getMqttProxyNumAcceptorThreads());
        proxyConfig.setMqttProxyNumIOThreads(mqttConfig.getMqttProxyNumIOThreads());
        proxyConfig.setMqttAuthenticationEnabled(mqttConfig.isMqttAuthenticationEnabled());
        proxyConfig.setMqttAuthenticationMethods(mqttConfig.getMqttAuthenticationMethods());
        proxyConfig.setMqttAuthorizationEnabled(mqttConfig.isMqttAuthorizationEnabled());
        proxyConfig.setBrokerClientAuthenticationPlugin(mqttConfig.getBrokerClientAuthenticationPlugin());
        proxyConfig.setBrokerClientAuthenticationParameters(mqttConfig.getBrokerClientAuthenticationParameters());

        proxyConfig.setTlsCertificateFilePath(mqttConfig.getTlsCertificateFilePath());
        proxyConfig.setTlsCertRefreshCheckDurationSec(mqttConfig.getTlsCertRefreshCheckDurationSec());
        proxyConfig.setTlsProtocols(mqttConfig.getTlsProtocols());
        proxyConfig.setTlsCiphers(mqttConfig.getTlsCiphers());
        proxyConfig.setTlsAllowInsecureConnection(mqttConfig.isTlsAllowInsecureConnection());

        proxyConfig.setTlsPskIdentityHint(mqttConfig.getTlsPskIdentityHint());
        proxyConfig.setTlsPskIdentity(mqttConfig.getTlsPskIdentity());
        proxyConfig.setTlsPskIdentityFile(mqttConfig.getTlsPskIdentityFile());

        proxyConfig.setTlsTrustStore(mqttConfig.getTlsTrustStore());
        proxyConfig.setTlsTrustCertsFilePath(mqttConfig.getTlsTrustCertsFilePath());
        proxyConfig.setTlsTrustStoreType(mqttConfig.getTlsTrustStoreType());
        proxyConfig.setTlsTrustStorePassword(mqttConfig.getTlsTrustStorePassword());

        proxyConfig.setTlsEnabledWithKeyStore(mqttConfig.isTlsEnabledWithKeyStore());
        proxyConfig.setTlsKeyStore(mqttConfig.getTlsKeyStore());
        proxyConfig.setTlsKeyStoreType(mqttConfig.getTlsKeyStoreType());
        proxyConfig.setTlsKeyStorePassword(mqttConfig.getTlsTrustStorePassword());
        proxyConfig.setTlsKeyFilePath(mqttConfig.getTlsKeyFilePath());

        proxyConfig.setMqttRealTopicCount(mqttConfig.getMqttRealTopicCount());
        proxyConfig.setMqttRealTopicNamePrefix(mqttConfig.getMqttRealTopicNamePrefix());
        proxyConfig.setMqttProxyMaxDelayMs(mqttConfig.getMqttProxyMaxDelayMs());
        proxyConfig.setMqttProxyMaxMsgInBatch(mqttConfig.getMqttProxyMaxMsgInBatch());
        proxyConfig.setSharder(mqttConfig.getSharder());
        log.info("proxyConfig broker service URL: {}", proxyConfig.getBrokerServiceURL());
        return proxyConfig;
    }

    private static Sharder initSharder(List<String> allRealTopics) {
        if (allRealTopics.isEmpty()) {
            return null;
        }
        return new ConsistentHashSharder(1000, allRealTopics);
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkArgument(mqttConfig != null);
        checkArgument(mqttConfig.getMqttListeners() != null);
        checkArgument(brokerService != null);

        String listeners = mqttConfig.getMqttListeners();
        String[] parts = listeners.split(LISTENER_DEL);
        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            for (String listener: parts) {
                if (listener.startsWith(PLAINTEXT_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, false));

                } else if (listener.startsWith(SSL_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, true));

                } else if (listener.startsWith(SSL_PSK_PREFIX) && mqttConfig.isTlsPskEnabled()) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, false, true));

                } else {
                    log.error("MQTT listener {} not supported. supports {}, {} or {}",
                            listener, PLAINTEXT_PREFIX, SSL_PREFIX, SSL_PSK_PREFIX);
                }
            }

            return builder.build();
        } catch (Exception e){
            log.error("MQTTProtocolHandler newChannelInitializers failed with", e);
            return null;
        }
    }

    @Override
    public void close() {
        if (proxyService != null) {
            proxyService.close();
        }
        if (mqttService != null) {
            mqttService.close();
        }
    }

}
