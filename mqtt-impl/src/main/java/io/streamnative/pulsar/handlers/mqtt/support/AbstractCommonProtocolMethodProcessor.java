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

import co.macrometa.c8streams.api.util.C8Retriever;
import com.c8db.C8DB;
import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.Jwts;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.util.CharsetUtil;
import io.streamnative.pulsar.handlers.mqtt.MQTTAuthenticationService;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.restrictions.InvalidReceiveMaximumException;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.MqttConnectAck;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ClientRestrictions;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.c8db.C8DBCluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Common protocol method processor.
 */
@Slf4j
public abstract class AbstractCommonProtocolMethodProcessor implements ProtocolMethodProcessor {
    private static final String MM_TENANT = "_mm";
    private static final String SYSTEM_FABRIC = "_system";
    private static final String KMS_COLLECTION_NAME = "_kmsKeys";
    private static final ValidationKeyCache validationKeyCache;
    private final static TimeoutConfigCache timeoutConfigCache;


    static {
        try {
            validationKeyCache = new ValidationKeyCache();
            timeoutConfigCache = new TimeoutConfigCache();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected final ChannelHandlerContext ctx;
    @Getter
    protected final Channel channel;

    protected final MQTTAuthenticationService authenticationService;

    private final boolean authenticationEnabled;

    public AbstractCommonProtocolMethodProcessor(MQTTAuthenticationService authenticationService,
                                                 boolean authenticationEnabled,
                                                 ChannelHandlerContext ctx) {
        this.authenticationService = authenticationService;
        this.authenticationEnabled = authenticationEnabled;
        this.ctx = ctx;
        this.channel = ctx.channel();
    }

    public abstract void doProcessConnect(MqttAdapterMessage msg, String userRole, ClientRestrictions restrictions);

    @Override
    public void processConnect(MqttAdapterMessage adapter) {
        MqttConnectMessage msg = (MqttConnectMessage) adapter.getMqttMessage();
        MqttConnectPayload payload = msg.payload();
        MqttConnectMessage connectMessage = msg;
        final int protocolVersion = msg.variableHeader().version();
        final String username = payload.userName();
        String clientId = payload.clientIdentifier();
        MqttConnectVariableHeader variableHeader = connectMessage.variableHeader();
        if (variableHeader.hasPassword() && !variableHeader.hasUserName()) {
            connectMessage = MqttMessageUtils.cloneMqttConnectMessageWithUserNameFlag(connectMessage);
            variableHeader = connectMessage.variableHeader();
            if (log.isDebugEnabled()) {
                log.debug("Proxy CONNECT message. Consists only password, username also should be present. " +
                        "Set hasUserName = true CId={}, username={}.",
                    clientId, username);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("[CONNECT] process CONNECT message. CId={}, username={}", clientId, username);
        }
        // Check MQTT protocol version.
        if (!MqttUtils.isSupportedVersion(protocolVersion)) {
            log.error("[CONNECT] MQTT protocol version is not valid. CId={}", clientId);
            MqttMessage mqttMessage = MqttConnectAck.errorBuilder().unsupportedVersion();
            adapter.setMqttMessage(mqttMessage);
            channel.writeAndFlush(mqttMessage);
            if (!adapter.fromProxy()) {
                channel.close();
            }
            return;
        }
        if (!MqttUtils.isWillQosSupported(msg)) {
            MqttMessage mqttMessage = MqttConnectAck.errorBuilder().willQosNotSupport(protocolVersion);
            adapter.setMqttMessage(mqttMessage);
            channel.writeAndFlush(adapter);
            if (!adapter.fromProxy()) {
                channel.close();
            }
            return;
        }
        // Client must specify the client ID except enable clean session on the connection.
        if (StringUtils.isEmpty(clientId)) {
            if (!variableHeader.isCleanSession()) {
                MqttMessage mqttMessage = MqttConnectAck.errorBuilder().identifierInvalid(protocolVersion);
                log.error("[CONNECT] The MQTT client ID cannot be empty. Username={}", username);
                adapter.setMqttMessage(mqttMessage);
                channel.writeAndFlush(adapter);
                if (!adapter.fromProxy()) {
                    channel.close();
                }
                return;
            }
            clientId = MqttMessageUtils.createClientIdentifier(channel);
            connectMessage = MqttMessageUtils.stuffClientIdToConnectMessage(msg, clientId);
            if (log.isDebugEnabled()) {
                log.debug("[CONNECT] Client has connected with generated identifier. CId={}", clientId);
            }
        }
        String userRole = null;
        if (!authenticationEnabled) {
            if (log.isDebugEnabled()) {
                log.debug("[CONNECT] Authentication is disabled, allowing client. CId={}, username={}",
                        clientId, username);
            }
        } else {
            MQTTAuthenticationService.AuthenticationResult authResult = authenticationService
                    .authenticate(connectMessage);
            if (authResult.isFailed()) {
                MqttMessage mqttMessage = MqttConnectAck.errorBuilder().authFail(protocolVersion);
                log.error("[CONNECT] Invalid or incorrect authentication. CId={}, username={}", clientId, username);
                adapter.setMqttMessage(mqttMessage);
                channel.writeAndFlush(adapter);
                if (!adapter.fromProxy()) {
                    channel.close();
                }
                return;
            }
            userRole = authResult.getUserRole();
        }
        try {
            ClientRestrictions.ClientRestrictionsBuilder clientRestrictionsBuilder = ClientRestrictions.builder();
            MqttPropertyUtils.parsePropertiesToStuffRestriction(clientRestrictionsBuilder, msg);
            clientRestrictionsBuilder
                    .keepAliveTime(getKeepAliveTimeout(variableHeader, payload))
                    .cleanSession(variableHeader.isCleanSession());
            adapter.setMqttMessage(connectMessage);
            doProcessConnect(adapter, userRole, clientRestrictionsBuilder.build());
        } catch (InvalidReceiveMaximumException invalidReceiveMaximumException) {
            log.error("[CONNECT] Fail to parse receive maximum because of zero value, CId={}", clientId);
            MqttMessage mqttMessage = MqttConnectAck.errorBuilder().protocolError(protocolVersion);
            adapter.setMqttMessage(mqttMessage);
            channel.writeAndFlush(adapter);
            if (!adapter.fromProxy()) {
                channel.close();
            }
        }
    }

    private int getKeepAliveTimeout(MqttConnectVariableHeader variableHeader, MqttConnectPayload payload) {
        byte[] passwordBytes = payload.passwordInBytes();
        String tenant = null;

        if (passwordBytes != null) {
            tenant = extractTenant(new String(passwordBytes, CharsetUtil.UTF_8));
        }

        KeepAliveTimeoutConfig config = tenant != null ? timeoutConfigCache.get(tenant) : timeoutConfigCache.getDefault();
        return calculateKeepAliveTimeout(config, variableHeader.keepAliveTimeSeconds());
    }

    private static String extractTenant(String jwt) {
        return validationKeyCache.getTenantForJwt(jwt);
    }

    private static int calculateKeepAliveTimeout(KeepAliveTimeoutConfig config, int clientRequestedTimeout) {
        int timeoutSeconds = config.getTimeoutSeconds();
        int timeoutByRatio = Math.round(clientRequestedTimeout * config.getTimeoutRatio());
        return Math.max(timeoutSeconds, timeoutByRatio);
    }

    @Override
    public void processPubAck(MqttAdapterMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubAck] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processPubRel(MqttAdapterMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRel] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processPubRec(MqttAdapterMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRec] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processPubComp(MqttAdapterMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubComp] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processAuthReq(MqttAdapterMessage adapter) {
        if (log.isDebugEnabled()) {
            log.debug("[AUTH] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
        MqttMessage mqttMessage = adapter.getMqttMessage();
        MqttProperties properties = ((MqttReasonCodeAndPropertiesVariableHeader) mqttMessage.variableHeader())
                .properties();
        MqttProperties.StringProperty authMethodProperty = (MqttProperties.StringProperty) properties
                .getProperty(MqttProperties.MqttPropertyType.AUTHENTICATION_METHOD.value());
        MqttProperties.BinaryProperty authDataProperty = (MqttProperties.BinaryProperty) properties
                .getProperty(MqttProperties.MqttPropertyType.AUTHENTICATION_DATA.value());
        MQTTAuthenticationService.AuthenticationResult authResult = authenticationService.authenticate(
                adapter.getClientId(), authMethodProperty.value(),
                new AuthenticationDataCommand(new String(authDataProperty.value())));
        if (authResult.isFailed()) {
            log.error("[AUTH] auth failed. CId={}", adapter.getClientId());
            MqttMessage mqttAuthSFailure = MqttMessageBuilders.auth()
                    .properties(properties)
                    .reasonCode(Mqtt5DisConnReasonCode.CONTINUE_AUTHENTICATION.byteValue()).build();
            adapter.setMqttMessage(mqttAuthSFailure);
            channel.writeAndFlush(adapter).addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("send auth result failed", future.cause());
                }
            });
        } else {
            MqttMessage mqttAuthSuccess = MqttMessageBuilders.auth()
                    .properties(properties)
                    .reasonCode(Mqtt5DisConnReasonCode.NORMAL.byteValue()).build();
            adapter.setMqttMessage(mqttAuthSuccess);
            channel.writeAndFlush(adapter).addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("send auth result failed", future.cause());
                }
            });
        }
    }

    @Data
    @AllArgsConstructor
    private static class KeepAliveTimeoutConfig {
        private int timeoutSeconds;
        private float timeoutRatio;
    }

    private static class TimeoutConfigCache {
        private static final int DEFAULT_TIMEOUT_SECONDS = 90;
        private static final float DEFAULT_TIMEOUT_RATIO = 1.5f;
        private static final KeepAliveTimeoutConfig DEFAULT_CONFIG =
                new KeepAliveTimeoutConfig(DEFAULT_TIMEOUT_SECONDS, DEFAULT_TIMEOUT_RATIO);

        private final ConcurrentHashMap<String, KeepAliveTimeoutConfig> configCache = new ConcurrentHashMap<>();
        private final C8DB c8db;

        public TimeoutConfigCache() throws Exception {
            c8db = C8Retriever.any(() -> {
                C8DBCluster cluster = new C8DBCluster();
                return cluster.getC8DB();
            });
            log.info("C8DBCluster connected.");

            C8Retriever.any(() -> {
                Object timeouts = c8db.db(MM_TENANT, SYSTEM_FABRIC).query(
                        "FOR doc in @@collection FILTER doc._key=='streamsMqttKeepAliveTimeoutSeconds' RETURN doc",
                        ImmutableMap.of("@collection", KMS_COLLECTION_NAME),
                        Object.class).first();

                if (timeouts != null) {
                    configCache.putAll((Map<? extends String, ? extends KeepAliveTimeoutConfig>) timeouts);
                }
                return null;
            });
        }
        public KeepAliveTimeoutConfig getDefault() {
            return DEFAULT_CONFIG;
        }

        public KeepAliveTimeoutConfig get(String tenant) {
            return configCache.getOrDefault(tenant, getDefault());
        }
    }

    private static class ValidationKeyCache {
        private final CopyOnWriteArrayList<ValidationKeyInfo> validationKeyInfo = new CopyOnWriteArrayList<>();
        private final C8DB c8db;

        public ValidationKeyCache() throws Exception {
            c8db = C8Retriever.any(() -> {
                C8DBCluster cluster = new C8DBCluster();
                return cluster.getC8DB();
            });
            log.info("C8DBCluster connected.");

            C8Retriever.any(() -> {
                validationKeyInfo.addAll(c8db.db(MM_TENANT, SYSTEM_FABRIC).query(
                        "FOR doc in @@collection FILTER doc.enabled==true AND doc.service=='CUSTOMER_JWT' RETURN doc.dataKey",
                        ImmutableMap.of("@collection", KMS_COLLECTION_NAME),
                        ValidationKeyInfo.class).asListRemaining());
                return null;
            });
        }
        public String getTenantForJwt(String jwt) {
            ValidationKeyInfo keyInfo = null;
            for (ValidationKeyInfo info : validationKeyInfo) {
                String dataKey = info.dataKey;
                if (dataKey != null) {
                    try {
                        Jwts.parserBuilder().setSigningKey(dataKey).build().parse(jwt);
                        keyInfo = info;
                        break;
                    } catch (Exception e) {
                        // Ignore
                    }
                }
            }

            if (keyInfo != null) {
                return keyInfo.tenant;
            } else {
                return null;
            }
        }

        @Data
        public static class ValidationKeyInfo {
            private String tenant;
            private String dataKey;
        }
    }
}
