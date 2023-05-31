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
import com.c8db.model.CollectionCreateOptions;
import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
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
import io.streamnative.pulsar.handlers.mqtt.utils.TimeoutConfigCache;
import io.streamnative.pulsar.handlers.mqtt.utils.ValidationKeyCache;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.data.Json;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.c8db.C8DBCluster;
import org.apache.pulsar.broker.c8streams.CollectionChangeListener;

import java.security.Key;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Common protocol method processor.
 */
@Slf4j
public abstract class AbstractCommonProtocolMethodProcessor implements ProtocolMethodProcessor {
    private static final ValidationKeyCache validationKeyCache;
    private final static TimeoutConfigCache timeoutConfigCache;


    static {
        try {
            validationKeyCache = new ValidationKeyCache();
        } catch (Exception e) {
            log.error("Could not initialize ValidationKeyCache.", e);
            throw new RuntimeException(e);
        }

        try {
            timeoutConfigCache = new TimeoutConfigCache();
        } catch (Exception e) {
            log.error("Could not initialize TimeoutConfigCache.", e);
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

        TimeoutConfigCache.KeepAliveTimeoutConfig config = !StringUtils.isBlank(tenant) ?
                timeoutConfigCache.get(tenant) : timeoutConfigCache.getDefault();
        return calculateKeepAliveTimeout(config, variableHeader.keepAliveTimeSeconds());
    }

    private static String extractTenant(String token) {
        String[] chunks = token.split("\\.");

        if (chunks.length != 3) {
            return null;
        }

        Base64.Decoder decoder = Base64.getUrlDecoder();
        Map<String, Object> header = (Map<String, Object>) Json.parseJson(new String(decoder.decode(chunks[0])));
        Map<String, Object> payload = (Map<String, Object>) Json.parseJson(new String(decoder.decode(chunks[1])));

        String tenant = (String) header.get("tenant");

        if (StringUtils.isBlank(tenant)) {
            log.debug("'tenant' is not available in JWT header.");
            tenant = (String) payload.get("tenant");
        }

        if (StringUtils.isBlank(tenant)) {
            log.debug("'tenant' is not available in JWT payload.");
            String kid = (String) header.get("kid");
            if (StringUtils.isBlank(kid)) {
                log.debug("'kid' is not available in JWT payload.");
                tenant = validationKeyCache.getTenantForJwt(token, (String) header.get("alg"));
            } else {
                tenant = validationKeyCache.getTenantForKid(kid);
            }
        }

        log.debug("'tenant' for the JWT is '{}'.", tenant);

        return tenant;
    }

    private static int calculateKeepAliveTimeout(TimeoutConfigCache.KeepAliveTimeoutConfig config, int clientRequestedTimeout) {
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
}
