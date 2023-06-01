/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.utils;

import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.data.Json;
import org.apache.commons.lang3.StringUtils;

import java.util.Base64;
import java.util.Map;

@Slf4j
public class TokenUtils {

    public static int getServerKeepAliveTimeoutSeconds(TimeoutConfigCache timeoutConfigCache,
                                                       int clientKeepAliveTimeoutSeconds, String tenantFabric) {
        TimeoutConfigCache.KeepAliveTimeoutConfig config = !StringUtils.isBlank(tenantFabric) ?
                timeoutConfigCache.get(tenantFabric) : timeoutConfigCache.getDefault();
        return calculateKeepAliveTimeout(config, clientKeepAliveTimeoutSeconds);
    }

    public static String extractTenantFabric(ValidationKeyCache validationKeyCache, MqttConnectPayload mqttPayload) {
        byte[] passwordBytes = mqttPayload.passwordInBytes();

        if (passwordBytes == null) {
            return null;
        }

        String token = new String(passwordBytes, CharsetUtil.UTF_8);

        String[] chunks = token.split("\\.");

        if (chunks.length != 3) {
            // Invalid JWT
            return null;
        }

        Base64.Decoder decoder = Base64.getUrlDecoder();
        Map<String, Object> header = (Map<String, Object>) Json.parseJson(new String(decoder.decode(chunks[0])));

        String kid = (String) header.get("kid");
        String alg = (String) header.get("alg");
        String tenantFabric;

        log.debug("Extracting tenant.fabric. kid={}", kid);

        if (StringUtils.isBlank(kid)) {
            tenantFabric = validationKeyCache.getTenantFabricForJwt(token, alg);
        } else {
            tenantFabric = validationKeyCache.getTenantFabricForKid(kid, token, alg);
        }

        log.debug("'tenant.fabric' for the JWT is '{}'.", tenantFabric);

        return tenantFabric;
    }

    private static int calculateKeepAliveTimeout(TimeoutConfigCache.KeepAliveTimeoutConfig config, int clientRequestedTimeout) {
        int timeoutSeconds = config.getTimeoutSeconds();
        int timeoutByRatio = Math.round(clientRequestedTimeout * config.getTimeoutRatio());
        return Math.max(timeoutSeconds, timeoutByRatio);
    }
}
