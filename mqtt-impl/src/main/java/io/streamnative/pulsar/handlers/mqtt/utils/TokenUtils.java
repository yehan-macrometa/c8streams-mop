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
                                                       int clientKeepAliveTimeoutSeconds, String tenant) {
        TimeoutConfigCache.KeepAliveTimeoutConfig config = !StringUtils.isBlank(tenant) ?
                timeoutConfigCache.get(tenant) : timeoutConfigCache.getDefault();
        return calculateKeepAliveTimeout(config, clientKeepAliveTimeoutSeconds);
    }

    public static String extractTenant(ValidationKeyCache validationKeyCache, MqttConnectPayload mqttPayload) {
        byte[] passwordBytes = mqttPayload.passwordInBytes();

        if (passwordBytes == null) {
            return null;
        }

        String token = new String(passwordBytes, CharsetUtil.UTF_8);

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
}
