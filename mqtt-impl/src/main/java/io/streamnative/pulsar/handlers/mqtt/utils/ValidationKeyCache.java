/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.utils;

import co.macrometa.c8streams.api.util.C8Retriever;
import com.c8db.C8DB;
import com.c8db.model.CollectionCreateOptions;
import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.c8db.C8DBCluster;
import org.apache.pulsar.broker.c8streams.CollectionChangeListener;

import java.security.Key;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.streamnative.pulsar.handlers.mqtt.Constants.KMS_COLLECTION_NAME;
import static io.streamnative.pulsar.handlers.mqtt.Constants.MM_TENANT;
import static io.streamnative.pulsar.handlers.mqtt.Constants.SYSTEM_FABRIC;

@Slf4j
public class ValidationKeyCache {
    private final CopyOnWriteArrayList<ValidationKeyInfo> validationKeyInfo = new CopyOnWriteArrayList<>();
    private final C8DB c8db;

    public ValidationKeyCache() throws Exception {
        C8Retriever.any(() -> {
            new CollectionChangeListener()
                    .listen(MM_TENANT, SYSTEM_FABRIC, KMS_COLLECTION_NAME, (reader, msg) -> loadConfig());
            return null;
        });

        c8db = C8Retriever.any(() -> {
            C8DBCluster cluster = new C8DBCluster();
            cluster.init();
            return cluster.getC8DB();
        });
        log.info("C8DBCluster connected.");

        c8db.db(MM_TENANT, SYSTEM_FABRIC).createCollection(KMS_COLLECTION_NAME,
                new CollectionCreateOptions()
                        .isLocal(true)
                        .isSystem(true)
                        .stream(true)
                        .waitForSync(true)
        );

        loadConfig();
    }

    private void loadConfig() {
        if (c8db == null) {
            log.warn("Cannot load config. C8DB is not initialized.");
            return;
        }

        try {
            List<Object> keyObjects = c8db.db(MM_TENANT, SYSTEM_FABRIC).query(
                    "FOR doc in @@collection FILTER doc.enabled==true AND doc.service=='CUSTOMER_JWT' RETURN doc",
                    ImmutableMap.of("@collection", KMS_COLLECTION_NAME),
                    Object.class).asListRemaining();

            validationKeyInfo.clear();

            for (Object ko : keyObjects) {
                log.debug("Loading key config {}.", ko);

                try {
                    Map<String, Object> km = (Map<String, Object>) ko;
                    String tenant = (String) km.get("tenant");
                    String fabric = (String) km.get("fabric");
                    String dataKey = (String) km.get("dataKey");
                    String kid = (String) km.get("kid");
                    ValidationKeyInfo info = new ValidationKeyInfo(tenant, fabric, dataKey, kid);

                    log.debug("ValidationKeyInfo={}", info);

                    validationKeyInfo.add(info);
                } catch (Exception e) {
                    log.warn("Could not decode key config for {}. {}", ko, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Could not load config.", e);
        }
    }

    public String getTenantFabricForJwt(String jwt, String alg) {
        ValidationKeyInfo keyInfo = null;
        for (ValidationKeyInfo info : validationKeyInfo) {
            String dataKey = info.dataKey;
            if (dataKey != null) {
                try {
                    Key validationKey = alg.startsWith("HS") ?
                            Keys.hmacShaKeyFor(dataKey.getBytes()) :
                            AuthTokenUtils.decodePublicKey(
                                    Base64.getDecoder().decode(dataKey), SignatureAlgorithm.forName(alg));

                    Jwts.parserBuilder().setSigningKey(validationKey).build().parse(jwt);
                    keyInfo = info;
                    break;
                } catch (Exception e) {
                    // Ignore
                    log.debug("JWT validation failed with validation key info: {}", keyInfo);
                }
            }
        }

        if (keyInfo != null) {
            return keyInfo.getTenantFabric();
        } else {
            return null;
        }
    }

    public String getTenantFabricForKid(String kid) {
        for (ValidationKeyInfo info : validationKeyInfo) {
            if (kid.equals(info.kid)) {
                return info.getTenantFabric();
            }
        }

        return null;
    }

    @Data
    @AllArgsConstructor
    public static class ValidationKeyInfo {
        private String tenant;
        private String fabric;
        private String dataKey;
        private String kid;

        public String getTenantFabric() {
            return String.format("%s.%s", tenant, fabric);
        }
    }
}
