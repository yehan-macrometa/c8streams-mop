/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.utils;

import co.macrometa.c8streams.api.util.C8Retriever;
import com.c8db.C8DB;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.c8db.C8DBCluster;
import org.apache.pulsar.broker.c8streams.CollectionChangeListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.streamnative.pulsar.handlers.mqtt.Constants.C8FEDERATION_COLLECTION_NAME;
import static io.streamnative.pulsar.handlers.mqtt.Constants.MM_TENANT;
import static io.streamnative.pulsar.handlers.mqtt.Constants.SYSTEM_FABRIC;

@Slf4j
public class TimeoutConfigCache {
    private static final int DEFAULT_TIMEOUT_SECONDS = 90;
    private static final float DEFAULT_TIMEOUT_RATIO = 1.5f;
    private static final KeepAliveTimeoutConfig DEFAULT_CONFIG =
            new KeepAliveTimeoutConfig(DEFAULT_TIMEOUT_SECONDS, DEFAULT_TIMEOUT_RATIO);

    private final ConcurrentHashMap<String, KeepAliveTimeoutConfig> configCache = new ConcurrentHashMap<>();
    private final C8DB c8db;

    public TimeoutConfigCache() throws Exception {
        C8Retriever.any(() -> {
            new CollectionChangeListener()
                    .listen(MM_TENANT, SYSTEM_FABRIC, C8FEDERATION_COLLECTION_NAME, (reader, msg) -> loadConfig());
            return null;
        });

        c8db = C8Retriever.any(() -> {
            C8DBCluster cluster = new C8DBCluster();
            cluster.init();
            return cluster.getC8DB();
        });
        log.info("C8DBCluster connected.");

        loadConfig();
    }

    private void loadConfig() {
        if (c8db == null) {
            log.warn("Cannot load config. C8DB is not initialized.");
            return;
        }

        try {
            Object doc = c8db.db(MM_TENANT, SYSTEM_FABRIC).query(
                    "FOR doc in @@collection FILTER doc._key=='streamsMqttKeepAliveTimeout' RETURN doc",
                    ImmutableMap.of("@collection", C8FEDERATION_COLLECTION_NAME),
                    Object.class).first();

            if (doc != null) {
                log.debug("Got timeout configs from DB. {}", doc);

                Map<String, Object> configs = (Map<String, Object>) ((Map<String, Object>) doc).get("configs");

                if (configs != null) {
                    configCache.clear();

                    for (String tenant : configs.keySet()) {
                        log.debug("Loading timeout config for tenant {}.", tenant);

                        Map<String, Object> tenantConfigs = (Map<String, Object>) configs.get(tenant);

                        for (String fabric : tenantConfigs.keySet()) {
                            log.debug("Loading timeout config for fabric {}.{}.", tenant, fabric);

                            try {
                                Map<String, Object> configMap = (Map<String, Object>) tenantConfigs.get(fabric);
                                long timeoutSeconds = (long) configMap.get("timeoutSeconds");
                                double timeoutRatio = (double) configMap.get("timeoutRatio");
                                KeepAliveTimeoutConfig config =
                                        new KeepAliveTimeoutConfig((int) timeoutSeconds, (float) timeoutRatio);

                                log.debug("configMap={}, KeepAliveTimeoutConfig={}", configMap, config);

                                configCache.put(String.format("%s.%s", tenant, fabric), config);
                            } catch (Exception e) {
                                log.warn("Could not decode timeout config for {}.{}. {}", tenant, fabric, e.getMessage());
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Could not load config.", e);
        }
    }

    public KeepAliveTimeoutConfig getDefault() {
        return DEFAULT_CONFIG;
    }

    public KeepAliveTimeoutConfig get(String tenantFabric) {
        return configCache.getOrDefault(tenantFabric, getDefault());
    }

    @Data
    @AllArgsConstructor
    public static class KeepAliveTimeoutConfig {
        private int timeoutSeconds;
        private float timeoutRatio;
    }
}
