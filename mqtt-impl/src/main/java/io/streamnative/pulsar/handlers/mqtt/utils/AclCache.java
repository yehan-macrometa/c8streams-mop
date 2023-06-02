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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.streamnative.pulsar.handlers.mqtt.Constants.C8FEDERATION_COLLECTION_NAME;
import static io.streamnative.pulsar.handlers.mqtt.Constants.MM_TENANT;
import static io.streamnative.pulsar.handlers.mqtt.Constants.SYSTEM_FABRIC;

@Slf4j
public class AclCache {

    private final ConcurrentHashMap<String, Acl> cache = new ConcurrentHashMap<>();
    private final C8DB c8db;

    public AclCache() throws Exception {
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
            Acls aclDoc = c8db.db(MM_TENANT, SYSTEM_FABRIC).query(
                    "FOR doc in @@collection FILTER doc._key=='streamsMqttAcls' RETURN doc",
                    ImmutableMap.of("@collection", C8FEDERATION_COLLECTION_NAME),
                    Acls.class).first();

            if (aclDoc != null) {
                log.debug("Got ACLs from DB. {}", aclDoc);

                if (aclDoc.configs != null) {
                    cache.clear();

                    for (String tenant : aclDoc.configs.keySet()) {
                        log.debug("Loading ACLs for tenant {}.", tenant);

                        Map<String, Acl> tenantAcls = aclDoc.configs.get(tenant);
                        for (String fabric : tenantAcls.keySet()) {
                            log.debug("Loading ACLs for fabric {}.{}.", tenant, fabric);
                            cache.put(String.format("%s.%s", tenant, fabric), tenantAcls.get(fabric));
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Could not load ACLs.", e);
        }
    }

    public Acl get(String tenantFabric) {
        return cache.get(tenantFabric);
    }

    @Data
    public static class Acls {
        private Map<String, Map<String, Acl>> configs;
    }

    @Data
    @AllArgsConstructor
    public static class Acl {
        /**
         * The DB has the following structure:
         * {
         *   "filter": "/sb/in/+/data",
         *   "pubGroups": [
         *     "devicePub"
         *   ],
         *   "subGroups": [
         *     "adminSub"
         *   ]
         * }
         */

        private String filter;
        private List<String> pubGroups;
        private List<String> subGroups;
    }
}
