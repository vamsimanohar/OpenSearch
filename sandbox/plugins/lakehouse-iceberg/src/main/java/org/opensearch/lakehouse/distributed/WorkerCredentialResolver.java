/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.lakehouse.catalog.AwsCredentials;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves AWS credentials on a worker node for distributed query execution.
 * <p>
 * The coordinator sends only the index name (no secrets) in the storage config.
 * Each worker independently resolves credentials via IMDS/STS/DefaultCredentialsProvider
 * by reading the catalog configuration from cluster state.
 * <p>
 * Extracted from {@link WorkerQueryExecutor} to follow single-responsibility principle.
 *
 * @opensearch.internal
 */
public final class WorkerCredentialResolver {

    private static final Logger logger = LogManager.getLogger(WorkerCredentialResolver.class);

    private WorkerCredentialResolver() {}

    /**
     * Resolves AWS credentials locally on this worker node using the index settings
     * from cluster state. The coordinator passes only the index name (no secrets);
     * each worker independently calls IMDS/STS/DefaultCredentialsProvider.
     * <p>
     * Uses {@link LakehouseState#instance()} for the catalog connector when
     * role/keys auth is required. For default auth or local mode, the connector
     * is never accessed.
     *
     * @param original       the storageConfig from the coordinator (contains region, bucket, indexName)
     * @param clusterService the cluster service for reading index metadata
     * @return a new map with credentials added
     */
    public static Map<String, String> resolve(Map<String, String> original, ClusterService clusterService) {
        return resolveWithConnector(original, clusterService, LakehouseState.instance().catalogConnector());
    }

    /**
     * Resolves AWS credentials locally on this worker node using an explicit
     * catalog connector. Package-private to allow unit testing without the
     * {@link LakehouseState} singleton.
     *
     * @param original       the storageConfig from the coordinator (contains region, bucket, indexName)
     * @param clusterService the cluster service for reading index metadata
     * @param connector      the catalog connector for credential resolution
     * @return a new map with credentials added
     */
    @SuppressWarnings("removal")
    static Map<String, String> resolveWithConnector(
        Map<String, String> original,
        ClusterService clusterService,
        IcebergCatalogConnector connector
    ) {
        Map<String, String> config = new HashMap<>(original);
        String indexName = config.remove("indexName");
        if (indexName == null || "true".equals(config.get("localMode"))) {
            return config;
        }

        // For "default" auth, Rust's object_store uses IMDS directly on each worker.
        String authType = config.getOrDefault("authType", "default");
        if ("default".equals(authType)) {
            logger.debug("[WorkerQuery] auth_type=default for index [{}], Rust will use IMDS directly", indexName);
            return config;
        }

        // For "role" and "keys" auth, resolve credentials locally from cluster state.
        try {
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata == null) {
                logger.warn("[WorkerQuery] Index [{}] not found in cluster state, skipping credential resolution", indexName);
                return config;
            }
            CatalogConfig catalogConfig = CatalogConfig.fromIndexSettings(indexMetadata);
            AwsCredentials creds = AccessController.doPrivileged(
                (PrivilegedAction<AwsCredentials>) () -> connector.getCredentials(catalogConfig)
            );
            if (creds != null && creds.isComplete()) {
                config.put("s3AccessKeyId", creds.getAccessKeyId());
                config.put("s3SecretAccessKey", creds.getSecretAccessKey());
                if (creds.getSessionToken() != null) {
                    config.put("s3SessionToken", creds.getSessionToken());
                }
            }
        } catch (Exception e) {
            logger.warn("[WorkerQuery] Local credential resolution failed for index [{}]: {}", indexName, e.getMessage());
        }
        return config;
    }
}
