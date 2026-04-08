/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Iceberg catalog connections and loads table metadata.
 * One catalog instance per index (simple approach — deduplication deferred).
 */
public class IcebergCatalogConnector {

    private static final Logger logger = LogManager.getLogger(IcebergCatalogConnector.class);

    /** Assumed lifetime for STS session credentials. */
    private static final long STS_CREDENTIAL_LIFETIME_MS = 10 * 60 * 1000L;

    /** Buffer before actual expiry to trigger early refresh. */
    private static final long REFRESH_BUFFER_MS = 10_000L;

    private final ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AwsCredentials> credentials = new ConcurrentHashMap<>();

    /** Creates a new catalog connector. */
    public IcebergCatalogConnector() {}

    /**
     * Loads an Iceberg table using the given catalog config. Creates and caches
     * the underlying Iceberg Catalog if not already present.
     *
     * @param config the catalog configuration built from index settings
     * @return the loaded Iceberg {@link Table}
     */
    @SuppressWarnings("removal")
    public Table loadTable(CatalogConfig config) {
        String catalogKey = config.indexName();
        Catalog catalog = catalogs.computeIfAbsent(catalogKey, k -> buildCatalog(config));

        TableIdentifier tableId = TableIdentifier.of(config.namespace(), config.tableName());
        logger.info("[CatalogConnector] Loading table [{}] for index [{}]", tableId, config.indexName());

        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        try {
            setCredentialsOnThread(config);
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            Table table = AccessController.doPrivileged((PrivilegedAction<Table>) () -> catalog.loadTable(tableId));
            logger.info(
                "[CatalogConnector] Loaded table [{}]: location=[{}], snapshot=[{}]",
                tableId,
                table.location(),
                table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : "none"
            );
            return table;
        } finally {
            LakehouseCredentialsProvider.clear();
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    /**
     * Returns cached credentials for a config, resolving fresh ones if expired.
     *
     * @param config the catalog configuration
     * @return resolved credentials, or null
     */
    public AwsCredentials getCredentials(CatalogConfig config) {
        return getFreshCredentials(config);
    }

    @SuppressWarnings("removal")
    private Catalog buildCatalog(CatalogConfig config) {
        logger.info(
            "[CatalogConnector] Building catalog for index [{}] type=[{}] region=[{}] warehouse=[{}]",
            config.indexName(),
            config.catalogType(),
            config.region(),
            config.warehouse()
        );

        AwsCredentials creds = resolveCredentials(config);
        if (creds != null && creds.isComplete()) {
            credentials.put(config.indexName(), creds);
        }

        Map<String, String> properties = buildCatalogProperties(config);

        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        try {
            setCredentialsOnThread(config);
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            return AccessController.doPrivileged(
                (PrivilegedAction<Catalog>) () -> CatalogUtil.buildIcebergCatalog(config.indexName(), properties, new Configuration())
            );
        } finally {
            LakehouseCredentialsProvider.clear();
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    private void setCredentialsOnThread(CatalogConfig config) {
        AwsCredentials creds = getFreshCredentials(config);
        if (creds != null && creds.isComplete()) {
            LakehouseCredentialsProvider.set(creds);
        }
    }

    private AwsCredentials getFreshCredentials(CatalogConfig config) {
        String key = config.indexName();
        AwsCredentials creds = credentials.get(key);
        if (creds != null && !creds.isExpired()) {
            return creds;
        }
        // ConcurrentHashMap.compute() locks per-bucket, so only the same index
        // blocks — unrelated indices can refresh concurrently.
        return credentials.compute(key, (k, existing) -> {
            if (existing != null && !existing.isExpired()) {
                return existing;
            }
            AwsCredentials fresh = resolveCredentials(config);
            if (fresh != null && fresh.isComplete()) {
                logger.info("[CatalogConnector] Refreshed credentials for index [{}]", key);
                return fresh;
            }
            return existing;
        });
    }

    @SuppressWarnings("removal")
    private AwsCredentials resolveCredentials(CatalogConfig config) {
        String authType = config.authType();
        return AccessController.doPrivileged((PrivilegedAction<AwsCredentials>) () -> {
            try {
                if ("role".equals(authType)) {
                    return resolveRoleCredentials(config);
                } else if ("keys".equals(authType)) {
                    return resolveKeystoreCredentials(config);
                } else {
                    return resolveDefaultCredentials();
                }
            } catch (Exception e) {
                logger.warn("[CatalogConnector] Credential resolution failed for index [{}]: {}", config.indexName(), e.getMessage());
                return null;
            }
        });
    }

    private AwsCredentials resolveRoleCredentials(CatalogConfig config) {
        String roleArn = config.roleArn();
        if (roleArn == null || roleArn.isEmpty()) {
            throw new IllegalArgumentException("role_arn is required when auth_type=role for index [" + config.indexName() + "]");
        }
        StsClient stsClient = StsClient.builder().region(config.region() != null ? Region.of(config.region()) : Region.US_EAST_1).build();
        try {
            AssumeRoleResponse response = stsClient.assumeRole(
                AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName("lakehouse-" + config.indexName()).build()
            );
            Credentials stsCreds = response.credentials();
            long expiry = stsCreds.expiration().toEpochMilli() - REFRESH_BUFFER_MS;
            return new AwsCredentials(stsCreds.accessKeyId(), stsCreds.secretAccessKey(), stsCreds.sessionToken(), expiry);
        } finally {
            stsClient.close();
        }
    }

    private AwsCredentials resolveKeystoreCredentials(CatalogConfig config) {
        // Keystore credentials are resolved at query time by the plugin
        // via Settings. For now, fall back to default credentials chain.
        // Full keystore integration comes when contributeSchema passes Settings.
        logger.info("[CatalogConnector] auth_type=keys for index [{}] — keystore integration pending", config.indexName());
        return resolveDefaultCredentials();
    }

    private AwsCredentials resolveDefaultCredentials() {
        software.amazon.awssdk.auth.credentials.AwsCredentials sdkCreds = DefaultCredentialsProvider.create().resolveCredentials();
        String sessionToken = null;
        long expiryTimestamp = 0;
        if (sdkCreds instanceof AwsSessionCredentials) {
            sessionToken = ((AwsSessionCredentials) sdkCreds).sessionToken();
            expiryTimestamp = System.currentTimeMillis() + STS_CREDENTIAL_LIFETIME_MS - REFRESH_BUFFER_MS;
        }
        return new AwsCredentials(sdkCreds.accessKeyId(), sdkCreds.secretAccessKey(), sessionToken, expiryTimestamp);
    }

    private Map<String, String> buildCatalogProperties(CatalogConfig config) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, config.warehouse());
        props.put(CatalogUtil.ICEBERG_CATALOG_TYPE, config.catalogType().name().toLowerCase(Locale.ROOT));

        if (config.region() != null) {
            props.put("client.region", config.region());
        }

        props.put("client.credentials-provider", "org.opensearch.lakehouse.catalog.LakehouseCredentialsProvider");

        return props;
    }
}
