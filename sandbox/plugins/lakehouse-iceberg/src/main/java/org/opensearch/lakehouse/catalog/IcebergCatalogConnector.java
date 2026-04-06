/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Iceberg catalog connections. Phase 1 supports Glue only.
 */
public class IcebergCatalogConnector {

    private static final Logger logger = LogManager.getLogger(IcebergCatalogConnector.class);

    private static final String S3_PREFIX = "s3://";
    private static final String FILE_PREFIX = "file://";

    /** Assumed lifetime for STS session credentials (10 minutes for now). */
    private static final long STS_CREDENTIAL_LIFETIME_MS = 10 * 60 * 1000L;

    /** Buffer before actual expiry to trigger early refresh. */
    private static final long REFRESH_BUFFER_MS = 10_000L;

    private final ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CatalogConfig> configs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AwsCredentials> credentials = new ConcurrentHashMap<>();

    /** Creates a new catalog connector instance. */
    public IcebergCatalogConnector() {}

    /**
     * Registers a new Iceberg catalog connection.
     *
     * @param name   logical name for the catalog
     * @param config catalog configuration
     * @throws IllegalArgumentException          if the name is already registered or the warehouse URI is invalid
     * @throws UnsupportedOperationException     if the catalog type is not yet supported
     */
    public void registerCatalog(String name, CatalogConfig config) {
        if (catalogs.containsKey(name)) {
            throw new IllegalArgumentException("Catalog already registered: " + name);
        }

        validateWarehouseUri(config.warehouse());
        validateCatalogType(config.catalogType());

        logger.info("[CatalogConnector] Registering catalog [{}] type=[{}] region=[{}] warehouse=[{}]",
            name, config.catalogType(), config.region(), config.warehouse());

        // Resolve credentials for DataFusion's Rust S3 client (needs explicit creds via JNI).
        // Iceberg SDK uses DefaultCredentialsProvider directly for its own AWS calls.
        AwsCredentials creds = resolveCredentials(name);
        if (creds != null && creds.isComplete()) {
            credentials.put(name, creds);
            logger.info("[CatalogConnector] Credentials resolved for catalog [{}], sessionToken={}",
                name, creds.getSessionToken() != null ? "present" : "absent");
        } else {
            logger.warn("[CatalogConnector] No credentials resolved for catalog [{}] — DataFusion S3 reads may fail", name);
        }

        Map<String, String> properties = buildCatalogProperties(config);

        // CatalogUtil.buildIcebergCatalog uses DynConstructors which relies on
        // Thread.contextClassLoader. When called from an SPI-created extension instance,
        // the context classloader may not include Iceberg/Hadoop classes.
        // doPrivileged is needed because contributeSchema() is called from analytics-engine,
        // and the security manager checks ALL frames in the call stack. Without doPrivileged,
        // the analytics-engine ProtectionDomain (which lacks file/socket perms) blocks access.
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        setCredentialsOnThread(name);
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            logger.info("[CatalogConnector] Building Iceberg catalog [{}] with properties: {}",
                name, sanitizeProperties(properties));
            @SuppressWarnings("removal")
            Catalog catalog = AccessController.doPrivileged((PrivilegedAction<Catalog>) () ->
                CatalogUtil.buildIcebergCatalog(name, properties, new Configuration())
            );
            catalogs.put(name, catalog);
            configs.put(name, config);
            logger.info("[CatalogConnector] Successfully registered catalog [{}]", name);
        } catch (Exception e) {
            logger.error("[CatalogConnector] Failed to build catalog [{}]: {}", name, e.getMessage(), e);
            throw e;
        } finally {
            clearCredentialsOnThread();
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    /**
     * Loads an Iceberg table from a registered catalog.
     *
     * @param catalogName name of the registered catalog
     * @param tableId     Iceberg table identifier
     * @return the loaded Iceberg {@link Table}
     * @throws IllegalArgumentException if the catalog is not registered
     */
    @SuppressWarnings("removal")
    public Table loadTable(String catalogName, TableIdentifier tableId) {
        Catalog catalog = catalogs.get(catalogName);
        if (catalog == null) {
            throw new IllegalArgumentException("Catalog not registered: " + catalogName);
        }
        logger.info("[CatalogConnector] Loading table [{}] from catalog [{}]", tableId, catalogName);

        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        setCredentialsOnThread(catalogName);
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            Table table = AccessController.doPrivileged((PrivilegedAction<Table>) () ->
                catalog.loadTable(tableId)
            );
            logger.info("[CatalogConnector] Loaded table [{}] from catalog [{}]: location=[{}], snapshot=[{}]",
                tableId, catalogName, table.location(),
                table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : "none");
            return table;
        } catch (Exception e) {
            logger.error("[CatalogConnector] Failed to load table [{}] from catalog [{}]: {}",
                tableId, catalogName, e.getMessage(), e);
            throw e;
        } finally {
            clearCredentialsOnThread();
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    /**
     * Returns the set of registered catalog names.
     */
    public Set<String> listCatalogs() {
        return Collections.unmodifiableSet(catalogs.keySet());
    }

    /**
     * Returns the resolved AWS credentials for a specific catalog, or null if none.
     *
     * @param catalogName the registered catalog name
     * @return resolved credentials, or null
     */
    public AwsCredentials getCredentials(String catalogName) {
        return getFreshCredentials(catalogName);
    }

    /**
     * Returns the config for a specific catalog, or null if not registered.
     *
     * @param catalogName the registered catalog name
     * @return catalog config, or null
     */
    public CatalogConfig getCatalogConfig(String catalogName) {
        return configs.get(catalogName);
    }

    /**
     * Validates the warehouse URI. S3 URIs are required for production catalogs.
     * File URIs are allowed for HADOOP catalogs (local testing).
     */
    static void validateWarehouseUri(String warehouse) {
        if (warehouse == null) {
            throw new IllegalArgumentException("Warehouse URI must not be null");
        }
        String lower = warehouse.toLowerCase(Locale.ROOT);
        if (!lower.startsWith(S3_PREFIX) && !lower.startsWith(FILE_PREFIX)) {
            throw new IllegalArgumentException(
                "Warehouse URI must start with 's3://' or 'file://'. Received: " + warehouse
            );
        }
    }

    /**
     * Validates the catalog type. Phase 1 supports Glue, REST, and Hadoop.
     */
    static void validateCatalogType(CatalogType type) {
        if (type == CatalogType.HIVE) {
            throw new UnsupportedOperationException("HIVE catalog type is not supported in Phase 1");
        }
    }

    /**
     * Sets per-catalog credentials on the current thread's ThreadLocal so that
     * {@link LakehouseCredentialsProvider} can return them to the Iceberg SDK.
     * Must be paired with {@link #clearCredentialsOnThread()} in a finally block.
     *
     * @param catalogName the registered catalog name whose credentials to set
     */
    public void setCredentialsOnThread(String catalogName) {
        AwsCredentials creds = getFreshCredentials(catalogName);
        if (creds != null && creds.isComplete()) {
            LakehouseCredentialsProvider.set(creds);
        }
    }

    /**
     * Clears credentials from the current thread's ThreadLocal.
     */
    public void clearCredentialsOnThread() {
        LakehouseCredentialsProvider.clear();
    }

    /**
     * Resolves AWS credentials using the default credential chain.
     * Called inside doPrivileged so the security manager allows file/network access.
     */
    @SuppressWarnings("removal")
    private AwsCredentials resolveCredentials(String catalogName) {
        return AccessController.doPrivileged((PrivilegedAction<AwsCredentials>) () -> {
            try {
                software.amazon.awssdk.auth.credentials.AwsCredentials sdkCreds =
                    software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.create().resolveCredentials();
                String sessionToken = null;
                long expiryTimestamp = 0; // 0 = never expires (IAM user keys)
                if (sdkCreds instanceof software.amazon.awssdk.auth.credentials.AwsSessionCredentials) {
                    sessionToken = ((software.amazon.awssdk.auth.credentials.AwsSessionCredentials) sdkCreds).sessionToken();
                    // STS tokens expire; SDK v2 doesn't expose expiry directly,
                    // so use a conservative assumed lifetime
                    expiryTimestamp = System.currentTimeMillis() + STS_CREDENTIAL_LIFETIME_MS - REFRESH_BUFFER_MS;
                }
                return new AwsCredentials(sdkCreds.accessKeyId(), sdkCreds.secretAccessKey(), sessionToken, expiryTimestamp);
            } catch (Exception e) {
                logger.warn("[CatalogConnector] DefaultCredentialsProvider failed for catalog [{}]: {}", catalogName, e.getMessage());
                return null;
            }
        });
    }

    /**
     * Returns fresh credentials for a catalog, resolving new ones if the cached credentials are expired.
     *
     * @param catalogName the registered catalog name
     * @return fresh credentials, or null if resolution fails
     */
    private AwsCredentials getFreshCredentials(String catalogName) {
        AwsCredentials creds = credentials.get(catalogName);
        if (creds != null && !creds.isExpired()) {
            return creds;
        }
        synchronized (this) {
            // Double-check after acquiring lock — another thread may have refreshed
            creds = credentials.get(catalogName);
            if (creds != null && !creds.isExpired()) {
                return creds;
            }
            creds = resolveCredentials(catalogName);
            if (creds != null && creds.isComplete()) {
                credentials.put(catalogName, creds);
                logger.info("[CatalogConnector] Refreshed credentials for catalog [{}], sessionToken={}",
                    catalogName, creds.getSessionToken() != null ? "present" : "absent");
            } else {
                logger.warn("[CatalogConnector] Failed to refresh credentials for catalog [{}]", catalogName);
            }
            return creds;
        }
    }

    private Map<String, String> buildCatalogProperties(CatalogConfig config) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, config.warehouse());
        props.put(CatalogUtil.ICEBERG_CATALOG_TYPE, catalogTypeToIceberg(config.catalogType()));

        // Region for AWS service clients (Glue, S3)
        if (config.region() != null) {
            props.put("client.region", config.region());
        }

        // Iceberg SDK's internal AWS clients use our custom LakehouseCredentialsProvider,
        // which reads credentials from a ThreadLocal. The calling code sets per-catalog
        // credentials on the ThreadLocal before each SDK call and clears them after.
        // This avoids writing to JVM system properties (globally visible) and is
        // thread-safe for concurrent multi-catalog queries.
        props.put("client.credentials-provider",
            "org.opensearch.lakehouse.catalog.LakehouseCredentialsProvider");

        if (config.uri() != null) {
            props.put(CatalogProperties.URI, config.uri());
        }
        return props;
    }

    /**
     * Returns a sanitized copy of properties for logging (redacts credential-related values).
     */
    private Map<String, String> sanitizeProperties(Map<String, String> props) {
        Map<String, String> safe = new HashMap<>(props);
        safe.remove("client.credentials-provider"); // not secret, but verbose
        return safe;
    }

    private String catalogTypeToIceberg(CatalogType type) {
        switch (type) {
            case GLUE:
                return CatalogUtil.ICEBERG_CATALOG_TYPE_GLUE;
            case HADOOP:
                return CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP;
            case REST:
                return CatalogUtil.ICEBERG_CATALOG_TYPE_REST;
            default:
                throw new UnsupportedOperationException("Unsupported catalog type: " + type);
        }
    }
}
