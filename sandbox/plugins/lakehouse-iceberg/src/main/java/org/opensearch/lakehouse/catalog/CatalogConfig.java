/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.lakehouse.LakehouseSettings;

import java.util.Objects;

/**
 * Immutable configuration for an Iceberg catalog connection,
 * built from OpenSearch index settings.
 */
public final class CatalogConfig {

    private final String indexName;
    private final CatalogType catalogType;
    private final String region;
    private final String warehouse;
    private final String namespace;
    private final String tableName;
    private final String authType;
    private final String roleArn;
    private final String credentialKey;

    /**
     * Creates a catalog configuration.
     *
     * @param indexName     the OpenSearch index name
     * @param catalogType   type of catalog backend
     * @param region        AWS region
     * @param warehouse     warehouse location (s3:// or file://)
     * @param namespace     Iceberg namespace
     * @param tableName     Iceberg table name
     * @param authType      authentication type: role, keys, or default
     * @param roleArn       IAM role ARN (for auth_type=role)
     * @param credentialKey keystore credential name (for auth_type=keys)
     */
    public CatalogConfig(
        String indexName,
        CatalogType catalogType,
        String region,
        String warehouse,
        String namespace,
        String tableName,
        String authType,
        String roleArn,
        String credentialKey
    ) {
        this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
        this.catalogType = Objects.requireNonNull(catalogType, "catalogType must not be null");
        this.warehouse = Objects.requireNonNull(warehouse, "warehouse must not be null");
        this.namespace = Objects.requireNonNull(namespace, "namespace must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.region = region;
        this.authType = (authType != null && !authType.isEmpty()) ? authType : "default";
        this.roleArn = roleArn;
        this.credentialKey = credentialKey;
    }

    /**
     * Builds a CatalogConfig from an OpenSearch index's settings.
     *
     * @param indexMetadata the index metadata containing lakehouse settings
     * @return a new CatalogConfig
     * @throws IllegalArgumentException if required settings are missing
     */
    public static CatalogConfig fromIndexSettings(IndexMetadata indexMetadata) {
        Settings settings = indexMetadata.getSettings();
        String indexName = indexMetadata.getIndex().getName();

        String typeStr = LakehouseSettings.INDEX_LAKEHOUSE_TYPE.get(settings);
        if (typeStr == null || typeStr.isEmpty()) {
            throw new IllegalArgumentException("index.lakehouse.type is required for index [" + indexName + "]");
        }
        String warehouse = LakehouseSettings.INDEX_LAKEHOUSE_WAREHOUSE.get(settings);
        if (warehouse == null || warehouse.isEmpty()) {
            throw new IllegalArgumentException("index.lakehouse.warehouse is required for index [" + indexName + "]");
        }
        String namespace = LakehouseSettings.INDEX_LAKEHOUSE_NAMESPACE.get(settings);
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("index.lakehouse.namespace is required for index [" + indexName + "]");
        }
        String table = LakehouseSettings.INDEX_LAKEHOUSE_TABLE.get(settings);
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("index.lakehouse.table is required for index [" + indexName + "]");
        }

        return new CatalogConfig(
            indexName,
            CatalogType.fromString(typeStr),
            LakehouseSettings.INDEX_LAKEHOUSE_REGION.get(settings),
            warehouse,
            namespace,
            table,
            LakehouseSettings.INDEX_LAKEHOUSE_AUTH_TYPE.get(settings),
            LakehouseSettings.INDEX_LAKEHOUSE_ROLE_ARN.get(settings),
            LakehouseSettings.INDEX_LAKEHOUSE_CREDENTIAL_KEY.get(settings)
        );
    }

    /** Returns the OpenSearch index name. */
    public String indexName() {
        return indexName;
    }

    /** Returns the catalog backend type. */
    public CatalogType catalogType() {
        return catalogType;
    }

    /** Returns the AWS region, or null. */
    public String region() {
        return region;
    }

    /** Returns the warehouse location. */
    public String warehouse() {
        return warehouse;
    }

    /** Returns the Iceberg namespace. */
    public String namespace() {
        return namespace;
    }

    /** Returns the Iceberg table name. */
    public String tableName() {
        return tableName;
    }

    /** Returns the authentication type. */
    public String authType() {
        return authType;
    }

    /** Returns the IAM role ARN, or null. */
    public String roleArn() {
        return roleArn;
    }

    /** Returns the keystore credential key, or null. */
    public String credentialKey() {
        return credentialKey;
    }
}
