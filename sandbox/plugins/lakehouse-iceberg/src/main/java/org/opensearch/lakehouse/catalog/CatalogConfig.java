/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for an Iceberg catalog connection.
 */
public final class CatalogConfig {

    private final String catalogName;
    private final CatalogType catalogType;
    private final String uri;
    private final String warehouse;
    private final String region;
    private final String database;
    private final String credentialProvider;
    private final Duration refreshInterval;

    /**
     * Creates a catalog configuration.
     *
     * @param catalogName        logical name of the catalog
     * @param catalogType        type of catalog backend (GLUE, HIVE, REST)
     * @param uri                catalog URI (e.g. Glue endpoint, Hive metastore URI)
     * @param warehouse          S3 warehouse location (must start with {@code s3://})
     * @param region             AWS region for Glue / S3
     * @param database           default database to use
     * @param credentialProvider credential strategy: "default", "explicit", or "sts_role"
     * @param refreshInterval    how often to refresh catalog metadata
     */
    public CatalogConfig(
        String catalogName,
        CatalogType catalogType,
        String uri,
        String warehouse,
        String region,
        String database,
        String credentialProvider,
        Duration refreshInterval
    ) {
        Objects.requireNonNull(catalogName, "catalogName must not be null");
        Objects.requireNonNull(catalogType, "catalogType must not be null");
        Objects.requireNonNull(warehouse, "warehouse must not be null");
        Objects.requireNonNull(credentialProvider, "credentialProvider must not be null");
        this.catalogName = catalogName;
        this.catalogType = catalogType;
        this.uri = uri;
        this.warehouse = warehouse;
        this.region = region;
        this.database = database;
        this.credentialProvider = credentialProvider;
        this.refreshInterval = refreshInterval != null ? refreshInterval : Duration.ofMinutes(5);
    }

    /** Returns the logical catalog name. */
    public String catalogName() {
        return catalogName;
    }

    /** Returns the catalog backend type. */
    public CatalogType catalogType() {
        return catalogType;
    }

    /** Returns the catalog URI. */
    public String uri() {
        return uri;
    }

    /** Returns the S3 warehouse location. */
    public String warehouse() {
        return warehouse;
    }

    /** Returns the AWS region. */
    public String region() {
        return region;
    }

    /** Returns the default database name. */
    public String database() {
        return database;
    }

    /** Returns the credential provider strategy. */
    public String credentialProvider() {
        return credentialProvider;
    }

    /** Returns the metadata refresh interval. */
    public Duration refreshInterval() {
        return refreshInterval;
    }
}
