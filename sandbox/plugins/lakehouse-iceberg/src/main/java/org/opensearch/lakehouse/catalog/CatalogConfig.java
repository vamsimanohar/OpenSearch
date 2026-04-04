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
public record CatalogConfig(
    String catalogName,
    CatalogType catalogType,
    String uri,
    String warehouse,
    String region,
    String database,
    String credentialProvider,
    Duration refreshInterval
) {
    public CatalogConfig {
        Objects.requireNonNull(catalogName, "catalogName must not be null");
        Objects.requireNonNull(catalogType, "catalogType must not be null");
        Objects.requireNonNull(warehouse, "warehouse must not be null");
        Objects.requireNonNull(credentialProvider, "credentialProvider must not be null");
        if (refreshInterval == null) {
            refreshInterval = Duration.ofMinutes(5);
        }
    }
}
