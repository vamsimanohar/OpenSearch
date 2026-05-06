/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Assembles the storage configuration map and normalizes file paths
 * for the DataFusion execution backend.
 * <p>
 * Extracted from {@link LakehouseQueryExecutor} to separate config assembly
 * concerns from query execution logic.
 *
 * @opensearch.internal
 */
public final class StorageConfigBuilder {

    private StorageConfigBuilder() {}

    /**
     * Builds a storage configuration map from the catalog connector, Iceberg table, and scan plan.
     * <p>
     * The returned map contains:
     * <ul>
     *   <li>{@code s3Region} — the AWS region from catalog config (if set)</li>
     *   <li>{@code s3Bucket} — extracted from the first S3 file path (if S3)</li>
     *   <li>{@code localMode} — "true" if paths are local files</li>
     *   <li>{@code indexName} — the OpenSearch index name from catalog config</li>
     *   <li>{@code authType} — the authentication type from catalog config</li>
     * </ul>
     *
     * @param icebergTable the Iceberg Calcite table
     * @param scanPlan     the scan plan with pruned file paths
     * @return the storage configuration map
     */
    public static Map<String, String> buildStorageConfig(IcebergCalciteTable icebergTable, IcebergScanPlan scanPlan) {
        Map<String, String> config = new HashMap<>();
        CatalogConfig catalogConfig = icebergTable.catalogConfig();
        if (catalogConfig != null && catalogConfig.region() != null) {
            config.put("s3Region", catalogConfig.region());
        }
        List<String> paths = scanPlan.getDataFilePaths();
        if (!paths.isEmpty()) {
            String firstPath = paths.get(0);
            if (firstPath.startsWith("s3://")) {
                String withoutScheme = firstPath.substring(5);
                int slashIdx = withoutScheme.indexOf('/');
                if (slashIdx > 0) {
                    config.put("s3Bucket", withoutScheme.substring(0, slashIdx));
                }
            }
            if (firstPath.startsWith("file:") || firstPath.startsWith("/")) {
                config.put("localMode", "true");
            }
        }
        if (catalogConfig != null) {
            config.put("indexName", catalogConfig.indexName());
            config.put("authType", catalogConfig.authType());
        }
        return config;
    }

    /**
     * Normalizes file paths to ensure consistent URI format for DataFusion.
     * <ul>
     *   <li>{@code file:/path} becomes {@code file:///path}</li>
     *   <li>{@code /path} becomes {@code file:///path}</li>
     *   <li>S3 paths are left unchanged</li>
     * </ul>
     *
     * @param paths the raw file paths from the Iceberg scan plan
     * @return the normalized file paths
     */
    public static List<String> normalizeFilePaths(List<String> paths) {
        return paths.stream()
            .map(p -> {
                if (p.startsWith("file:/") && !p.startsWith("file://")) {
                    return "file://" + p.substring("file:".length());
                } else if (p.startsWith("/")) {
                    return "file://" + p;
                }
                return p;
            })
            .toList();
    }
}
