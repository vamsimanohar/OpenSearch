/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.CatalogType;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageConfigBuilderTests extends OpenSearchTestCase {

    // --- buildStorageConfig tests ---

    public void testBuildStorageConfigWithS3Paths() {
        IcebergCalciteTable table = mockIcebergTable("us-west-2", "my-index", "default");
        IcebergScanPlan scanPlan = makeScanPlan(List.of(
            new IcebergScanPlan.FileInfo("s3://my-bucket/data/file1.parquet", 1024),
            new IcebergScanPlan.FileInfo("s3://my-bucket/data/file2.parquet", 2048)
        ));

        Map<String, String> config = StorageConfigBuilder.buildStorageConfig(table, scanPlan);

        assertEquals("us-west-2", config.get("s3Region"));
        assertEquals("my-bucket", config.get("s3Bucket"));
        assertEquals("my-index", config.get("indexName"));
        assertEquals("default", config.get("authType"));
        assertNull(config.get("localMode"));
    }

    public void testBuildStorageConfigWithLocalFileColonPaths() {
        IcebergCalciteTable table = mockIcebergTable(null, "local-index", "default");
        IcebergScanPlan scanPlan = makeScanPlan(List.of(
            new IcebergScanPlan.FileInfo("file:/tmp/data/file1.parquet", 512)
        ));

        Map<String, String> config = StorageConfigBuilder.buildStorageConfig(table, scanPlan);

        assertEquals("true", config.get("localMode"));
        assertNull(config.get("s3Region"));
        assertNull(config.get("s3Bucket"));
    }

    public void testBuildStorageConfigWithAbsoluteLocalPaths() {
        IcebergCalciteTable table = mockIcebergTable(null, "abs-index", "default");
        IcebergScanPlan scanPlan = makeScanPlan(List.of(
            new IcebergScanPlan.FileInfo("/tmp/data/file1.parquet", 256)
        ));

        Map<String, String> config = StorageConfigBuilder.buildStorageConfig(table, scanPlan);

        assertEquals("true", config.get("localMode"));
        assertNull(config.get("s3Bucket"));
    }

    public void testBuildStorageConfigWithEmptyPaths() {
        IcebergCalciteTable table = mockIcebergTable("eu-west-1", "empty-index", "role");
        IcebergScanPlan scanPlan = makeScanPlan(List.of());

        Map<String, String> config = StorageConfigBuilder.buildStorageConfig(table, scanPlan);

        assertEquals("eu-west-1", config.get("s3Region"));
        assertNull(config.get("s3Bucket"));
        assertNull(config.get("localMode"));
        assertEquals("empty-index", config.get("indexName"));
        assertEquals("role", config.get("authType"));
    }

    public void testBuildStorageConfigWithNullCatalogConfig() {
        IcebergCalciteTable table = mock(IcebergCalciteTable.class);
        when(table.catalogConfig()).thenReturn(null);
        IcebergScanPlan scanPlan = makeScanPlan(List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/file.parquet", 100)
        ));

        Map<String, String> config = StorageConfigBuilder.buildStorageConfig(table, scanPlan);

        assertNull(config.get("s3Region"));
        assertEquals("bucket", config.get("s3Bucket"));
        assertNull(config.get("indexName"));
        assertNull(config.get("authType"));
    }

    public void testBuildStorageConfigWithNullRegion() {
        IcebergCalciteTable table = mockIcebergTable(null, "no-region-index", "keys");
        IcebergScanPlan scanPlan = makeScanPlan(List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/path/file.parquet", 100)
        ));

        Map<String, String> config = StorageConfigBuilder.buildStorageConfig(table, scanPlan);

        assertNull(config.get("s3Region"));
        assertEquals("bucket", config.get("s3Bucket"));
    }

    public void testBuildStorageConfigS3PathWithoutSlash() {
        // Edge case: s3://bucket-only (no trailing slash or path)
        IcebergCalciteTable table = mockIcebergTable(null, "idx", "default");
        IcebergScanPlan scanPlan = makeScanPlan(List.of(
            new IcebergScanPlan.FileInfo("s3://bucketonly", 50)
        ));

        Map<String, String> config = StorageConfigBuilder.buildStorageConfig(table, scanPlan);

        // No slash found after removing "s3://", slashIdx == -1, so no s3Bucket
        assertNull(config.get("s3Bucket"));
    }

    // --- normalizeFilePaths tests ---

    public void testNormalizeS3PathsUnchanged() {
        List<String> result = StorageConfigBuilder.normalizeFilePaths(List.of(
            "s3://bucket/path/file1.parquet",
            "s3://bucket/path/file2.parquet"
        ));

        assertEquals(2, result.size());
        assertEquals("s3://bucket/path/file1.parquet", result.get(0));
        assertEquals("s3://bucket/path/file2.parquet", result.get(1));
    }

    public void testNormalizeFileColonPaths() {
        List<String> result = StorageConfigBuilder.normalizeFilePaths(List.of(
            "file:/tmp/data/file.parquet"
        ));

        assertEquals(1, result.size());
        assertEquals("file:///tmp/data/file.parquet", result.get(0));
    }

    public void testNormalizeFileDoubleSlashAlreadyCorrect() {
        List<String> result = StorageConfigBuilder.normalizeFilePaths(List.of(
            "file:///tmp/data/file.parquet"
        ));

        assertEquals(1, result.size());
        assertEquals("file:///tmp/data/file.parquet", result.get(0));
    }

    public void testNormalizeAbsolutePaths() {
        List<String> result = StorageConfigBuilder.normalizeFilePaths(List.of(
            "/tmp/data/file1.parquet",
            "/var/data/file2.parquet"
        ));

        assertEquals(2, result.size());
        assertEquals("file:///tmp/data/file1.parquet", result.get(0));
        assertEquals("file:///var/data/file2.parquet", result.get(1));
    }

    public void testNormalizeMixedPaths() {
        List<String> result = StorageConfigBuilder.normalizeFilePaths(List.of(
            "s3://bucket/file.parquet",
            "file:/local/file.parquet",
            "/absolute/file.parquet"
        ));

        assertEquals(3, result.size());
        assertEquals("s3://bucket/file.parquet", result.get(0));
        assertEquals("file:///local/file.parquet", result.get(1));
        assertEquals("file:///absolute/file.parquet", result.get(2));
    }

    public void testNormalizeEmptyList() {
        List<String> result = StorageConfigBuilder.normalizeFilePaths(List.of());
        assertTrue(result.isEmpty());
    }

    // --- Helper methods ---

    private IcebergCalciteTable mockIcebergTable(String region, String indexName, String authType) {
        IcebergCalciteTable table = mock(IcebergCalciteTable.class);
        CatalogConfig catalogConfig = new CatalogConfig(
            indexName,
            CatalogType.GLUE,
            region,
            "s3://warehouse",
            "db",
            "tbl",
            authType,
            null,
            null
        );
        when(table.catalogConfig()).thenReturn(catalogConfig);
        return table;
    }

    private IcebergScanPlan makeScanPlan(List<IcebergScanPlan.FileInfo> files) {
        return new IcebergScanPlan(files, List.of());
    }
}
