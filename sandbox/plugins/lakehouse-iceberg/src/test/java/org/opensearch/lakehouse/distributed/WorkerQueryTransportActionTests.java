/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.analytics.exec.ExternalQueryBackend;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class WorkerQueryTransportActionTests extends OpenSearchTestCase {

    public void testBuildResponseWithRows() {
        List<Object[]> rows = List.of(
            new Object[]{1, "alice", 9.5},
            new Object[]{2, "bob", 8.0},
            new Object[]{3, "charlie", 7.5}
        );

        WorkerQueryResponse response = WorkerQueryExecutor.buildResponse(rows);

        assertEquals(3, response.getRowCount());
        assertEquals(3, response.getColumnNames().size());
        assertEquals("col_0", response.getColumnNames().get(0));
        assertEquals("col_1", response.getColumnNames().get(1));
        assertEquals("col_2", response.getColumnNames().get(2));

        // Verify column types are inferred
        assertEquals("Integer", response.getColumnTypes().get(0));
        assertEquals("String", response.getColumnTypes().get(1));
        assertEquals("Double", response.getColumnTypes().get(2));

        // Verify data is column-major
        assertEquals(1, response.getColumnData()[0][0]);
        assertEquals(2, response.getColumnData()[0][1]);
        assertEquals(3, response.getColumnData()[0][2]);
        assertEquals("alice", response.getColumnData()[1][0]);
        assertEquals("bob", response.getColumnData()[1][1]);
        assertEquals("charlie", response.getColumnData()[1][2]);
    }

    public void testBuildResponseWithEmptyRows() {
        List<Object[]> rows = List.of();

        WorkerQueryResponse response = WorkerQueryExecutor.buildResponse(rows);

        assertEquals(0, response.getRowCount());
        assertEquals(0, response.getColumnNames().size());
        assertEquals(0, response.getColumnTypes().size());
        assertEquals(0, response.getColumnData().length);
    }

    public void testBuildResponseWithSingleRow() {
        List<Object[]> rows = List.<Object[]>of(new Object[]{"value"});

        WorkerQueryResponse response = WorkerQueryExecutor.buildResponse(rows);

        assertEquals(1, response.getRowCount());
        assertEquals(1, response.getColumnNames().size());
        assertEquals("col_0", response.getColumnNames().get(0));
        assertEquals("String", response.getColumnTypes().get(0));
        assertEquals("value", response.getColumnData()[0][0]);
    }

    public void testBuildResponseWithNullValues() {
        List<Object[]> rows = List.of(
            new Object[]{null, "x"},
            new Object[]{"y", null}
        );

        WorkerQueryResponse response = WorkerQueryExecutor.buildResponse(rows);

        assertEquals(2, response.getRowCount());
        assertNull(response.getColumnData()[0][0]);
        assertEquals("y", response.getColumnData()[0][1]);
        assertEquals("x", response.getColumnData()[1][0]);
        assertNull(response.getColumnData()[1][1]);

        // Type inferred from first non-null
        assertEquals("String", response.getColumnTypes().get(0));
        assertEquals("String", response.getColumnTypes().get(1));
    }

    public void testBuildResponseWithAllNullColumn() {
        List<Object[]> rows = List.of(
            new Object[]{null},
            new Object[]{null}
        );

        WorkerQueryResponse response = WorkerQueryExecutor.buildResponse(rows);

        assertEquals(2, response.getRowCount());
        assertEquals("UNKNOWN", response.getColumnTypes().get(0));
    }

    public void testBuildResponseTypeInferenceSkipsLeadingNulls() {
        List<Object[]> rows = List.of(
            new Object[]{null},
            new Object[]{null},
            new Object[]{42L}
        );

        WorkerQueryResponse response = WorkerQueryExecutor.buildResponse(rows);

        assertEquals("Long", response.getColumnTypes().get(0));
    }

    public void testSanitizeRowConvertsLocalDateTime() {
        LocalDateTime dt = LocalDateTime.of(2013, 7, 15, 10, 30, 0);
        Object[] row = new Object[]{1, dt, "text"};

        Object[] sanitized = WorkerQueryExecutor.sanitizeRow(row);

        assertNotSame(row, sanitized); // defensive copy, not in-place
        assertSame(dt, row[1]); // original not mutated
        assertEquals(1, sanitized[0]);
        assertEquals("2013-07-15T10:30", sanitized[1]);
        assertEquals("text", sanitized[2]);
    }

    public void testSanitizeRowConvertsLocalDate() {
        LocalDate date = LocalDate.of(2013, 7, 15);
        Object[] row = new Object[]{date};

        Object[] sanitized = WorkerQueryExecutor.sanitizeRow(row);

        assertSame(date, row[0]); // original not mutated
        assertEquals("2013-07-15", sanitized[0]);
    }

    public void testSanitizeRowPreservesNullsAndPrimitives() {
        Object[] row = new Object[]{null, 42L, "hello", 3.14, true};

        Object[] sanitized = WorkerQueryExecutor.sanitizeRow(row);

        assertNotSame(row, sanitized);
        assertNull(sanitized[0]);
        assertEquals(42L, sanitized[1]);
        assertEquals("hello", sanitized[2]);
        assertEquals(3.14, sanitized[3]);
        assertEquals(true, sanitized[4]);
    }

    public void testBuildResponseSanitizesTimestamps() {
        LocalDateTime dt = LocalDateTime.of(2013, 7, 15, 10, 30, 0);
        List<Object[]> rows = Arrays.<Object[]>asList(
            new Object[]{1, dt}
        );

        WorkerQueryResponse response = WorkerQueryExecutor.buildResponse(rows);

        assertEquals(1, response.getRowCount());
        assertEquals("2013-07-15T10:30", response.getColumnData()[1][0]);
        assertEquals("String", response.getColumnTypes().get(1));
    }

    // ---- resolveCredentials tests ----

    public void testResolveCredentialsDefaultAuthSkipsCredentialResolution() {
        ClusterService clusterService = mock(ClusterService.class);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("authType", "default");
        config.put("s3Region", "us-west-2");
        config.put("s3Bucket", "test-bucket");

        Map<String, String> result = WorkerQueryExecutor.resolveCredentials(config, clusterService);

        // default auth should NOT resolve credentials — Rust handles IMDS directly
        assertNull(result.get("s3AccessKeyId"));
        assertNull(result.get("s3SecretAccessKey"));
        assertNull(result.get("s3SessionToken"));
        // Other config preserved
        assertEquals("us-west-2", result.get("s3Region"));
        assertEquals("test-bucket", result.get("s3Bucket"));
        assertEquals("default", result.get("authType"));
        // indexName consumed (removed from config)
        assertNull(result.get("indexName"));
        // ClusterService should NOT be accessed for default auth
        verifyNoInteractions(clusterService);
    }

    public void testResolveCredentialsNoIndexNameReturnsEarly() {
        ClusterService clusterService = mock(ClusterService.class);

        Map<String, String> config = new HashMap<>();
        config.put("s3Region", "us-west-2");

        Map<String, String> result = WorkerQueryExecutor.resolveCredentials(config, clusterService);

        assertEquals("us-west-2", result.get("s3Region"));
        assertNull(result.get("s3AccessKeyId"));
        verifyNoInteractions(clusterService);
    }

    public void testResolveCredentialsLocalModeReturnsEarly() {
        ClusterService clusterService = mock(ClusterService.class);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("localMode", "true");

        Map<String, String> result = WorkerQueryExecutor.resolveCredentials(config, clusterService);

        assertEquals("true", result.get("localMode"));
        assertNull(result.get("s3AccessKeyId"));
        verifyNoInteractions(clusterService);
    }

    public void testResolveCredentialsMissingAuthTypeDefaultsToDefault() {
        ClusterService clusterService = mock(ClusterService.class);

        // No authType key → defaults to "default"
        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("s3Region", "us-west-2");

        Map<String, String> result = WorkerQueryExecutor.resolveCredentials(config, clusterService);

        // Should take the default auth path (no credentials)
        assertNull(result.get("s3AccessKeyId"));
        verifyNoInteractions(clusterService);
    }

    // ---- execute tests ----

    public void testExecuteReturnsResponse() {
        ExternalQueryBackend mockBackend = mock(ExternalQueryBackend.class);
        when(mockBackend.executeRemoteQuery(any(ExternalScanContext.class)))
            .thenReturn(List.of(new Object[]{1, "hello"}, new Object[]{2, "world"}));

        ClusterService clusterService = mock(ClusterService.class);
        Map<String, String> storageConfig = new HashMap<>();
        storageConfig.put("localMode", "true");

        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT * FROM t",
            List.of("/tmp/file1.parquet"),
            new long[]{1024L},
            storageConfig,
            "test_table"
        );

        WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, mockBackend);

        assertEquals(2, response.getRowCount());
        assertEquals(2, response.getColumnNames().size());
        assertEquals(1, response.getColumnData()[0][0]);
        assertEquals("hello", response.getColumnData()[1][0]);
        assertEquals(2, response.getColumnData()[0][1]);
        assertEquals("world", response.getColumnData()[1][1]);

        verify(mockBackend).executeRemoteQuery(any(ExternalScanContext.class));
    }

    public void testExecuteWithNoBackendThrows() {
        ClusterService clusterService = mock(ClusterService.class);
        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT * FROM t",
            List.of("/tmp/file1.parquet"),
            new long[]{1024L},
            Map.of("localMode", "true"),
            "test_table"
        );

        expectThrows(IllegalStateException.class, () ->
            WorkerQueryExecutor.execute(request, clusterService, null)
        );
    }

    public void testExecuteWithDefaultAuthSkipsCredentials() {
        ExternalQueryBackend mockBackend = mock(ExternalQueryBackend.class);
        when(mockBackend.executeRemoteQuery(any(ExternalScanContext.class)))
            .thenReturn(List.<Object[]>of(new Object[]{42}));

        ClusterService clusterService = mock(ClusterService.class);
        Map<String, String> storageConfig = new HashMap<>();
        storageConfig.put("indexName", "test_index");
        storageConfig.put("authType", "default");
        storageConfig.put("s3Region", "us-west-2");

        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT COUNT(*) FROM t",
            List.of("s3://bucket/file.parquet"),
            new long[]{2048L},
            storageConfig,
            "test_table"
        );

        WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, mockBackend);

        assertEquals(1, response.getRowCount());
        verifyNoInteractions(clusterService);
    }
}
