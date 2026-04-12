/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests for the distributed execution extensions on {@link ExternalScanContext}.
 */
public class ExternalScanContextDistributedTests extends OpenSearchTestCase {

    public void testPreComputedResultsDefaultsToNull() {
        ExternalScanContext ctx = new ExternalScanContext(
            "test_table",
            List.of("s3://bucket/file.parquet"),
            new long[]{1024L},
            "SELECT * FROM test_table",
            Map.of("s3Region", "us-west-2")
        );
        assertNull(ctx.getPreComputedResults());
    }

    public void testConstructorWithPreComputedResults() {
        List<Object[]> results = Arrays.asList(
            new Object[]{1, "alice"},
            new Object[]{2, "bob"}
        );

        ExternalScanContext ctx = new ExternalScanContext(
            "test_table",
            List.of("s3://bucket/file.parquet"),
            new long[]{1024L},
            "SELECT * FROM test_table",
            Map.of(),
            results
        );

        assertNotNull(ctx.getPreComputedResults());
        int count = 0;
        for (Object[] row : ctx.getPreComputedResults()) {
            count++;
        }
        assertEquals(2, count);
    }

    public void testConstructorWithNullPreComputedResults() {
        ExternalScanContext ctx = new ExternalScanContext(
            "test_table",
            List.of(),
            new long[0],
            "SELECT 1",
            Map.of(),
            null
        );
        assertNull(ctx.getPreComputedResults());
    }

    public void testOriginalFieldsUnaffectedByPreComputedResults() {
        ExternalScanContext ctx = new ExternalScanContext(
            "my_table",
            List.of("file1.parquet", "file2.parquet"),
            new long[]{100L, 200L},
            "SELECT COUNT(*) FROM my_table",
            Map.of("s3Region", "us-east-1"),
            Arrays.<Object[]>asList(new Object[]{42L})
        );

        assertEquals("my_table", ctx.getTableName());
        assertEquals(2, ctx.getDataFilePaths().size());
        assertEquals(100L, ctx.getFileSizes()[0]);
        assertEquals("SELECT COUNT(*) FROM my_table", ctx.getSqlQuery());
        assertEquals("us-east-1", ctx.getStorageConfig().get("s3Region"));
        assertNotNull(ctx.getPreComputedResults());
    }
}
