/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link IcebergQueryExecutor} and {@link IcebergExecutionContext}.
 *
 * <p>Full integration testing of the prepare flow with a real Iceberg table
 * is deferred to integration test tasks. These tests cover the data carrier
 * and utility logic.</p>
 */
public class IcebergQueryExecutorTests extends OpenSearchTestCase {

    public void testExtractBucketFromS3Path() {
        assertEquals("my-bucket", IcebergQueryExecutor.extractBucket("s3://my-bucket/path/to/file.parquet"));
        assertEquals("my-bucket", IcebergQueryExecutor.extractBucket("s3://my-bucket/"));
        assertEquals("my-bucket", IcebergQueryExecutor.extractBucket("s3://my-bucket"));
        assertEquals("", IcebergQueryExecutor.extractBucket("/local/path/file.parquet"));
        assertEquals("", IcebergQueryExecutor.extractBucket(""));
    }

    public void testExecutionContextCarriesAllFields() {
        byte[] plan = new byte[] { 1, 2, 3 };
        IcebergExecutionContext ctx = new IcebergExecutionContext(
            "test_table",
            List.of("s3://bucket/file1.parquet", "s3://bucket/file2.parquet"),
            plan,
            List.of("id", "name"),
            "us-east-1",
            "bucket",
            null,
            null,
            null
        );

        assertEquals("test_table", ctx.getTableName());
        assertEquals(2, ctx.getDataFilePaths().size());
        assertEquals("s3://bucket/file1.parquet", ctx.getDataFilePaths().get(0));
        assertEquals("s3://bucket/file2.parquet", ctx.getDataFilePaths().get(1));
        assertArrayEquals(plan, ctx.getSubstraitPlan());
        assertEquals(List.of("id", "name"), ctx.getProjectedColumns());
        assertEquals("us-east-1", ctx.getS3Region());
        assertEquals("bucket", ctx.getS3Bucket());
        assertNull(ctx.getAccessKeyId());
        assertNull(ctx.getSecretAccessKey());
        assertNull(ctx.getSessionToken());
    }

    public void testExecutionContextWithCredentials() {
        IcebergExecutionContext ctx = new IcebergExecutionContext(
            "table",
            List.of(),
            new byte[0],
            List.of(),
            "eu-west-1",
            "my-bucket",
            "AKIAEXAMPLE",
            "secretKey",
            "sessionToken"
        );

        assertEquals("AKIAEXAMPLE", ctx.getAccessKeyId());
        assertEquals("secretKey", ctx.getSecretAccessKey());
        assertEquals("sessionToken", ctx.getSessionToken());
        assertEquals("eu-west-1", ctx.getS3Region());
        assertEquals("my-bucket", ctx.getS3Bucket());
    }

    public void testExtractBucketFromNonS3Path() {
        assertEquals("", IcebergQueryExecutor.extractBucket("hdfs://namenode/path"));
        assertEquals("", IcebergQueryExecutor.extractBucket("http://example.com/path"));
    }

    public void testExecutionContextEmptyFilePaths() {
        IcebergExecutionContext ctx = new IcebergExecutionContext(
            "empty_table",
            List.of(),
            new byte[0],
            List.of(),
            "us-west-2",
            "",
            null,
            null,
            null
        );

        assertEquals(0, ctx.getDataFilePaths().size());
        assertEquals("empty_table", ctx.getTableName());
        assertEquals("", ctx.getS3Bucket());
    }
}
