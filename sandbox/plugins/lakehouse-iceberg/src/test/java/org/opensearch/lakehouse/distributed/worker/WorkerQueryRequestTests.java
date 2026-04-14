/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.worker;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class WorkerQueryRequestTests extends OpenSearchTestCase {

    public void testGetters() {
        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT * FROM t",
            List.of("s3://bucket/file1.parquet", "s3://bucket/file2.parquet"),
            new long[]{1000L, 2000L},
            Map.of("s3Region", "us-west-2", "s3Bucket", "bucket"),
            "my_table"
        );

        assertEquals("SELECT * FROM t", request.getSqlQuery());
        assertEquals(2, request.getFilePaths().size());
        assertEquals("s3://bucket/file1.parquet", request.getFilePaths().get(0));
        assertEquals("s3://bucket/file2.parquet", request.getFilePaths().get(1));
        assertArrayEquals(new long[]{1000L, 2000L}, request.getFileSizes());
        assertEquals("us-west-2", request.getStorageConfig().get("s3Region"));
        assertEquals("bucket", request.getStorageConfig().get("s3Bucket"));
        assertEquals("my_table", request.getTableName());
    }

    public void testSerializationRoundtrip() throws IOException {
        WorkerQueryRequest original = new WorkerQueryRequest(
            "SELECT COUNT(*) FROM t",
            List.of("s3://bucket/part-0.parquet", "s3://bucket/part-1.parquet", "s3://bucket/part-2.parquet"),
            new long[]{100L, 200L, 300L},
            Map.of("s3Region", "us-east-1", "s3Bucket", "my-bucket", "s3AccessKeyId", "AKID"),
            "events"
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WorkerQueryRequest deserialized = new WorkerQueryRequest(in);

        assertEquals(original.getSqlQuery(), deserialized.getSqlQuery());
        assertEquals(original.getFilePaths(), deserialized.getFilePaths());
        assertArrayEquals(original.getFileSizes(), deserialized.getFileSizes());
        assertEquals(original.getStorageConfig().get("s3Region"), deserialized.getStorageConfig().get("s3Region"));
        assertEquals(original.getStorageConfig().get("s3Bucket"), deserialized.getStorageConfig().get("s3Bucket"));
        assertEquals(original.getStorageConfig().get("s3AccessKeyId"), deserialized.getStorageConfig().get("s3AccessKeyId"));
        assertEquals(original.getTableName(), deserialized.getTableName());
    }

    public void testSerializationWithEmptyStorageConfig() throws IOException {
        WorkerQueryRequest original = new WorkerQueryRequest(
            "SELECT 1",
            List.of("file:///tmp/data.parquet"),
            new long[]{42L},
            Map.of(),
            "local_table"
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WorkerQueryRequest deserialized = new WorkerQueryRequest(in);

        assertEquals(original.getSqlQuery(), deserialized.getSqlQuery());
        assertEquals(original.getFilePaths(), deserialized.getFilePaths());
        assertArrayEquals(original.getFileSizes(), deserialized.getFileSizes());
        assertTrue(deserialized.getStorageConfig().isEmpty());
        assertEquals(original.getTableName(), deserialized.getTableName());
    }

    public void testSerializationWithEmptyFileSizes() throws IOException {
        WorkerQueryRequest original = new WorkerQueryRequest(
            "SELECT 1",
            List.of("file:///tmp/data.parquet"),
            new long[0],
            Map.of(),
            "t"
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WorkerQueryRequest deserialized = new WorkerQueryRequest(in);

        assertArrayEquals(new long[0], deserialized.getFileSizes());
    }

    public void testValidateSucceeds() {
        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT 1",
            List.of("file.parquet"),
            new long[]{10L},
            Map.of(),
            "table"
        );
        assertNull(request.validate());
    }

    public void testValidateFailsForNullSqlQuery() {
        WorkerQueryRequest request = new WorkerQueryRequest(
            null,
            List.of("file.parquet"),
            new long[]{10L},
            Map.of(),
            "table"
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("sqlQuery is missing or empty"));
    }

    public void testValidateFailsForEmptySqlQuery() {
        WorkerQueryRequest request = new WorkerQueryRequest(
            "",
            List.of("file.parquet"),
            new long[]{10L},
            Map.of(),
            "table"
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("sqlQuery is missing or empty"));
    }

    public void testValidateFailsForNullFilePaths() {
        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT 1",
            null,
            new long[]{10L},
            Map.of(),
            "table"
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("filePaths is missing or empty"));
    }

    public void testValidateFailsForEmptyFilePaths() {
        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT 1",
            List.of(),
            new long[]{},
            Map.of(),
            "table"
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("filePaths is missing or empty"));
    }

    public void testValidateFailsForNullTableName() {
        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT 1",
            List.of("file.parquet"),
            new long[]{10L},
            Map.of(),
            null
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("tableName is missing or empty"));
    }

    public void testValidateFailsForEmptyTableName() {
        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT 1",
            List.of("file.parquet"),
            new long[]{10L},
            Map.of(),
            ""
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("tableName is missing or empty"));
    }

    public void testValidateReportsMultipleErrors() {
        WorkerQueryRequest request = new WorkerQueryRequest(
            null,
            null,
            new long[0],
            Map.of(),
            null
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("sqlQuery"));
        assertTrue(e.getMessage().contains("filePaths"));
        assertTrue(e.getMessage().contains("tableName"));
    }
}
