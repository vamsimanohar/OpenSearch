/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import org.opensearch.action.ActionRequestValidationException;

import java.io.IOException;
import java.util.Map;

public class LakehouseWorkerRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        String[] filePaths = new String[] {
            "s3://my-bucket/warehouse/db/table/data/00000-0-abc.parquet",
            "s3://my-bucket/warehouse/db/table/data/00001-0-def.parquet" };
        byte[] substraitPlan = new byte[] { 0x0A, 0x1B, 0x2C, 0x3D };
        Map<String, String> storageConfig = Map.of(
            "s3Region", "us-east-1",
            "s3Bucket", "my-bucket",
            "s3AccessKeyId", "AKIAIOSFODNN7EXAMPLE",
            "s3SecretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "s3SessionToken", "FwoGZXIvYXdzEBY...",
            "s3Endpoint", "https://s3.us-east-1.amazonaws.com"
        );
        String tableName = "my_catalog.my_db.my_table";

        LakehouseWorkerRequest original = new LakehouseWorkerRequest(filePaths, substraitPlan, storageConfig, tableName);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseWorkerRequest deserialized = new LakehouseWorkerRequest(in);

        assertArrayEquals(filePaths, deserialized.getFilePaths());
        assertArrayEquals(substraitPlan, deserialized.getSubstraitPlan());
        assertEquals(storageConfig, deserialized.getStorageConfig());
        assertEquals(tableName, deserialized.getTableName());
    }

    public void testEmptyFilePaths() throws IOException {
        String[] filePaths = new String[0];
        byte[] substraitPlan = new byte[] { 0x01 };
        Map<String, String> storageConfig = Map.of("s3Region", "us-west-2");
        String tableName = "t";

        LakehouseWorkerRequest original = new LakehouseWorkerRequest(filePaths, substraitPlan, storageConfig, tableName);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseWorkerRequest deserialized = new LakehouseWorkerRequest(in);

        assertArrayEquals(filePaths, deserialized.getFilePaths());
        assertEquals(tableName, deserialized.getTableName());
    }

    public void testEmptyStorageConfig() throws IOException {
        String[] filePaths = new String[] { "file:///tmp/data.parquet" };
        byte[] substraitPlan = new byte[] { 0x0A };
        Map<String, String> storageConfig = Map.of();
        String tableName = "local_table";

        LakehouseWorkerRequest original = new LakehouseWorkerRequest(filePaths, substraitPlan, storageConfig, tableName);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseWorkerRequest deserialized = new LakehouseWorkerRequest(in);

        assertArrayEquals(filePaths, deserialized.getFilePaths());
        assertArrayEquals(substraitPlan, deserialized.getSubstraitPlan());
        assertEquals(storageConfig, deserialized.getStorageConfig());
        assertEquals(tableName, deserialized.getTableName());
    }

    // ---- Validation tests ----

    public void testValidRequestReturnsNoErrors() {
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            new String[] { "file.parquet" },
            new byte[] { 0x01 },
            Map.of(),
            "table"
        );
        assertNull("Valid request should have no validation errors", request.validate());
    }

    public void testNullFilePathsFailsValidation() {
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            null,
            new byte[] { 0x01 },
            Map.of(),
            "table"
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull("Null filePaths should fail validation", e);
        assertTrue(e.getMessage().contains("filePaths must not be null or empty"));
    }

    public void testEmptyFilePathsFailsValidation() {
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            new String[0],
            new byte[] { 0x01 },
            Map.of(),
            "table"
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull("Empty filePaths should fail validation", e);
        assertTrue(e.getMessage().contains("filePaths must not be null or empty"));
    }

    public void testNullSubstraitPlanFailsValidation() {
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            new String[] { "file.parquet" },
            null,
            Map.of(),
            "table"
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull("Null substraitPlan should fail validation", e);
        assertTrue(e.getMessage().contains("substraitPlan must not be null or empty"));
    }

    public void testEmptySubstraitPlanFailsValidation() {
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            new String[] { "file.parquet" },
            new byte[0],
            Map.of(),
            "table"
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull("Empty substraitPlan should fail validation", e);
        assertTrue(e.getMessage().contains("substraitPlan must not be null or empty"));
    }

    public void testBothInvalidReportsBothErrors() {
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            null,
            null,
            Map.of(),
            "table"
        );
        ActionRequestValidationException e = request.validate();
        assertNotNull("Both null should fail validation", e);
        assertTrue(e.getMessage().contains("filePaths must not be null or empty"));
        assertTrue(e.getMessage().contains("substraitPlan must not be null or empty"));
    }

    public void testLargeSubstraitPlanSerializes() throws IOException {
        byte[] largePlan = new byte[1024 * 1024]; // 1 MB
        java.util.Arrays.fill(largePlan, (byte) 0xAB);

        LakehouseWorkerRequest original = new LakehouseWorkerRequest(
            new String[] { "file.parquet" },
            largePlan,
            Map.of("s3Region", "us-east-1"),
            "big_table"
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseWorkerRequest deserialized = new LakehouseWorkerRequest(in);

        assertArrayEquals(largePlan, deserialized.getSubstraitPlan());
        assertEquals("big_table", deserialized.getTableName());
    }

    public void testManyFilePathsSerialize() throws IOException {
        String[] paths = new String[100];
        for (int i = 0; i < 100; i++) {
            paths[i] = "s3://bucket/data/part-" + String.format("%05d", i) + ".parquet";
        }

        LakehouseWorkerRequest original = new LakehouseWorkerRequest(
            paths,
            new byte[] { 0x01, 0x02 },
            Map.of("s3Region", "eu-west-1", "s3Bucket", "bucket"),
            "wide_table"
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseWorkerRequest deserialized = new LakehouseWorkerRequest(in);

        assertEquals(100, deserialized.getFilePaths().length);
        assertEquals("s3://bucket/data/part-00099.parquet", deserialized.getFilePaths()[99]);
    }
}
