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

import java.io.IOException;
import java.util.Map;

/**
 * Tests for {@link LakehouseWorkerRequest} serialization round-trip.
 */
public class LakehouseWorkerRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        String[] filePaths = new String[] {
            "s3://bucket/data/file1.parquet",
            "s3://bucket/data/file2.parquet",
            "s3://bucket/data/file3.parquet"
        };
        String sqlQuery = "SELECT city, COUNT(*) AS cnt FROM test_table GROUP BY city";
        Map<String, String> storageConfig = Map.of(
            "s3Region", "us-west-2",
            "s3Bucket", "my-bucket",
            "s3AccessKeyId", "AKIATEST",
            "s3SecretAccessKey", "secretkey123"
        );
        String tableName = "test_table";

        LakehouseWorkerRequest original = new LakehouseWorkerRequest(filePaths, sqlQuery, storageConfig, tableName);

        // Serialize
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        // Deserialize
        StreamInput input = output.bytes().streamInput();
        LakehouseWorkerRequest deserialized = new LakehouseWorkerRequest(input);

        // Verify
        assertArrayEquals("File paths should match", filePaths, deserialized.getFilePaths());
        assertEquals("SQL query should match", sqlQuery, deserialized.getSqlQuery());
        assertEquals("Table name should match", tableName, deserialized.getTableName());
        assertEquals("Storage config size should match", storageConfig.size(), deserialized.getStorageConfig().size());
        for (Map.Entry<String, String> entry : storageConfig.entrySet()) {
            assertEquals("Storage config value for " + entry.getKey() + " should match",
                entry.getValue(), deserialized.getStorageConfig().get(entry.getKey()));
        }
    }

    public void testValidation() {
        // Valid request
        LakehouseWorkerRequest valid = new LakehouseWorkerRequest(
            new String[] { "file.parquet" },
            "SELECT * FROM t",
            Map.of(),
            "t"
        );
        assertNull("Valid request should not have validation errors", valid.validate());

        // Missing file paths
        LakehouseWorkerRequest noFiles = new LakehouseWorkerRequest(
            new String[0],
            "SELECT * FROM t",
            Map.of(),
            "t"
        );
        assertNotNull("Should have validation error for empty filePaths", noFiles.validate());

        // Missing SQL query
        LakehouseWorkerRequest noSql = new LakehouseWorkerRequest(
            new String[] { "file.parquet" },
            "",
            Map.of(),
            "t"
        );
        assertNotNull("Should have validation error for empty sqlQuery", noSql.validate());

        // Both missing
        LakehouseWorkerRequest bothMissing = new LakehouseWorkerRequest(
            new String[0],
            "",
            Map.of(),
            "t"
        );
        assertNotNull("Should have validation errors", bothMissing.validate());
        assertTrue("Should have multiple validation errors",
            bothMissing.validate().validationErrors().size() >= 2);
    }

    public void testSerializationWithEmptyStorageConfig() throws IOException {
        LakehouseWorkerRequest original = new LakehouseWorkerRequest(
            new String[] { "file.parquet" },
            "SELECT 1",
            Map.of(),
            "test"
        );

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        LakehouseWorkerRequest deserialized = new LakehouseWorkerRequest(input);

        assertTrue("Storage config should be empty", deserialized.getStorageConfig().isEmpty());
    }
}
