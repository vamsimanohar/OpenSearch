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
}
