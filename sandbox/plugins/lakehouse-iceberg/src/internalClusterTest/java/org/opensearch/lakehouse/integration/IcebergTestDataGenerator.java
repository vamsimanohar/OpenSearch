/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Verifies that local Iceberg test data exists at the expected warehouse location.
 * <p>
 * The test data is pre-generated externally (e.g., via the pyiceberg script or
 * the {@code createLocalIcebergTable} Gradle task). This class only checks
 * that the data is present and provides constants for the table schema.
 * <p>
 * The test_events table contains a NYC-taxi-like schema with 20 columns
 * and approximately 5000 rows across 4 Parquet files.
 */
final class IcebergTestDataGenerator {

    /** Warehouse location for local Hadoop catalog. */
    static final String WAREHOUSE = "/tmp/iceberg-test-warehouse";

    /** Namespace (Iceberg/Hadoop directory). */
    static final String NAMESPACE = "default";

    /** Table name within the namespace. */
    static final String TABLE_NAME = "test_events";

    /** Expected total row count in the test data. */
    static final int ROW_COUNT = 5000;

    /** Number of columns in the test table. */
    static final int COLUMN_COUNT = 20;

    private IcebergTestDataGenerator() {}

    /**
     * Verifies that the test data warehouse exists and contains the expected table.
     *
     * @throws IllegalStateException if the test data is missing
     */
    static void verifyTestData() {
        Path warehousePath = Paths.get(WAREHOUSE);
        Path tablePath = warehousePath.resolve(NAMESPACE).resolve(TABLE_NAME);
        Path metadataPath = tablePath.resolve("metadata");
        Path dataPath = tablePath.resolve("data");

        if (!Files.isDirectory(warehousePath)) {
            throw new IllegalStateException(
                "Test data warehouse not found at " + WAREHOUSE + ". "
                    + "Generate test data first using the pyiceberg script or Gradle task."
            );
        }
        if (!Files.isDirectory(tablePath)) {
            throw new IllegalStateException(
                "Test table not found at " + tablePath + ". "
                    + "Expected table '" + NAMESPACE + "." + TABLE_NAME + "' in warehouse."
            );
        }
        if (!Files.isDirectory(metadataPath)) {
            throw new IllegalStateException("Iceberg metadata directory not found at " + metadataPath);
        }
        if (!Files.isDirectory(dataPath)) {
            throw new IllegalStateException("Iceberg data directory not found at " + dataPath);
        }
    }
}
