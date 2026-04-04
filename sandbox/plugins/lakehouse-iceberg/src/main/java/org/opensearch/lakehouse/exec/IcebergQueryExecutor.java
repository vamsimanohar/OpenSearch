/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.apache.calcite.rel.RelNode;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.scan.IcebergScanPlanner;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.lakehouse.substrait.CalciteSubstraitConverter;

import java.io.IOException;
import java.util.List;

/**
 * Orchestrates Iceberg query execution on a single node.
 * Flow: extract predicates &rarr; scan plan &rarr; substrait convert &rarr; JNI execute
 */
public class IcebergQueryExecutor {

    private final IcebergScanPlanner scanPlanner;

    public IcebergQueryExecutor(IcebergScanPlanner scanPlanner) {
        this.scanPlanner = scanPlanner;
    }

    /**
     * Prepare an execution context for a query against an Iceberg table.
     *
     * @param relNode      the optimized Calcite plan
     * @param icebergTable the Calcite table wrapping the Iceberg table
     * @return execution context ready for JNI bridge
     * @throws IOException if substrait conversion fails
     */
    public IcebergExecutionContext prepare(RelNode relNode, IcebergCalciteTable icebergTable) throws IOException {
        // 1. Plan scan with predicate pushdown
        IcebergScanPlan scanPlan = scanPlanner.planScan(
            icebergTable.getIcebergTable(),
            icebergTable.getPinnedSnapshotId(),
            List.of(),  // predicates extracted separately
            null         // all columns
        );

        // 2. Convert Calcite plan to Substrait bytes
        byte[] substraitBytes = CalciteSubstraitConverter.toSubstrait(relNode);

        // 3. Extract S3 bucket from first file path
        String firstPath = scanPlan.getDataFilePaths().isEmpty() ? "" : scanPlan.getDataFilePaths().get(0);
        String bucket = extractBucket(firstPath);
        String region = "us-east-1"; // TODO: get from catalog config

        // 4. Build execution context
        return new IcebergExecutionContext(
            icebergTable.getIcebergTable().name(),
            scanPlan.getDataFilePaths(),
            substraitBytes,
            scanPlan.getProjectedColumns(),
            region,
            bucket,
            null, null, null  // use default credential chain
        );
    }

    /**
     * Extracts the S3 bucket name from an S3 path.
     *
     * @param s3Path the S3 path (e.g. "s3://my-bucket/path/to/file.parquet")
     * @return the bucket name, or empty string if the path is not an S3 path
     */
    static String extractBucket(String s3Path) {
        if (s3Path.startsWith("s3://")) {
            String withoutScheme = s3Path.substring(5);
            int slashIdx = withoutScheme.indexOf('/');
            return slashIdx > 0 ? withoutScheme.substring(0, slashIdx) : withoutScheme;
        }
        return "";
    }
}
