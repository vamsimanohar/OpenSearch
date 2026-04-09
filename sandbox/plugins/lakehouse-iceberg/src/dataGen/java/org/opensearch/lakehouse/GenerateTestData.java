/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Standalone utility to generate local Iceberg test data with NYC-taxi-like schema.
 * Run via: ./gradlew :sandbox:plugins:lakehouse-iceberg:generateIcebergTestData
 */
public class GenerateTestData {

    /** Creates test data at /tmp/iceberg-test-warehouse. */
    public static void main(String[] args) throws Exception {
        String warehousePath = "/tmp/iceberg-test-warehouse";

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "file://" + warehousePath);
        properties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, "hadoop");

        Configuration hadoopConf = new Configuration();
        Catalog catalog = CatalogUtil.buildIcebergCatalog("test-catalog", properties, hadoopConf);

        // 20-column NYC-taxi-like schema matching integration test expectations
        Schema schema = new Schema(
            Types.NestedField.optional(1, "vendorid", Types.IntegerType.get()),
            Types.NestedField.optional(2, "tpep_pickup_datetime", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(3, "tpep_dropoff_datetime", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "passenger_count", Types.IntegerType.get()),
            Types.NestedField.optional(5, "trip_distance", Types.DoubleType.get()),
            Types.NestedField.optional(6, "ratecodeid", Types.IntegerType.get()),
            Types.NestedField.optional(7, "store_and_fwd_flag", Types.StringType.get()),
            Types.NestedField.optional(8, "pulocationid", Types.IntegerType.get()),
            Types.NestedField.optional(9, "dolocationid", Types.IntegerType.get()),
            Types.NestedField.optional(10, "payment_type", Types.IntegerType.get()),
            Types.NestedField.optional(11, "fare_amount", Types.DoubleType.get()),
            Types.NestedField.optional(12, "extra", Types.DoubleType.get()),
            Types.NestedField.optional(13, "mta_tax", Types.DoubleType.get()),
            Types.NestedField.optional(14, "tip_amount", Types.DoubleType.get()),
            Types.NestedField.optional(15, "tolls_amount", Types.DoubleType.get()),
            Types.NestedField.optional(16, "improvement_surcharge", Types.DoubleType.get()),
            Types.NestedField.optional(17, "total_amount", Types.DoubleType.get()),
            Types.NestedField.optional(18, "congestion_surcharge", Types.DoubleType.get()),
            Types.NestedField.optional(19, "airport_fee", Types.DoubleType.get()),
            Types.NestedField.optional(20, "cbd_congestion_fee", Types.DoubleType.get())
        );

        TableIdentifier tableId = TableIdentifier.of(Namespace.of("default"), "test_events");

        if (catalog.tableExists(tableId)) {
            catalog.dropTable(tableId, true);
            System.out.println("Dropped existing table: " + tableId);
        }

        Table table = catalog.createTable(tableId, schema);
        System.out.println("Created table: " + table.location());

        // Generate 5000 realistic records with diverse values for integration tests
        Random rng = new Random(42);
        List<GenericRecord> records = new ArrayList<>();
        GenericRecord template = GenericRecord.create(schema);

        String[] flags = {"Y", "N"};
        LocalDateTime basePickup = LocalDateTime.of(2024, 1, 15, 8, 0, 0);

        for (int i = 0; i < 5000; i++) {
            int vendorId = (i % 3) + 1; // 1, 2, 3
            int passengers = rng.nextInt(6) + 1; // 1-6
            // Most trips 0.5-25 miles, every 50th row has a very long trip (51-65 miles)
            double distance;
            if (i % 50 == 25) {
                distance = Math.round((51.0 + rng.nextDouble() * 14.0) * 100.0) / 100.0;
            } else {
                distance = Math.round((0.5 + rng.nextDouble() * 25.0) * 100.0) / 100.0;
            }
            int rateCode = (i % 5) + 1; // 1-5
            String flag = flags[i % 2];
            int puLoc = 100 + rng.nextInt(200);
            int doLoc = 100 + rng.nextInt(200);
            int payType = (i % 4) + 1; // 1-4
            // Most fares are 5+, but every 20th row has a very low fare (1-4)
            double fare;
            if (i % 20 == 10) {
                fare = Math.round((1.0 + rng.nextDouble() * 3.0) * 100.0) / 100.0;
            } else {
                fare = Math.round((5.0 + distance * 2.5 + rng.nextDouble() * 10.0) * 100.0) / 100.0;
            }
            double extraAmt = (i % 3 == 0) ? 0.5 : (i % 3 == 1) ? 1.0 : 0.0;
            double mtaTax = 0.5;
            double tip = (payType == 1) ? Math.round(fare * (0.1 + rng.nextDouble() * 0.2) * 100.0) / 100.0 : 0.0;
            double tolls = (i % 7 == 0) ? Math.round((5.0 + rng.nextDouble() * 8.0) * 100.0) / 100.0 : 0.0;
            double improvement = 0.3;
            // Every 10th row has NULL congestion_surcharge (for IS NULL tests)
            Double congestion = (i % 10 == 5) ? null : ((i % 4 == 0) ? 2.5 : 0.0);
            double airportFee = (i % 10 == 0) ? 1.25 : 0.0;
            double cbdFee = (i % 8 == 0) ? 2.75 : 0.0;
            double congestionVal = congestion != null ? congestion : 0.0;
            double total = Math.round((fare + extraAmt + mtaTax + tip + tolls + improvement + congestionVal + airportFee + cbdFee) * 100.0) / 100.0;

            LocalDateTime pickup = basePickup.plusMinutes(i * 30L + rng.nextInt(15));
            int tripMinutes = 5 + rng.nextInt(40);
            LocalDateTime dropoff = pickup.plusMinutes(tripMinutes);

            GenericRecord r = template.copy();
            r.setField("vendorid", vendorId);
            r.setField("tpep_pickup_datetime", pickup);
            r.setField("tpep_dropoff_datetime", dropoff);
            r.setField("passenger_count", passengers);
            r.setField("trip_distance", distance);
            r.setField("ratecodeid", rateCode);
            r.setField("store_and_fwd_flag", flag);
            r.setField("pulocationid", puLoc);
            r.setField("dolocationid", doLoc);
            r.setField("payment_type", payType);
            r.setField("fare_amount", fare);
            r.setField("extra", extraAmt);
            r.setField("mta_tax", mtaTax);
            r.setField("tip_amount", tip);
            r.setField("tolls_amount", tolls);
            r.setField("improvement_surcharge", improvement);
            r.setField("total_amount", total);
            if (congestion != null) {
                r.setField("congestion_surcharge", congestion);
            }
            r.setField("airport_fee", airportFee);
            r.setField("cbd_congestion_fee", cbdFee);
            records.add(r);
        }

        // Write Parquet data
        GenericAppenderFactory appenderFactory = new GenericAppenderFactory(schema);
        String dataFilePath = table.location() + "/data/test-data-00000.parquet";
        OutputFile outputFile = table.io().newOutputFile(dataFilePath);

        DataWriter<Record> writer = appenderFactory.newDataWriter(
            table.encryption().encrypt(outputFile),
            FileFormat.PARQUET,
            null
        );

        try {
            for (GenericRecord r : records) {
                writer.write(r);
            }
        } finally {
            writer.close();
        }

        table.newAppend().appendFile(writer.toDataFile()).commit();
        table.refresh();

        // Verify
        List<Record> readBack = new ArrayList<>();
        try (var reader = IcebergGenerics.read(table).build()) {
            for (Record r : reader) {
                readBack.add(r);
            }
        }

        System.out.println("=== Iceberg Test Data Created ===");
        System.out.println("Warehouse: file://" + warehousePath);
        System.out.println("Table: default.test_events");
        System.out.println("Location: " + table.location());
        System.out.println("Snapshot: " + table.currentSnapshot().snapshotId());
        System.out.println("Records written: " + readBack.size());
        System.out.println("Schema: " + table.schema());
        System.out.println("=================================");
        System.out.println();
        System.out.println("To register in OpenSearch:");
        System.out.println("  curl -X PUT localhost:9200/_lakehouse/catalog/local-hadoop \\");
        System.out.println("    -H 'Content-Type: application/json' \\");
        System.out.println("    -d '{\"type\":\"hadoop\",\"warehouse\":\"file:///tmp/iceberg-test-warehouse\"}'");
        System.out.println();
        System.out.println("  curl -X PUT localhost:9200/_lakehouse/table/nyc_taxi \\");
        System.out.println("    -H 'Content-Type: application/json' \\");
        System.out.println("    -d '{\"catalog\":\"local-hadoop\",\"namespace\":\"default\",\"table\":\"test_events\"}'");
    }
}
