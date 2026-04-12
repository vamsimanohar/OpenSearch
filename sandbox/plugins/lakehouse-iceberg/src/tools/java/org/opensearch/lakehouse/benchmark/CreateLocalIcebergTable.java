/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Creates local Iceberg tables wrapping existing ClickBench Parquet files.
 * Args: warehouse namespace table parquet-path
 */
public class CreateLocalIcebergTable {

    private static final Schema CLICKBENCH_SCHEMA = new Schema(
        Types.NestedField.required(1, "WatchID", Types.LongType.get()),
        Types.NestedField.required(2, "JavaEnable", Types.IntegerType.get()),
        Types.NestedField.required(3, "Title", Types.StringType.get()),
        Types.NestedField.required(4, "GoodEvent", Types.IntegerType.get()),
        Types.NestedField.required(5, "EventTime", Types.LongType.get()),
        Types.NestedField.required(6, "EventDate", Types.IntegerType.get()),
        Types.NestedField.required(7, "CounterID", Types.IntegerType.get()),
        Types.NestedField.required(8, "ClientIP", Types.IntegerType.get()),
        Types.NestedField.required(9, "RegionID", Types.IntegerType.get()),
        Types.NestedField.required(10, "UserID", Types.LongType.get()),
        Types.NestedField.required(11, "CounterClass", Types.IntegerType.get()),
        Types.NestedField.required(12, "OS", Types.IntegerType.get()),
        Types.NestedField.required(13, "UserAgent", Types.IntegerType.get()),
        Types.NestedField.required(14, "URL", Types.StringType.get()),
        Types.NestedField.required(15, "Referer", Types.StringType.get()),
        Types.NestedField.required(16, "IsRefresh", Types.IntegerType.get()),
        Types.NestedField.required(17, "RefererCategoryID", Types.IntegerType.get()),
        Types.NestedField.required(18, "RefererRegionID", Types.IntegerType.get()),
        Types.NestedField.required(19, "URLCategoryID", Types.IntegerType.get()),
        Types.NestedField.required(20, "URLRegionID", Types.IntegerType.get()),
        Types.NestedField.required(21, "ResolutionWidth", Types.IntegerType.get()),
        Types.NestedField.required(22, "ResolutionHeight", Types.IntegerType.get()),
        Types.NestedField.required(23, "ResolutionDepth", Types.IntegerType.get()),
        Types.NestedField.required(24, "FlashMajor", Types.IntegerType.get()),
        Types.NestedField.required(25, "FlashMinor", Types.IntegerType.get()),
        Types.NestedField.required(26, "FlashMinor2", Types.StringType.get()),
        Types.NestedField.required(27, "NetMajor", Types.IntegerType.get()),
        Types.NestedField.required(28, "NetMinor", Types.IntegerType.get()),
        Types.NestedField.required(29, "UserAgentMajor", Types.IntegerType.get()),
        Types.NestedField.required(30, "UserAgentMinor", Types.StringType.get()),
        Types.NestedField.required(31, "CookieEnable", Types.IntegerType.get()),
        Types.NestedField.required(32, "JavascriptEnable", Types.IntegerType.get()),
        Types.NestedField.required(33, "IsMobile", Types.IntegerType.get()),
        Types.NestedField.required(34, "MobilePhone", Types.IntegerType.get()),
        Types.NestedField.required(35, "MobilePhoneModel", Types.StringType.get()),
        Types.NestedField.required(36, "Params", Types.StringType.get()),
        Types.NestedField.required(37, "IPNetworkID", Types.IntegerType.get()),
        Types.NestedField.required(38, "TraficSourceID", Types.IntegerType.get()),
        Types.NestedField.required(39, "SearchEngineID", Types.IntegerType.get()),
        Types.NestedField.required(40, "SearchPhrase", Types.StringType.get()),
        Types.NestedField.required(41, "AdvEngineID", Types.IntegerType.get()),
        Types.NestedField.required(42, "IsArtifical", Types.IntegerType.get()),
        Types.NestedField.required(43, "WindowClientWidth", Types.IntegerType.get()),
        Types.NestedField.required(44, "WindowClientHeight", Types.IntegerType.get()),
        Types.NestedField.required(45, "ClientTimeZone", Types.IntegerType.get()),
        Types.NestedField.required(46, "ClientEventTime", Types.LongType.get()),
        Types.NestedField.required(47, "SilverlightVersion1", Types.IntegerType.get()),
        Types.NestedField.required(48, "SilverlightVersion2", Types.IntegerType.get()),
        Types.NestedField.required(49, "SilverlightVersion3", Types.IntegerType.get()),
        Types.NestedField.required(50, "SilverlightVersion4", Types.IntegerType.get()),
        Types.NestedField.required(51, "PageCharset", Types.StringType.get()),
        Types.NestedField.required(52, "CodeVersion", Types.IntegerType.get()),
        Types.NestedField.required(53, "IsLink", Types.IntegerType.get()),
        Types.NestedField.required(54, "IsDownload", Types.IntegerType.get()),
        Types.NestedField.required(55, "IsNotBounce", Types.IntegerType.get()),
        Types.NestedField.required(56, "FUniqID", Types.LongType.get()),
        Types.NestedField.required(57, "OriginalURL", Types.StringType.get()),
        Types.NestedField.required(58, "HID", Types.IntegerType.get()),
        Types.NestedField.required(59, "IsOldCounter", Types.IntegerType.get()),
        Types.NestedField.required(60, "IsEvent", Types.IntegerType.get()),
        Types.NestedField.required(61, "IsParameter", Types.IntegerType.get()),
        Types.NestedField.required(62, "DontCountHits", Types.IntegerType.get()),
        Types.NestedField.required(63, "WithHash", Types.IntegerType.get()),
        Types.NestedField.required(64, "HitColor", Types.StringType.get()),
        Types.NestedField.required(65, "LocalEventTime", Types.LongType.get()),
        Types.NestedField.required(66, "Age", Types.IntegerType.get()),
        Types.NestedField.required(67, "Sex", Types.IntegerType.get()),
        Types.NestedField.required(68, "Income", Types.IntegerType.get()),
        Types.NestedField.required(69, "Interests", Types.IntegerType.get()),
        Types.NestedField.required(70, "Robotness", Types.IntegerType.get()),
        Types.NestedField.required(71, "RemoteIP", Types.IntegerType.get()),
        Types.NestedField.required(72, "WindowName", Types.IntegerType.get()),
        Types.NestedField.required(73, "OpenerName", Types.IntegerType.get()),
        Types.NestedField.required(74, "HistoryLength", Types.IntegerType.get()),
        Types.NestedField.required(75, "BrowserLanguage", Types.StringType.get()),
        Types.NestedField.required(76, "BrowserCountry", Types.StringType.get()),
        Types.NestedField.required(77, "SocialNetwork", Types.StringType.get()),
        Types.NestedField.required(78, "SocialAction", Types.StringType.get()),
        Types.NestedField.required(79, "HTTPError", Types.IntegerType.get()),
        Types.NestedField.required(80, "SendTiming", Types.IntegerType.get()),
        Types.NestedField.required(81, "DNSTiming", Types.IntegerType.get()),
        Types.NestedField.required(82, "ConnectTiming", Types.IntegerType.get()),
        Types.NestedField.required(83, "ResponseStartTiming", Types.IntegerType.get()),
        Types.NestedField.required(84, "ResponseEndTiming", Types.IntegerType.get()),
        Types.NestedField.required(85, "FetchTiming", Types.IntegerType.get()),
        Types.NestedField.required(86, "SocialSourceNetworkID", Types.IntegerType.get()),
        Types.NestedField.required(87, "SocialSourcePage", Types.StringType.get()),
        Types.NestedField.required(88, "ParamPrice", Types.LongType.get()),
        Types.NestedField.required(89, "ParamOrderID", Types.StringType.get()),
        Types.NestedField.required(90, "ParamCurrency", Types.StringType.get()),
        Types.NestedField.required(91, "ParamCurrencyID", Types.IntegerType.get()),
        Types.NestedField.required(92, "OpenstatServiceName", Types.StringType.get()),
        Types.NestedField.required(93, "OpenstatCampaignID", Types.StringType.get()),
        Types.NestedField.required(94, "OpenstatAdID", Types.StringType.get()),
        Types.NestedField.required(95, "OpenstatSourceID", Types.StringType.get()),
        Types.NestedField.required(96, "UTMSource", Types.StringType.get()),
        Types.NestedField.required(97, "UTMMedium", Types.StringType.get()),
        Types.NestedField.required(98, "UTMCampaign", Types.StringType.get()),
        Types.NestedField.required(99, "UTMContent", Types.StringType.get()),
        Types.NestedField.required(100, "UTMTerm", Types.StringType.get()),
        Types.NestedField.required(101, "FromTag", Types.StringType.get()),
        Types.NestedField.required(102, "HasGCLID", Types.IntegerType.get()),
        Types.NestedField.required(103, "RefererHash", Types.LongType.get()),
        Types.NestedField.required(104, "URLHash", Types.LongType.get()),
        Types.NestedField.required(105, "CLID", Types.IntegerType.get())
    );

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: CreateLocalIcebergTable <warehouse> <namespace> <table> <parquet-path>");
            System.err.println("  <parquet-path> can be a single .parquet file or a directory of .parquet files");
            System.exit(1);
        }

        String warehousePath = args[0];
        String namespace = args[1];
        String tableName = args[2];
        String parquetPath = args[3];

        Path parquet = Paths.get(parquetPath);
        if (!Files.exists(parquet)) {
            System.err.println("ERROR: Parquet path does not exist: " + parquetPath);
            System.exit(1);
        }

        // Create warehouse directory
        Files.createDirectories(Paths.get(warehousePath));

        // Initialize Hadoop catalog
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(conf);
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", warehousePath);
        catalog.initialize("local", properties);

        TableIdentifier tableId = TableIdentifier.of(Namespace.of(namespace), tableName);

        // Drop table if it already exists
        if (catalog.tableExists(tableId)) {
            System.out.println("Dropping existing table: " + tableId);
            catalog.dropTable(tableId, true);
        }

        // Create table
        Table table = catalog.createTable(tableId, CLICKBENCH_SCHEMA, PartitionSpec.unpartitioned());
        System.out.println("Created table: " + tableId + " at " + table.location());

        // Collect parquet files
        if (Files.isDirectory(parquet)) {
            try (Stream<Path> files = Files.list(parquet)) {
                files.filter(f -> f.toString().endsWith(".parquet"))
                    .sorted()
                    .forEach(f -> appendFile(table, f));
            }
        } else {
            appendFile(table, parquet);
        }

        // Verify
        table.refresh();
        System.out.println("Table created successfully!");
        System.out.println("  Location: " + table.location());
        System.out.println("  Snapshot: " + (table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : "none"));
        System.out.println("  Files: " + (table.currentSnapshot() != null ? table.currentSnapshot().summary().get("added-data-files") : "0"));

        catalog.close();
    }

    private static void appendFile(Table table, Path parquetFile) {
        File file = parquetFile.toFile();
        long fileSize = file.length();
        // Use file:// URI for the path
        String filePath = "file://" + parquetFile.toAbsolutePath().toString();

        DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(filePath)
            .withFileSizeInBytes(fileSize)
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(estimateRecordCount(fileSize))
            .build();

        table.newAppend().appendFile(dataFile).commit();
        System.out.println("  Appended: " + parquetFile.getFileName() + " (" + (fileSize / 1024 / 1024) + " MB)");
    }

    /**
     * Estimate record count from file size. The exact count isn't critical —
     * it's used by Iceberg for statistics, not for correctness.
     * ClickBench: ~100M rows in 14GB → ~140 bytes/row.
     */
    private static long estimateRecordCount(long fileSizeBytes) {
        return Math.max(1, fileSizeBytes / 140);
    }
}
