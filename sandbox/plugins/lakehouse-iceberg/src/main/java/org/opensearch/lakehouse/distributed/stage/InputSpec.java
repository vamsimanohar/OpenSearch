/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.stage;

import org.opensearch.lakehouse.scan.IcebergScanPlan;

import java.util.List;
import java.util.Map;

/** Describes the input source for a stage, either a scan or an exchange from upstream stages. */
public abstract sealed class InputSpec permits InputSpec.ScanInput, InputSpec.ExchangeInput {

    /** Creates a new InputSpec. */
    protected InputSpec() {}

    /** Input specification that reads from Iceberg data files. */
    public static final class ScanInput extends InputSpec {
        private final List<IcebergScanPlan.FileInfo> files;
        private final Map<String, String> storageConfig;

        /**
         * Creates a new ScanInput with the given files and storage configuration.
         *
         * @param files         the data files to scan
         * @param storageConfig storage access configuration
         */
        public ScanInput(List<IcebergScanPlan.FileInfo> files, Map<String, String> storageConfig) {
            this.files = List.copyOf(files);
            this.storageConfig = Map.copyOf(storageConfig);
        }

        /** Returns the list of files to scan. */
        public List<IcebergScanPlan.FileInfo> getFiles() { return files; }
        /** Returns the storage configuration for accessing the files. */
        public Map<String, String> getStorageConfig() { return storageConfig; }

        @Override
        public String toString() {
            return "SCAN(" + files.size() + " files)";
        }
    }

    /** Input specification that receives data from upstream stages via exchange. */
    public static final class ExchangeInput extends InputSpec {
        private final Map<StageId, String> sourceTableNames;

        /**
         * Creates a new ExchangeInput with the given source stage to table name mapping.
         *
         * @param sourceTableNames mapping of source stages to table names
         */
        public ExchangeInput(Map<StageId, String> sourceTableNames) {
            this.sourceTableNames = Map.copyOf(sourceTableNames);
        }

        /** Returns the mapping from source stage IDs to their table names. */
        public Map<StageId, String> getSourceTableNames() { return sourceTableNames; }

        @Override
        public String toString() {
            return "EXCHANGE(" + sourceTableNames + ")";
        }
    }
}
