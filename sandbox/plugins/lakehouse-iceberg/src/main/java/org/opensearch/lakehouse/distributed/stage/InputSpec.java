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

public abstract sealed class InputSpec permits InputSpec.ScanInput, InputSpec.ExchangeInput {

    public static final class ScanInput extends InputSpec {
        private final List<IcebergScanPlan.FileInfo> files;
        private final Map<String, String> storageConfig;

        public ScanInput(List<IcebergScanPlan.FileInfo> files, Map<String, String> storageConfig) {
            this.files = List.copyOf(files);
            this.storageConfig = Map.copyOf(storageConfig);
        }

        public List<IcebergScanPlan.FileInfo> getFiles() { return files; }
        public Map<String, String> getStorageConfig() { return storageConfig; }

        @Override
        public String toString() {
            return "SCAN(" + files.size() + " files)";
        }
    }

    public static final class ExchangeInput extends InputSpec {
        private final Map<StageId, String> sourceTableNames;

        public ExchangeInput(Map<StageId, String> sourceTableNames) {
            this.sourceTableNames = Map.copyOf(sourceTableNames);
        }

        public Map<StageId, String> getSourceTableNames() { return sourceTableNames; }

        @Override
        public String toString() {
            return "EXCHANGE(" + sourceTableNames + ")";
        }
    }
}
