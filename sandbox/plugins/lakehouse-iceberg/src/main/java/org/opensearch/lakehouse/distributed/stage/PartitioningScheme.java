/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.stage;

import java.util.List;

public final class PartitioningScheme {
    public enum Type { HASH, BROADCAST, GATHER, NONE }

    private final Type type;
    private final List<String> partitionColumns;
    private final int partitionCount;

    private PartitioningScheme(Type type, List<String> partitionColumns, int partitionCount) {
        this.type = type;
        this.partitionColumns = List.copyOf(partitionColumns);
        this.partitionCount = partitionCount;
    }

    public static PartitioningScheme hash(List<String> columns, int partitionCount) {
        return new PartitioningScheme(Type.HASH, columns, partitionCount);
    }

    public static PartitioningScheme broadcast() {
        return new PartitioningScheme(Type.BROADCAST, List.of(), 0);
    }

    public static PartitioningScheme gather() {
        return new PartitioningScheme(Type.GATHER, List.of(), 1);
    }

    public static PartitioningScheme none() {
        return new PartitioningScheme(Type.NONE, List.of(), 0);
    }

    public Type getType() { return type; }
    public List<String> getPartitionColumns() { return partitionColumns; }
    public int getPartitionCount() { return partitionCount; }

    @Override
    public String toString() {
        return switch (type) {
            case HASH -> "HASH(" + String.join(", ", partitionColumns) + ", " + partitionCount + ")";
            case BROADCAST -> "BROADCAST";
            case GATHER -> "GATHER";
            case NONE -> "NONE";
        };
    }
}
