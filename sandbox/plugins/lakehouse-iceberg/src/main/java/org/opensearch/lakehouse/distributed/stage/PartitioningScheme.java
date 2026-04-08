/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.stage;

import java.util.List;

/** Defines how data is partitioned between stages in a distributed query plan. */
public final class PartitioningScheme {
    /** The type of data partitioning strategy. */
    public enum Type {
        /** Hash-based partitioning on specified columns. */
        HASH,
        /** Broadcast data to all target partitions. */
        BROADCAST,
        /** Gather all data to a single target. */
        GATHER,
        /** No repartitioning applied. */
        NONE
    }

    private final Type type;
    private final List<String> partitionColumns;
    private final int partitionCount;

    private PartitioningScheme(Type type, List<String> partitionColumns, int partitionCount) {
        this.type = type;
        this.partitionColumns = List.copyOf(partitionColumns);
        this.partitionCount = partitionCount;
    }

    /**
     * Creates a hash partitioning scheme on the given columns.
     *
     * @param columns        columns to hash on
     * @param partitionCount number of target partitions
     * @return the hash partitioning scheme
     */
    public static PartitioningScheme hash(List<String> columns, int partitionCount) {
        return new PartitioningScheme(Type.HASH, columns, partitionCount);
    }

    /** Creates a broadcast partitioning scheme that replicates data to all targets. */
    public static PartitioningScheme broadcast() {
        return new PartitioningScheme(Type.BROADCAST, List.of(), 0);
    }

    /** Creates a gather partitioning scheme that sends all data to a single target. */
    public static PartitioningScheme gather() {
        return new PartitioningScheme(Type.GATHER, List.of(), 1);
    }

    /** Creates a no-op partitioning scheme with no repartitioning. */
    public static PartitioningScheme none() {
        return new PartitioningScheme(Type.NONE, List.of(), 0);
    }

    /** Returns the partitioning type. */
    public Type getType() { return type; }
    /** Returns the columns used for partitioning. */
    public List<String> getPartitionColumns() { return partitionColumns; }
    /** Returns the number of partitions. */
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
