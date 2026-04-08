/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.exchange;

import org.opensearch.lakehouse.distributed.stage.PartitioningScheme;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ArrowPartitionerTests extends OpenSearchTestCase {

    public void testGatherAllToPartitionZero() {
        List<byte[]> outputs = List.of(new byte[]{1}, new byte[]{2}, new byte[]{3});
        List<List<byte[]>> result = ArrowPartitioner.partition(outputs, PartitioningScheme.gather(), 3);
        assertEquals(3, result.get(0).size());
        assertEquals(0, result.get(1).size());
        assertEquals(0, result.get(2).size());
    }

    public void testBroadcastToAll() {
        List<byte[]> outputs = List.of(new byte[]{1}, new byte[]{2});
        List<List<byte[]>> result = ArrowPartitioner.partition(outputs, PartitioningScheme.broadcast(), 3);
        assertEquals(2, result.get(0).size());
        assertEquals(2, result.get(1).size());
        assertEquals(2, result.get(2).size());
    }

    public void testHashRoundRobin() {
        List<byte[]> outputs = List.of(new byte[]{1}, new byte[]{2}, new byte[]{3}, new byte[]{4});
        List<List<byte[]>> result = ArrowPartitioner.partition(
            outputs, PartitioningScheme.hash(List.of("col"), 3), 3);
        // 4 outputs across 3 partitions: 0->0, 1->1, 2->2, 3->0
        assertEquals(2, result.get(0).size());
        assertEquals(1, result.get(1).size());
        assertEquals(1, result.get(2).size());
    }

    public void testEmptyOutputs() {
        List<byte[]> outputs = List.of();
        List<List<byte[]>> result = ArrowPartitioner.partition(outputs, PartitioningScheme.gather(), 2);
        assertEquals(0, result.get(0).size());
        assertEquals(0, result.get(1).size());
    }

    public void testSingleOutputGather() {
        List<byte[]> outputs = List.of(new byte[]{1, 2, 3});
        List<List<byte[]>> result = ArrowPartitioner.partition(outputs, PartitioningScheme.gather(), 1);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).size());
        assertEquals(3, result.get(0).get(0).length);
    }
}
