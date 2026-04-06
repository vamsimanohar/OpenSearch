/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link DistributedResultMerger}.
 *
 * <p>Verifies correct merge behavior for scan-only, global aggregate,
 * and grouped aggregate queries with various data types and edge cases.</p>
 */
public class DistributedResultMergerTests extends OpenSearchTestCase {

    // ---- SCAN_ONLY tests ----

    public void testScanOnlyMergeConcatenatesRows() {
        LakehouseWorkerResponse r1 = response(
            new String[]{"id", "name"},
            new Object[][]{{1, "alice"}, {2, "bob"}}
        );
        LakehouseWorkerResponse r2 = response(
            new String[]{"id", "name"},
            new Object[][]{{3, "carol"}}
        );

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2), DistributionPlan.scanOnly());

        assertEquals(3, result.size());
        assertEquals(1, result.get(0)[0]);
        assertEquals("alice", result.get(0)[1]);
        assertEquals(2, result.get(1)[0]);
        assertEquals("bob", result.get(1)[1]);
        assertEquals(3, result.get(2)[0]);
        assertEquals("carol", result.get(2)[1]);
    }

    public void testScanOnlyMergeEmptyResponses() {
        List<Object[]> result = DistributedResultMerger.merge(List.of(), DistributionPlan.scanOnly());
        assertTrue(result.isEmpty());
    }

    public void testScanOnlyMergeNullResponses() {
        List<Object[]> result = DistributedResultMerger.merge(null, DistributionPlan.scanOnly());
        assertTrue(result.isEmpty());
    }

    public void testScanOnlyMergeWithEmptyWorkerResponse() {
        LakehouseWorkerResponse r1 = response(
            new String[]{"id"},
            new Object[][]{{1}}
        );
        LakehouseWorkerResponse r2 = response(
            new String[]{"id"},
            new Object[0][]
        );
        LakehouseWorkerResponse r3 = response(
            new String[]{"id"},
            new Object[][]{{2}}
        );

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2, r3), DistributionPlan.scanOnly());
        assertEquals(2, result.size());
    }

    // ---- GLOBAL_AGGREGATE tests ----

    public void testGlobalAggregateCountFromThreeWorkers() {
        // SELECT COUNT(*) FROM t — each worker returns its partial count
        DistributionPlan plan = DistributionPlan.globalAggregate(List.of(
            new DistributionPlan.AggMergeInfo(0, DistributionPlan.MergeOp.SUM)
        ));

        LakehouseWorkerResponse r1 = response(new String[]{"count"}, new Object[][]{{10L}});
        LakehouseWorkerResponse r2 = response(new String[]{"count"}, new Object[][]{{25L}});
        LakehouseWorkerResponse r3 = response(new String[]{"count"}, new Object[][]{{15L}});

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2, r3), plan);

        assertEquals(1, result.size());
        assertEquals(50L, result.get(0)[0]);
    }

    public void testGlobalAggregateSumAndCount() {
        // SELECT COUNT(*), SUM(amount) FROM t
        DistributionPlan plan = DistributionPlan.globalAggregate(List.of(
            new DistributionPlan.AggMergeInfo(0, DistributionPlan.MergeOp.SUM),
            new DistributionPlan.AggMergeInfo(1, DistributionPlan.MergeOp.SUM)
        ));

        LakehouseWorkerResponse r1 = response(new String[]{"count", "sum"}, new Object[][]{{5L, 100.0}});
        LakehouseWorkerResponse r2 = response(new String[]{"count", "sum"}, new Object[][]{{3L, 75.5}});

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2), plan);

        assertEquals(1, result.size());
        assertEquals(8L, result.get(0)[0]);
        assertEquals(175.5, (Double) result.get(0)[1], 0.001);
    }

    public void testGlobalAggregateMinMax() {
        // SELECT MIN(amount), MAX(amount) FROM t
        DistributionPlan plan = DistributionPlan.globalAggregate(List.of(
            new DistributionPlan.AggMergeInfo(0, DistributionPlan.MergeOp.MIN),
            new DistributionPlan.AggMergeInfo(1, DistributionPlan.MergeOp.MAX)
        ));

        LakehouseWorkerResponse r1 = response(new String[]{"min", "max"}, new Object[][]{{5.0, 100.0}});
        LakehouseWorkerResponse r2 = response(new String[]{"min", "max"}, new Object[][]{{3.0, 200.0}});
        LakehouseWorkerResponse r3 = response(new String[]{"min", "max"}, new Object[][]{{8.0, 150.0}});

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2, r3), plan);

        assertEquals(1, result.size());
        assertEquals(3.0, (Double) result.get(0)[0], 0.001);
        assertEquals(200.0, (Double) result.get(0)[1], 0.001);
    }

    public void testGlobalAggregateEmptyResponses() {
        DistributionPlan plan = DistributionPlan.globalAggregate(List.of(
            new DistributionPlan.AggMergeInfo(0, DistributionPlan.MergeOp.SUM)
        ));

        List<Object[]> result = DistributedResultMerger.merge(List.of(), plan);
        assertTrue(result.isEmpty());
    }

    public void testGlobalAggregateAllEmptyWorkers() {
        DistributionPlan plan = DistributionPlan.globalAggregate(List.of(
            new DistributionPlan.AggMergeInfo(0, DistributionPlan.MergeOp.SUM)
        ));

        LakehouseWorkerResponse r1 = response(new String[]{"count"}, new Object[0][]);
        LakehouseWorkerResponse r2 = response(new String[]{"count"}, new Object[0][]);

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2), plan);
        assertTrue(result.isEmpty());
    }

    // ---- GROUPED_AGGREGATE tests ----

    public void testGroupedAggregateWithOverlappingGroups() {
        // SELECT region, COUNT(*) FROM t GROUP BY region
        DistributionPlan plan = DistributionPlan.groupedAggregate(
            new int[]{0},
            List.of(new DistributionPlan.AggMergeInfo(1, DistributionPlan.MergeOp.SUM))
        );

        // Worker 1: east=5, west=3
        LakehouseWorkerResponse r1 = response(
            new String[]{"region", "count"},
            new Object[][]{{"east", 5L}, {"west", 3L}}
        );
        // Worker 2: east=7, north=2
        LakehouseWorkerResponse r2 = response(
            new String[]{"region", "count"},
            new Object[][]{{"east", 7L}, {"north", 2L}}
        );

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2), plan);

        assertEquals(3, result.size());

        // Find each group by region
        Object[] east = findGroupByKey(result, 0, "east");
        Object[] west = findGroupByKey(result, 0, "west");
        Object[] north = findGroupByKey(result, 0, "north");

        assertNotNull("Expected 'east' group", east);
        assertNotNull("Expected 'west' group", west);
        assertNotNull("Expected 'north' group", north);

        assertEquals(12L, east[1]);   // 5 + 7
        assertEquals(3L, west[1]);    // 3
        assertEquals(2L, north[1]);   // 2
    }

    public void testGroupedAggregateWithMultipleAggFunctions() {
        // SELECT region, COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM t GROUP BY region
        DistributionPlan plan = DistributionPlan.groupedAggregate(
            new int[]{0},
            List.of(
                new DistributionPlan.AggMergeInfo(1, DistributionPlan.MergeOp.SUM),   // COUNT
                new DistributionPlan.AggMergeInfo(2, DistributionPlan.MergeOp.SUM),   // SUM
                new DistributionPlan.AggMergeInfo(3, DistributionPlan.MergeOp.MIN),   // MIN
                new DistributionPlan.AggMergeInfo(4, DistributionPlan.MergeOp.MAX)    // MAX
            )
        );

        LakehouseWorkerResponse r1 = response(
            new String[]{"region", "count", "sum", "min", "max"},
            new Object[][]{{"east", 3L, 150.0, 10.0, 80.0}}
        );
        LakehouseWorkerResponse r2 = response(
            new String[]{"region", "count", "sum", "min", "max"},
            new Object[][]{{"east", 2L, 100.0, 5.0, 90.0}}
        );

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2), plan);

        assertEquals(1, result.size());
        Object[] row = result.get(0);
        assertEquals("east", row[0]);
        assertEquals(5L, row[1]);            // COUNT: 3 + 2
        assertEquals(250.0, (Double) row[2], 0.001);  // SUM: 150 + 100
        assertEquals(5.0, (Double) row[3], 0.001);    // MIN: min(10, 5)
        assertEquals(90.0, (Double) row[4], 0.001);   // MAX: max(80, 90)
    }

    public void testGroupedAggregateNoOverlap() {
        // Workers return completely different groups
        DistributionPlan plan = DistributionPlan.groupedAggregate(
            new int[]{0},
            List.of(new DistributionPlan.AggMergeInfo(1, DistributionPlan.MergeOp.SUM))
        );

        LakehouseWorkerResponse r1 = response(
            new String[]{"region", "count"},
            new Object[][]{{"east", 5L}}
        );
        LakehouseWorkerResponse r2 = response(
            new String[]{"region", "count"},
            new Object[][]{{"west", 3L}}
        );

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2), plan);

        assertEquals(2, result.size());
    }

    public void testGroupedAggregateMultipleGroupKeys() {
        // SELECT region, name, COUNT(*) FROM t GROUP BY region, name
        DistributionPlan plan = DistributionPlan.groupedAggregate(
            new int[]{0, 1},
            List.of(new DistributionPlan.AggMergeInfo(2, DistributionPlan.MergeOp.SUM))
        );

        LakehouseWorkerResponse r1 = response(
            new String[]{"region", "name", "count"},
            new Object[][]{{"east", "alice", 3L}, {"east", "bob", 2L}}
        );
        LakehouseWorkerResponse r2 = response(
            new String[]{"region", "name", "count"},
            new Object[][]{{"east", "alice", 1L}, {"west", "carol", 4L}}
        );

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2), plan);

        assertEquals(3, result.size());

        // Find the merged (east, alice) group
        Object[] eastAlice = null;
        for (Object[] row : result) {
            if ("east".equals(row[0]) && "alice".equals(row[1])) {
                eastAlice = row;
                break;
            }
        }
        assertNotNull("Expected (east, alice) group", eastAlice);
        assertEquals(4L, eastAlice[2]); // 3 + 1
    }

    public void testGroupedAggregateEmptyResponses() {
        DistributionPlan plan = DistributionPlan.groupedAggregate(
            new int[]{0},
            List.of(new DistributionPlan.AggMergeInfo(1, DistributionPlan.MergeOp.SUM))
        );

        List<Object[]> result = DistributedResultMerger.merge(List.of(), plan);
        assertTrue(result.isEmpty());
    }

    // ---- Numeric type handling tests ----

    public void testMergeOpSumWithIntegers() {
        Object result = DistributedResultMerger.applyMergeOp(
            DistributionPlan.MergeOp.SUM, 10L, 20L
        );
        assertEquals(30L, result);
    }

    public void testMergeOpSumWithDoubles() {
        Object result = DistributedResultMerger.applyMergeOp(
            DistributionPlan.MergeOp.SUM, 10.5, 20.3
        );
        assertEquals(30.8, (Double) result, 0.001);
    }

    public void testMergeOpSumWithMixedTypes() {
        // Long + Double should produce Double
        Object result = DistributedResultMerger.applyMergeOp(
            DistributionPlan.MergeOp.SUM, 10L, 20.5
        );
        assertEquals(30.5, (Double) result, 0.001);
    }

    public void testMergeOpMinWithLongs() {
        Object result = DistributedResultMerger.applyMergeOp(
            DistributionPlan.MergeOp.MIN, 10L, 5L
        );
        assertEquals(5L, result);
    }

    public void testMergeOpMaxWithLongs() {
        Object result = DistributedResultMerger.applyMergeOp(
            DistributionPlan.MergeOp.MAX, 10L, 20L
        );
        assertEquals(20L, result);
    }

    public void testMergeOpWithNullAccumulated() {
        Object result = DistributedResultMerger.applyMergeOp(
            DistributionPlan.MergeOp.SUM, null, 42L
        );
        assertEquals(42L, result);
    }

    public void testMergeOpWithNullIncoming() {
        Object result = DistributedResultMerger.applyMergeOp(
            DistributionPlan.MergeOp.SUM, 42L, null
        );
        assertEquals(42L, result);
    }

    public void testMergeOpWithBothNull() {
        Object result = DistributedResultMerger.applyMergeOp(
            DistributionPlan.MergeOp.SUM, null, null
        );
        assertNull(result);
    }

    // ---- UNSUPPORTED plan throws ----

    public void testUnsupportedPlanThrows() {
        LakehouseWorkerResponse r = response(new String[]{"x"}, new Object[][]{{1}});
        expectThrows(
            IllegalStateException.class,
            () -> DistributedResultMerger.merge(List.of(r), DistributionPlan.unsupported())
        );
    }

    // ---- Helper methods ----

    private static LakehouseWorkerResponse response(String[] columnNames, Object[][] rows) {
        return new LakehouseWorkerResponse(rows, columnNames);
    }

    private static Object[] findGroupByKey(List<Object[]> rows, int keyCol, Object keyValue) {
        for (Object[] row : rows) {
            if (keyValue.equals(row[keyCol])) {
                return row;
            }
        }
        return null;
    }
}
