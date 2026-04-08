/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.stage;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class StageDAGTests extends OpenSearchTestCase {

    public void testTwoStageAggregation() {
        StageId scan = new StageId(0);
        StageId agg = new StageId(1);

        Stage scanStage = new Stage(scan,
            "SELECT payment_type, SUM(amount) AS ps FROM __input GROUP BY payment_type",
            "orders",
            new InputSpec.ScanInput(List.of(), Map.of()),
            PartitioningScheme.gather(),
            Stage.StageType.SCAN, List.of());

        Stage aggStage = new Stage(agg,
            "SELECT payment_type, SUM(ps) AS total FROM __input GROUP BY payment_type",
            "__partial",
            new InputSpec.ExchangeInput(Map.of(scan, "__input")),
            PartitioningScheme.gather(),
            Stage.StageType.FINAL, List.of(scan));

        StageDAG dag = new StageDAG(List.of(scanStage, aggStage), agg);

        assertEquals(2, dag.stageCount());
        assertEquals(agg, dag.getRootStageId());
        assertTrue(scanStage.isLeaf());
        assertFalse(aggStage.isLeaf());

        List<Stage> topo = dag.topologicalOrder();
        assertEquals(scan, topo.get(0).getId());
        assertEquals(agg, topo.get(1).getId());

        assertEquals(1, dag.getLeafStages().size());
        assertEquals(scan, dag.getLeafStages().get(0).getId());
    }

    public void testThreeStageJoin() {
        StageId scanFact = new StageId(0);
        StageId scanDim = new StageId(1);
        StageId join = new StageId(2);

        Stage factStage = new Stage(scanFact,
            "SELECT * FROM orders", "orders",
            new InputSpec.ScanInput(List.of(), Map.of()),
            PartitioningScheme.hash(List.of("customer_id"), 3),
            Stage.StageType.SCAN, List.of());

        Stage dimStage = new Stage(scanDim,
            "SELECT * FROM customers", "customers",
            new InputSpec.ScanInput(List.of(), Map.of()),
            PartitioningScheme.broadcast(),
            Stage.StageType.SCAN, List.of());

        Stage joinStage = new Stage(join,
            "SELECT o.order_id, c.name FROM __orders o JOIN __customers c ON o.customer_id = c.customer_id",
            null,
            new InputSpec.ExchangeInput(Map.of(scanFact, "__orders", scanDim, "__customers")),
            PartitioningScheme.gather(),
            Stage.StageType.FINAL, List.of(scanFact, scanDim));

        StageDAG dag = new StageDAG(List.of(factStage, dimStage, joinStage), join);

        assertEquals(3, dag.stageCount());
        List<Stage> leaves = dag.getLeafStages();
        assertEquals(2, leaves.size());

        List<Stage> topo = dag.topologicalOrder();
        assertEquals(join, topo.get(2).getId()); // join must be last
        assertTrue(topo.indexOf(dag.getStage(scanFact)) < topo.indexOf(dag.getStage(join)));
        assertTrue(topo.indexOf(dag.getStage(scanDim)) < topo.indexOf(dag.getStage(join)));
    }

    public void testExplainMap() {
        StageId s0 = new StageId(0);
        Stage stage = new Stage(s0, "SELECT 1", "t",
            new InputSpec.ScanInput(List.of(), Map.of()),
            PartitioningScheme.none(), Stage.StageType.SCAN, List.of());
        StageDAG dag = new StageDAG(List.of(stage), s0);

        Map<String, Object> explain = dag.toExplainMap();
        assertEquals(1, explain.get("stageCount"));
        assertEquals("Stage-0", explain.get("root"));
    }

    public void testStageIdEquality() {
        StageId a = new StageId(5);
        StageId b = new StageId(5);
        StageId c = new StageId(6);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    public void testPartitioningSchemeToString() {
        assertEquals("GATHER", PartitioningScheme.gather().toString());
        assertEquals("BROADCAST", PartitioningScheme.broadcast().toString());
        assertEquals("NONE", PartitioningScheme.none().toString());
        assertEquals("HASH(col_a, col_b, 4)", PartitioningScheme.hash(List.of("col_a", "col_b"), 4).toString());
    }
}
