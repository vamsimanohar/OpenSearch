/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.engine;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class FragmentedPlanTests extends OpenSearchTestCase {

    public void testDistributedPlan() {
        PlanFragment leaf = PlanFragment.leaf(0, "SELECT * FROM t", ExchangeType.GATHER, null);
        PlanFragment fin = PlanFragment.intermediate(1, "SELECT * FROM __exchange_input__", ExchangeType.NONE, null);
        FragmentedPlan plan = FragmentedPlan.distributed(List.of(leaf, fin));

        assertFalse(plan.isSingleNode());
        assertEquals(2, plan.getStageCount());
        assertEquals(2, plan.getStages().size());
        assertSame(leaf, plan.getLeafStage());
        assertSame(fin, plan.getFinalStage());
    }

    public void testThreeStageDistributedPlan() {
        PlanFragment leaf = PlanFragment.leaf(0, "SELECT a, COUNT(*) FROM t GROUP BY a", ExchangeType.HASH, new int[] { 0 });
        PlanFragment mid = PlanFragment.intermediate(1, "SELECT * FROM __exchange_input__", ExchangeType.GATHER, null);
        PlanFragment fin = PlanFragment.intermediate(2, "SELECT * FROM __exchange_input__", ExchangeType.NONE, null);
        FragmentedPlan plan = FragmentedPlan.distributed(List.of(leaf, mid, fin));

        assertFalse(plan.isSingleNode());
        assertEquals(3, plan.getStageCount());
        assertSame(leaf, plan.getLeafStage());
        assertSame(fin, plan.getFinalStage());
    }

    public void testSingleNodePlan() {
        FragmentedPlan plan = FragmentedPlan.singleNode();
        assertTrue(plan.isSingleNode());
        assertEquals(0, plan.getStageCount());
        assertTrue(plan.getStages().isEmpty());
        assertNull(plan.getLeafStage());
        assertNull(plan.getFinalStage());
    }

    public void testDistributedPlanRequiresNonEmptyStages() {
        expectThrows(IllegalArgumentException.class, () -> FragmentedPlan.distributed(List.of()));
    }

    public void testDistributedPlanRequiresNonNullStages() {
        expectThrows(IllegalArgumentException.class, () -> FragmentedPlan.distributed(null));
    }

    public void testStagesListIsImmutable() {
        PlanFragment leaf = PlanFragment.leaf(0, "sql", ExchangeType.GATHER, null);
        FragmentedPlan plan = FragmentedPlan.distributed(List.of(leaf));
        expectThrows(UnsupportedOperationException.class, () -> plan.getStages().add(
            PlanFragment.intermediate(1, "more sql", ExchangeType.NONE, null)
        ));
    }

    public void testToStringSingleNode() {
        FragmentedPlan plan = FragmentedPlan.singleNode();
        assertEquals("FragmentedPlan{SINGLE_NODE}", plan.toString());
    }

    public void testToStringDistributed() {
        PlanFragment leaf = PlanFragment.leaf(0, "sql", ExchangeType.GATHER, null);
        PlanFragment fin = PlanFragment.intermediate(1, "more sql", ExchangeType.NONE, null);
        FragmentedPlan plan = FragmentedPlan.distributed(List.of(leaf, fin));
        String str = plan.toString();
        assertTrue(str.contains("stages=2"));
    }
}
