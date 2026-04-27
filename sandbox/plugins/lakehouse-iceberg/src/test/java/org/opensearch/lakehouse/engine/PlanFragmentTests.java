/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.engine;

import org.opensearch.test.OpenSearchTestCase;

public class PlanFragmentTests extends OpenSearchTestCase {

    public void testLeafFragment() {
        PlanFragment frag = PlanFragment.leaf(0, "SELECT * FROM t", ExchangeType.GATHER, null);
        assertEquals(0, frag.getStageId());
        assertEquals("SELECT * FROM t", frag.getSql());
        assertEquals(ExchangeType.GATHER, frag.getOutputExchange());
        assertNull(frag.getHashColumns());
        assertTrue(frag.isLeaf());
    }

    public void testLeafFragmentWithHashColumns() {
        int[] hashCols = new int[] { 0, 1 };
        PlanFragment frag = PlanFragment.leaf(0, "SELECT a, b FROM t GROUP BY a, b", ExchangeType.HASH, hashCols);
        assertEquals(ExchangeType.HASH, frag.getOutputExchange());
        assertArrayEquals(new int[] { 0, 1 }, frag.getHashColumns());
        assertTrue(frag.isLeaf());
    }

    public void testIntermediateFragment() {
        PlanFragment frag = PlanFragment.intermediate(1, "SELECT * FROM __exchange_input__", ExchangeType.NONE, null);
        assertEquals(1, frag.getStageId());
        assertEquals("SELECT * FROM __exchange_input__", frag.getSql());
        assertEquals(ExchangeType.NONE, frag.getOutputExchange());
        assertNull(frag.getHashColumns());
        assertFalse(frag.isLeaf());
    }

    public void testIntermediateFragmentWithGatherOutput() {
        PlanFragment frag = PlanFragment.intermediate(
            1,
            "SELECT \"col_0\", SUM(\"col_1\") FROM __exchange_input__ GROUP BY \"col_0\" ORDER BY 2 DESC LIMIT 10",
            ExchangeType.GATHER,
            null
        );
        assertEquals(1, frag.getStageId());
        assertEquals(ExchangeType.GATHER, frag.getOutputExchange());
        assertFalse(frag.isLeaf());
    }

    public void testToStringLeaf() {
        PlanFragment frag = PlanFragment.leaf(0, "SELECT x FROM t", ExchangeType.GATHER, null);
        String str = frag.toString();
        assertTrue(str.contains("stageId=0"));
        assertTrue(str.contains("leaf=true"));
        assertTrue(str.contains("GATHER"));
        assertTrue(str.contains("SELECT x FROM t"));
    }

    public void testToStringIntermediate() {
        PlanFragment frag = PlanFragment.intermediate(2, "SELECT * FROM __exchange_input__", ExchangeType.NONE, null);
        String str = frag.toString();
        assertTrue(str.contains("stageId=2"));
        assertTrue(str.contains("leaf=false"));
        assertTrue(str.contains("NONE"));
    }
}
