/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import org.opensearch.ppl.action.PPLResponse;

/**
 * PPL integration tests for the lakehouse plugin.
 * <p>
 * Each test executes a PPL query end-to-end: PPL parsing, Calcite planning,
 * Iceberg schema contribution, predicate pushdown, DataFusion native execution,
 * and Arrow result streaming back to Java.
 */
public class LakehousePplIT extends LakehouseIntegTestBase {

    // ---- Basic source queries ----

    public void testBasicSource() {
        PPLResponse r = executePpl("source = " + INDEX_NAME);
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testHead() {
        PPLResponse r = executePpl("source = " + INDEX_NAME + " | head 10");
        assertNoError(r);
        assertTrue("Should have <= 10 rows", r.getRows().size() <= 10);
    }

    public void testFieldProjection() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | fields vendorid, passenger_count | head 5"
        );
        assertNoError(r);
        assertEquals(2, r.getColumns().size());
    }

    // ---- Filter queries ----

    public void testWhereEquality() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | where vendorid = 1 | head 10"
        );
        assertNoError(r);
    }

    public void testWhereRange() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | where fare_amount > 50.0 | head 10"
        );
        assertNoError(r);
    }

    public void testWhereLike() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | where store_and_fwd_flag LIKE 'Y' | head 10"
        );
        assertNoError(r);
    }

    // ---- Aggregation queries ----

    public void testStatsCount() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | stats count() as cnt"
        );
        assertNoError(r);
        assertRowCount(r, 1);
    }

    public void testStatsCountByGroup() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | stats count() as cnt by vendorid"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testStatsSumByGroup() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | stats sum(fare_amount) as total_fare by vendorid"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testStatsAvgByGroup() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | stats avg(trip_distance) as avg_distance by vendorid"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testStatsMinMax() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | stats min(fare_amount) as min_fare, max(fare_amount) as max_fare"
        );
        assertNoError(r);
        assertRowCount(r, 1);
        assertEquals(2, r.getColumns().size());
    }

    public void testStatsCountDistinct() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | stats distinct_count(vendorid) as dc"
        );
        assertNoError(r);
        assertRowCount(r, 1);
    }

    // ---- Sort queries ----

    public void testSortAsc() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | sort fare_amount | head 10"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testSortDesc() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | sort - fare_amount | head 10"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    // ---- Filter + null check ----

    public void testWhereIsNotNull() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | where isnotnull(vendorid) | head 10"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testWhereCompound() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | where vendorid = 1 AND fare_amount > 20.0 | head 10"
        );
        assertNoError(r);
    }

    // ---- Eval (computed columns) ----

    public void testEval() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | eval total = fare_amount + tip_amount | head 5"
        );
        assertNoError(r);
    }

    // ---- IN filter ----

    public void testWhereIn() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | where vendorid IN (1, 2) | head 20"
        );
        assertNoError(r);
    }

    // ---- Chained aggregation + sort ----

    public void testStatsGroupBySort() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | stats count() as cnt by vendorid | sort - cnt"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    // ---- Rename ----

    public void testRename() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | rename vendorid as vendor | head 5"
        );
        assertNoError(r);
    }

    // ---- Dedup ----

    public void testDedup() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME + " | dedup vendorid | head 10"
        );
        assertNoError(r);
    }

    // ---- Chained operations ----

    public void testChainedFieldsWhereStats() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME
                + " | fields vendorid, fare_amount"
                + " | where fare_amount > 0"
                + " | stats count() as cnt"
        );
        assertNoError(r);
        assertRowCount(r, 1);
    }

    // ---- Multiple stats aggregations ----

    public void testMultipleStatsAggregations() {
        PPLResponse r = executePpl(
            "source = " + INDEX_NAME
                + " | stats count() as cnt, sum(fare_amount) as total_fare, avg(tip_amount) as avg_tip"
        );
        assertNoError(r);
        assertRowCount(r, 1);
        assertEquals(3, r.getColumns().size());
    }
}
