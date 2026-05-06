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
 * SQL integration tests for the lakehouse plugin.
 * <p>
 * Each test executes a SQL query end-to-end: SQL parsing, Calcite planning,
 * Iceberg schema contribution, predicate pushdown, DataFusion native execution,
 * and Arrow result streaming back to Java.
 */
public class LakehouseSqlIT extends LakehouseIntegTestBase {

    // ---- Basic queries ----

    public void testSelectCountStar() {
        PPLResponse r = executeSql("SELECT COUNT(*) FROM " + INDEX_NAME);
        assertNoError(r);
        assertRowCount(r, 1);
        assertMinRowCount(r, 1);
    }

    public void testSelectStarWithLimit() {
        PPLResponse r = executeSql("SELECT * FROM " + INDEX_NAME + " LIMIT 10");
        assertNoError(r);
        assertMinRowCount(r, 1);
        assertTrue("Should have <= 10 rows", r.getRows().size() <= 10);
    }

    public void testColumnProjection() {
        PPLResponse r = executeSql(
            "SELECT vendorid, passenger_count, fare_amount FROM " + INDEX_NAME + " LIMIT 5"
        );
        assertNoError(r);
        assertEquals(3, r.getColumns().size());
        assertTrue("Should have <= 5 rows", r.getRows().size() <= 5);
    }

    public void testSelectDistinct() {
        PPLResponse r = executeSql("SELECT DISTINCT vendorid FROM " + INDEX_NAME);
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testCountDistinct() {
        PPLResponse r = executeSql("SELECT COUNT(DISTINCT vendorid) FROM " + INDEX_NAME);
        assertNoError(r);
        assertRowCount(r, 1);
    }

    // ---- Aggregation queries ----

    public void testGroupByCount() {
        PPLResponse r = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt FROM " + INDEX_NAME + " GROUP BY vendorid"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
        assertEquals(2, r.getColumns().size());
    }

    public void testGroupBySum() {
        PPLResponse r = executeSql(
            "SELECT vendorid, SUM(fare_amount) AS total_fare FROM " + INDEX_NAME + " GROUP BY vendorid"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testGroupByAvg() {
        PPLResponse r = executeSql(
            "SELECT vendorid, AVG(trip_distance) AS avg_distance FROM " + INDEX_NAME + " GROUP BY vendorid"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testMinMax() {
        PPLResponse r = executeSql(
            "SELECT MIN(fare_amount), MAX(fare_amount) FROM " + INDEX_NAME
        );
        assertNoError(r);
        assertRowCount(r, 1);
        assertEquals(2, r.getColumns().size());
    }

    // ---- Filter queries ----

    public void testWhereEquality() {
        PPLResponse r = executeSql(
            "SELECT * FROM " + INDEX_NAME + " WHERE vendorid = 1 LIMIT 10"
        );
        assertNoError(r);
        // May have 0 rows if no vendorid=1, but shouldn't error
    }

    public void testWhereRange() {
        PPLResponse r = executeSql(
            "SELECT vendorid, fare_amount FROM " + INDEX_NAME + " WHERE fare_amount > 50.0 LIMIT 10"
        );
        assertNoError(r);
    }

    public void testWhereLike() {
        PPLResponse r = executeSql(
            "SELECT store_and_fwd_flag FROM " + INDEX_NAME + " WHERE store_and_fwd_flag LIKE 'Y%' LIMIT 10"
        );
        assertNoError(r);
    }

    public void testWhereIsNotNull() {
        PPLResponse r = executeSql(
            "SELECT vendorid FROM " + INDEX_NAME + " WHERE vendorid IS NOT NULL LIMIT 10"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testWhereCompound() {
        PPLResponse r = executeSql(
            "SELECT vendorid, fare_amount FROM " + INDEX_NAME
                + " WHERE vendorid = 1 AND fare_amount > 20.0 LIMIT 10"
        );
        assertNoError(r);
    }

    public void testWhereIn() {
        PPLResponse r = executeSql(
            "SELECT vendorid FROM " + INDEX_NAME + " WHERE vendorid IN (1, 2) LIMIT 20"
        );
        assertNoError(r);
    }

    // ---- Sort queries ----

    public void testOrderByAsc() {
        PPLResponse r = executeSql(
            "SELECT fare_amount FROM " + INDEX_NAME + " ORDER BY fare_amount ASC LIMIT 10"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testOrderByDesc() {
        PPLResponse r = executeSql(
            "SELECT fare_amount FROM " + INDEX_NAME + " ORDER BY fare_amount DESC LIMIT 10"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    // ---- Advanced queries ----

    public void testHaving() {
        PPLResponse r = executeSql(
            "SELECT payment_type, COUNT(*) AS cnt FROM " + INDEX_NAME
                + " GROUP BY payment_type HAVING COUNT(*) > 10"
        );
        assertNoError(r);
    }

    public void testGroupBySortLimit() {
        PPLResponse r = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt FROM " + INDEX_NAME
                + " GROUP BY vendorid ORDER BY cnt DESC LIMIT 5"
        );
        assertNoError(r);
        assertMinRowCount(r, 1);
    }

    public void testArithmetic() {
        PPLResponse r = executeSql(
            "SELECT fare_amount + tip_amount AS total FROM " + INDEX_NAME + " LIMIT 5"
        );
        assertNoError(r);
    }

    public void testUpperFunction() {
        PPLResponse r = executeSql(
            "SELECT UPPER(store_and_fwd_flag) FROM " + INDEX_NAME + " LIMIT 5"
        );
        assertNoError(r);
    }

    public void testMultipleAggregations() {
        PPLResponse r = executeSql(
            "SELECT COUNT(*) AS cnt, SUM(fare_amount) AS total_fare, AVG(tip_amount) AS avg_tip FROM " + INDEX_NAME
        );
        assertNoError(r);
        assertRowCount(r, 1);
        assertEquals(3, r.getColumns().size());
    }

    public void testSelectWithAlias() {
        PPLResponse r = executeSql(
            "SELECT vendorid AS vendor, fare_amount AS fare FROM " + INDEX_NAME + " LIMIT 5"
        );
        assertNoError(r);
    }

    public void testWhereWithBetween() {
        PPLResponse r = executeSql(
            "SELECT fare_amount FROM " + INDEX_NAME + " WHERE fare_amount BETWEEN 10.0 AND 30.0 LIMIT 10"
        );
        assertNoError(r);
    }
}
