/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration.ppl;

import org.opensearch.lakehouse.integration.AbstractIcebergQueryIT;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class PplComplexPipelinesIT extends AbstractIcebergQueryIT {

    public void testWhereEvalFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where vendorid = 1 | eval tip_pct = tip_amount / total_amount | fields vendorid, tip_pct | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
        assertPplColumnCount(response, 2);
    }

    public void testStatsSort() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats count() as cnt by vendorid | sort - cnt");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereStatsSort() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where trip_distance > 5 | stats count() as cnt, avg(fare_amount) as avg_fare by vendorid | sort - avg_fare");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalStatsGroupBy() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval cost = fare_amount + tip_amount | stats avg(cost) as avg_cost by vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMultipleEvals() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval tip_pct = tip_amount / total_amount | eval is_tipper = if(tip_pct > 0.15, 'generous', 'normal') | fields vendorid, tip_pct, is_tipper | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
        assertPplColumnCount(response, 3);
    }

    public void testRenameFieldsSort() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | rename vendorid as vendor, trip_distance as dist | fields vendor, dist | sort - dist | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereStatsDedupSort() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats avg(fare_amount) as avg_fare by vendorid, payment_type | dedup vendorid | sort - avg_fare");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalWhereStats() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval total_cost = fare_amount + tip_amount + tolls_amount | where total_cost > 50 | stats count() as cnt by vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testStatsEvalSort() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats sum(fare_amount) as total_fare, sum(tip_amount) as total_tip by vendorid | eval tip_ratio = total_tip / total_fare | sort - tip_ratio");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testComplexPipeline() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where trip_distance > 1 | eval tip_pct = tip_amount / total_amount | stats avg(tip_pct) as avg_tip_pct, count() as cnt by vendorid | sort - avg_tip_pct");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
