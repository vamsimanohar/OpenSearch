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

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class PplEvalIT extends AbstractIcebergQueryIT {

    public void testEvalArithmetic() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval total_with_tip = fare_amount + tip_amount | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalSubtraction() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval net_fare = total_amount - tip_amount | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalMultiplication() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval fare_110pct = fare_amount * 1.1 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalDivision() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval half_fare = fare_amount / 2 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalMultipleFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval tip_pct = tip_amount / total_amount, fare_pct = fare_amount / total_amount | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalWithFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval tip_pct = tip_amount / total_amount | fields vendorid, tip_pct | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
        assertPplColumnCount(response, 2);
    }

    public void testEvalWithWhere() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval tip_pct = tip_amount / total_amount | where tip_pct > 0.2 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalWithStats() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval cost = fare_amount + tip_amount + tolls_amount | stats avg(cost)");
        assertPplNotEmpty(response);
        assertPplColumnCount(response, 1);
    }

    public void testEvalConcat() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval label = concat(store_and_fwd_flag, '-', cast(vendorid as varchar)) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalAbs() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval abs_fare = abs(fare_amount) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalCeil() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval ceil_dist = ceil(trip_distance) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testEvalFloor() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval floor_dist = floor(trip_distance) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
