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
public class PplRenameMathIT extends AbstractIcebergQueryIT {

    public void testRename() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | rename vendorid as vendor | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testRenameMultiple() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | rename vendorid as vendor, trip_distance as distance | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMathAbs() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = abs(fare_amount) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMathCeil() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = ceil(trip_distance) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMathFloor() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = floor(trip_distance) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMathRound() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = round(fare_amount, 1) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMathSqrt() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where trip_distance > 0 | eval v = sqrt(trip_distance) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMathPow() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = pow(trip_distance, 2) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMathLog() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where trip_distance > 0 | eval v = ln(trip_distance) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMathModulo() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = vendorid % 2 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
