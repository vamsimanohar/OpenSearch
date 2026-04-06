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
public class PplConditionalIT extends AbstractIcebergQueryIT {

    public void testIf() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval label = if(vendorid = 1, 'CMT', 'VTS') | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testIfNull() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = ifnull(congestion_surcharge, 0) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testNullIf() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = nullif(vendorid, 1) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testIsNull() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where isnull(congestion_surcharge) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testIsNotNull() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where isnotnull(congestion_surcharge) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testCase() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval label = case(vendorid = 1, 'CMT', vendorid = 2, 'VTS') | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testCoalesce() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = coalesce(congestion_surcharge, airport_fee, 0) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testNestedIf() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval label = if(trip_distance < 1, 'short', if(trip_distance < 5, 'medium', 'long')) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
