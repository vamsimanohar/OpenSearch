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
public class PplDedupIT extends AbstractIcebergQueryIT {

    public void testDedupSingleField() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | dedup vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testDedupMultipleFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | dedup vendorid, payment_type");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testDedupWithFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | dedup vendorid | fields vendorid, trip_distance");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
        assertPplColumnCount(response, 2);
    }

    public void testDedupWithSort() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | sort - trip_distance | dedup vendorid | head 5");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testDedupKeepEmpty() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | dedup vendorid keepempty=true | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

}
