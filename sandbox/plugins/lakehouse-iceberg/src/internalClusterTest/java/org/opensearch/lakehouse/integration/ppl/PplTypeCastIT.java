/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration.ppl;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.lakehouse.integration.AbstractIcebergQueryIT;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class PplTypeCastIT extends AbstractIcebergQueryIT {

    public void testCastToInt() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = cast(trip_distance as integer) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testCastToDouble() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = cast(vendorid as double) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testCastToString() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = cast(vendorid as string) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testCastInWhere() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where cast(vendorid as double) > 1.5 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testCastInStats() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats avg(cast(passenger_count as double))");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
        assertPplColumnCount(response, 1);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testCastToDate() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = cast(tpep_pickup_datetime as date) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
