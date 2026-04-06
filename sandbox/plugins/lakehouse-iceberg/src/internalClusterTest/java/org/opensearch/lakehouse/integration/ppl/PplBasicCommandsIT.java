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

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class PplBasicCommandsIT extends AbstractIcebergQueryIT {

    public void testSourceOnly() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | head 10");
        assertPplNotEmpty(response);
        assertTrue("Expected at most 10 rows but got " + response.getRows().size(),
            response.getRows().size() <= 10);
    }

    public void testFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | fields vendorid, trip_distance, total_amount | head 10");
        assertPplNotEmpty(response);
        assertPplColumnCount(response, 3);
    }

    public void testFieldsRemove() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | fields - congestion_surcharge, airport_fee | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhere() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where vendorid = 1 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereGreaterThan() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where trip_distance > 10 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereAnd() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where vendorid = 1 and trip_distance > 5 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereOr() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where vendorid = 1 or vendorid = 2 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereNot() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where not vendorid = 1 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereIn() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where payment_type in (1, 2, 3) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereBetween() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where fare_amount between 10 and 50 | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testWhereIsNull() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where isnull(congestion_surcharge) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereIsNotNull() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where isnotnull(congestion_surcharge) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testWhereLike() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where like(store_and_fwd_flag, 'Y%') | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testHead() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | head 5");
        assertPplNotEmpty(response);
        assertTrue("Expected at most 5 rows but got " + response.getRows().size(),
            response.getRows().size() <= 5);
    }

    public void testHeadDefault() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | head");
        assertPplNotEmpty(response);
        assertTrue("Expected at most 10 rows but got " + response.getRows().size(),
            response.getRows().size() <= 10);
    }
}
