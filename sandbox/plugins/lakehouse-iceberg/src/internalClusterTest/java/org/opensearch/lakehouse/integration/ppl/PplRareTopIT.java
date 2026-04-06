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
public class PplRareTopIT extends AbstractIcebergQueryIT {

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testTop() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | top vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testTopN() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | top 3 payment_type");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testTopByField() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | top 3 payment_type by vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testRare() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | rare payment_type");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testRareByField() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | rare payment_type by vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testTopWithWhere() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where trip_distance > 5 | top 3 vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testRareWithWhere() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where fare_amount > 50 | rare vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testTopWithFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | top 3 vendorid | fields vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
