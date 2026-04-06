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
public class PplStatsIT extends AbstractIcebergQueryIT {

    public void testStatsCount() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats count()");
        assertPplNotEmpty(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testStatsCountBy() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats count() by vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testStatsSum() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats sum(total_amount)");
        assertPplNotEmpty(response);
    }

    public void testStatsSumBy() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats sum(total_amount) by payment_type");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testStatsAvg() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats avg(trip_distance)");
        assertPplNotEmpty(response);
    }

    public void testStatsAvgBy() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats avg(fare_amount) by vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testStatsMin() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats min(fare_amount)");
        assertPplNotEmpty(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testStatsMax() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats max(fare_amount)");
        assertPplNotEmpty(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testStatsMinMax() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats min(trip_distance) as min_dist, max(trip_distance) as max_dist");
        assertPplNotEmpty(response);
        assertPplColumnCount(response, 2);
    }

    public void testStatsMultipleAggs() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats count() as cnt, sum(total_amount) as total, avg(trip_distance) as avg_dist");
        assertPplNotEmpty(response);
        assertPplColumnCount(response, 3);
    }

    public void testStatsMultipleGroupBy() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats count() by vendorid, payment_type");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testStatsCountDistinct() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats dc(vendorid)");
        assertPplNotEmpty(response);
    }

    public void testStatsWithWhere() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where vendorid = 1 | stats count() as cnt, avg(fare_amount) as avg_fare");
        assertPplNotEmpty(response);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testStatsWithAlias() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats count() as total_trips by vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testStatsMultipleAggsByMultipleFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats count(), avg(fare_amount) by vendorid, payment_type");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testStatsMaxBy() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats max(total_amount) by vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testStatsMinBy() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats min(trip_distance) by payment_type");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testStatsSumMultipleFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats sum(fare_amount) as total_fare, sum(tip_amount) as total_tip by vendorid");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
