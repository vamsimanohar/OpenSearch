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
public class PplSortIT extends AbstractIcebergQueryIT {

    public void testSortAsc() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | sort trip_distance | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSortDesc() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | sort - trip_distance | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSortMultipleFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | sort vendorid, - trip_distance | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSortWithFields() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | sort - total_amount | fields vendorid, total_amount | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSortWithWhere() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where vendorid = 1 | sort - trip_distance | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSortNullsOrder() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | sort congestion_surcharge | head 20");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSortByExpression() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | sort - fare_amount + tip_amount | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSortWithAggregation() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats count() as cnt by vendorid | sort - cnt");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSortAfterStats() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | stats avg(fare_amount) as avg_fare by payment_type | sort avg_fare");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSortMultipleDesc() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | sort - vendorid, - trip_distance | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
