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
public class PplDateTimeFunctionsIT extends AbstractIcebergQueryIT {

    public void testYear() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval yr = year(tpep_pickup_datetime) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMonth() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval mo = month(tpep_pickup_datetime) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testDay() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval dy = dayofmonth(tpep_pickup_datetime) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testHour() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval hr = hour(tpep_pickup_datetime) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testMinute() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval mi = minute(tpep_pickup_datetime) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testNow() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval ts = now() | head 1");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
        assertTrue("Expected at least 1 row", response.getRows().size() >= 1);
    }

    public void testGroupByDatePart() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval hr = hour(tpep_pickup_datetime) | stats count() by hr | sort hr");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
        assertPplColumnCount(response, 2);
    }

    public void testDayOfWeek() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval dow = dayofweek(tpep_pickup_datetime) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
