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
public class PplStringFunctionsIT extends AbstractIcebergQueryIT {

    public void testUpper() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = upper(store_and_fwd_flag) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testLower() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = lower(store_and_fwd_flag) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testLength() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = length(store_and_fwd_flag) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testTrim() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = trim(store_and_fwd_flag) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testSubstring() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = substring(store_and_fwd_flag, 1, 1) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testConcat() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = concat(store_and_fwd_flag, '-test') | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testReplace() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = replace(store_and_fwd_flag, 'Y', 'YES') | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testLtrim() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = ltrim(store_and_fwd_flag) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testRtrim() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | eval v = rtrim(store_and_fwd_flag) | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }

    public void testLike() throws Exception {
        PPLResponse response = executePpl("source=" + TABLE_NAME + " | where like(store_and_fwd_flag, 'Y') | head 10");
        assertPplNotEmpty(response);
        assertPplHasRows(response);
    }
}
