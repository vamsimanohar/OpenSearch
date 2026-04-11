/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.action;

import org.opensearch.test.OpenSearchTestCase;

public class LakehouseQueryActionTests extends OpenSearchTestCase {

    public void testNameConstant() {
        assertEquals("cluster:internal/lakehouse/query", LakehouseQueryAction.NAME);
    }

    public void testInstanceNotNull() {
        assertNotNull(LakehouseQueryAction.INSTANCE);
    }

    public void testInstanceName() {
        assertEquals(LakehouseQueryAction.NAME, LakehouseQueryAction.INSTANCE.name());
    }

    public void testSingleton() {
        assertSame(LakehouseQueryAction.INSTANCE, LakehouseQueryAction.INSTANCE);
    }
}
