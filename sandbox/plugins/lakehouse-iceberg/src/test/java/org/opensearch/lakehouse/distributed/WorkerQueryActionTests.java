/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.test.OpenSearchTestCase;

public class WorkerQueryActionTests extends OpenSearchTestCase {

    public void testActionName() {
        assertEquals("cluster:internal/lakehouse/worker/query", WorkerQueryAction.NAME);
    }

    public void testSingletonInstance() {
        assertNotNull(WorkerQueryAction.INSTANCE);
        assertSame(WorkerQueryAction.INSTANCE, WorkerQueryAction.INSTANCE);
    }

    public void testActionNameMatchesInstance() {
        assertEquals(WorkerQueryAction.NAME, WorkerQueryAction.INSTANCE.name());
    }
}
