/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class LakehousePluginTests extends OpenSearchTestCase {

    public void testPluginCreation() throws IOException {
        try (LakehousePlugin plugin = new LakehousePlugin()) {
            assertNotNull(plugin);
        }
    }
}
