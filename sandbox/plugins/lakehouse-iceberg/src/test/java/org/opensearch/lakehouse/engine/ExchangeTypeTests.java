/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.engine;

import org.opensearch.test.OpenSearchTestCase;

public class ExchangeTypeTests extends OpenSearchTestCase {

    public void testAllValuesPresent() {
        ExchangeType[] values = ExchangeType.values();
        assertEquals(3, values.length);
    }

    public void testValueOf() {
        assertEquals(ExchangeType.GATHER, ExchangeType.valueOf("GATHER"));
        assertEquals(ExchangeType.HASH, ExchangeType.valueOf("HASH"));
        assertEquals(ExchangeType.NONE, ExchangeType.valueOf("NONE"));
    }

    public void testInvalidValueOfThrows() {
        expectThrows(IllegalArgumentException.class, () -> ExchangeType.valueOf("INVALID"));
    }
}
