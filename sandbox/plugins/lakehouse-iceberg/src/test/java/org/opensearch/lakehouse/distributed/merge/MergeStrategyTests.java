/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.opensearch.test.OpenSearchTestCase;

public class MergeStrategyTests extends OpenSearchTestCase {

    public void testAllValuesPresent() {
        MergeStrategy[] values = MergeStrategy.values();
        assertEquals(4, values.length);
    }

    public void testValueOf() {
        assertEquals(MergeStrategy.CONCAT, MergeStrategy.valueOf("CONCAT"));
        assertEquals(MergeStrategy.GLOBAL_MERGE, MergeStrategy.valueOf("GLOBAL_MERGE"));
        assertEquals(MergeStrategy.TOPK_MERGE, MergeStrategy.valueOf("TOPK_MERGE"));
        assertEquals(MergeStrategy.SINGLE_NODE, MergeStrategy.valueOf("SINGLE_NODE"));
    }

    public void testInvalidValueOfThrows() {
        expectThrows(IllegalArgumentException.class, () -> MergeStrategy.valueOf("INVALID"));
    }
}
