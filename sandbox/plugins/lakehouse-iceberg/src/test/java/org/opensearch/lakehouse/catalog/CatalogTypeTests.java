/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import org.opensearch.test.OpenSearchTestCase;

public class CatalogTypeTests extends OpenSearchTestCase {

    public void testFromStringGlue() {
        assertEquals(CatalogType.GLUE, CatalogType.fromString("glue"));
        assertEquals(CatalogType.GLUE, CatalogType.fromString("GLUE"));
        assertEquals(CatalogType.GLUE, CatalogType.fromString("Glue"));
    }

    public void testFromStringHadoop() {
        assertEquals(CatalogType.HADOOP, CatalogType.fromString("hadoop"));
        assertEquals(CatalogType.HADOOP, CatalogType.fromString("HADOOP"));
    }

    public void testFromStringRest() {
        assertEquals(CatalogType.REST, CatalogType.fromString("rest"));
        assertEquals(CatalogType.REST, CatalogType.fromString("REST"));
    }

    public void testFromStringInvalid() {
        expectThrows(IllegalArgumentException.class, () -> CatalogType.fromString("invalid"));
    }
}
