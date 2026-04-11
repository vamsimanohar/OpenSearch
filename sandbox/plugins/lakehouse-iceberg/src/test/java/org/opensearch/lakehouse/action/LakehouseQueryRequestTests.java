/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class LakehouseQueryRequestTests extends OpenSearchTestCase {

    public void testSqlRequestGetters() {
        LakehouseQueryRequest request = new LakehouseQueryRequest("SELECT 1", true);
        assertEquals("SELECT 1", request.getQueryText());
        assertTrue(request.isSql());
    }

    public void testPplRequestGetters() {
        LakehouseQueryRequest request = new LakehouseQueryRequest("source = t", false);
        assertEquals("source = t", request.getQueryText());
        assertFalse(request.isSql());
    }

    public void testSerializationRoundtripSql() throws IOException {
        LakehouseQueryRequest original = new LakehouseQueryRequest("SELECT * FROM t", true);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseQueryRequest deserialized = new LakehouseQueryRequest(in);
        assertEquals(original.getQueryText(), deserialized.getQueryText());
        assertEquals(original.isSql(), deserialized.isSql());
    }

    public void testSerializationRoundtripPpl() throws IOException {
        LakehouseQueryRequest original = new LakehouseQueryRequest("source = idx | head 10", false);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseQueryRequest deserialized = new LakehouseQueryRequest(in);
        assertEquals(original.getQueryText(), deserialized.getQueryText());
        assertEquals(original.isSql(), deserialized.isSql());
    }

    public void testValidateSucceeds() {
        LakehouseQueryRequest request = new LakehouseQueryRequest("SELECT 1", true);
        assertNull(request.validate());
    }

    public void testValidateFailsForNullQuery() {
        LakehouseQueryRequest request = new LakehouseQueryRequest(null, true);
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("query text is missing or empty"));
    }

    public void testValidateFailsForEmptyQuery() {
        LakehouseQueryRequest request = new LakehouseQueryRequest("", false);
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("query text is missing or empty"));
    }
}
