/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.action;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.List;

public class LakehouseRestActionsTests extends OpenSearchTestCase {

    // ── SQL handler ──

    public void testSqlHandlerName() {
        LakehouseSqlRestAction handler = new LakehouseSqlRestAction();
        assertEquals("lakehouse_sql_query", handler.getName());
    }

    public void testSqlHandlerRoutes() {
        LakehouseSqlRestAction handler = new LakehouseSqlRestAction();
        List<RestRequest.Method> methods = handler.routes().stream().map(r -> r.getMethod()).toList();
        List<String> paths = handler.routes().stream().map(r -> r.getPath()).toList();
        assertEquals(1, handler.routes().size());
        assertTrue(methods.contains(RestRequest.Method.POST));
        assertTrue(paths.contains("_lakehouse/sql"));
    }

    // ── PPL handler ──

    public void testPplHandlerName() {
        LakehousePplRestAction handler = new LakehousePplRestAction();
        assertEquals("lakehouse_ppl_query", handler.getName());
    }

    public void testPplHandlerRoutes() {
        LakehousePplRestAction handler = new LakehousePplRestAction();
        List<RestRequest.Method> methods = handler.routes().stream().map(r -> r.getMethod()).toList();
        List<String> paths = handler.routes().stream().map(r -> r.getPath()).toList();
        assertEquals(1, handler.routes().size());
        assertTrue(methods.contains(RestRequest.Method.POST));
        assertTrue(paths.contains("_lakehouse/ppl"));
    }

    // ── parseQuery ──

    public void testParseQueryValid() throws IOException {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{\"query\": \"SELECT 1\"}"), XContentType.JSON)
            .build();
        assertEquals("SELECT 1", LakehouseSqlRestAction.parseQuery(request));
    }

    public void testParseQueryWithExtraFields() throws IOException {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{\"query\": \"SELECT 1\", \"format\": \"json\"}"), XContentType.JSON)
            .build();
        assertEquals("SELECT 1", LakehouseSqlRestAction.parseQuery(request));
    }

    public void testParseQueryMissingField() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{\"other\": \"value\"}"), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> LakehouseSqlRestAction.parseQuery(request));
        assertTrue(e.getMessage().contains("Missing 'query' field"));
    }

    public void testParseQueryBlankValue() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{\"query\": \"   \"}"), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> LakehouseSqlRestAction.parseQuery(request));
        assertTrue(e.getMessage().contains("Missing 'query' field"));
    }

    public void testParseQueryEmptyObject() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> LakehouseSqlRestAction.parseQuery(request));
        assertTrue(e.getMessage().contains("Missing 'query' field"));
    }

    // ── buildResponseJson ──

    public void testBuildResponseJson() throws IOException {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{\"query\": \"SELECT 1\"}"), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        PPLResponse response = new PPLResponse(
            List.of("id", "name"),
            List.of(new Object[] { 1, "Alice" }, new Object[] { 2, "Bob" })
        );

        XContentBuilder builder = LakehouseSqlRestAction.buildResponseJson(channel, "SELECT 1", response);
        String json = builder.toString();
        assertTrue("Should contain query field, got: " + json, json.contains("\"query\":\"SELECT 1\""));
        assertTrue("Should contain schema, got: " + json, json.contains("\"schema\""));
        assertTrue("Should contain column id, got: " + json, json.contains("\"name\":\"id\""));
        assertTrue("Should contain column name, got: " + json, json.contains("\"name\":\"name\""));
        assertTrue("Should contain rows, got: " + json, json.contains("\"rows\""));
        assertTrue("Should contain total=2, got: " + json, json.contains("\"total\":2"));
    }

    public void testBuildResponseJsonEmptyResult() throws IOException {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{\"query\": \"SELECT 1\"}"), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        PPLResponse response = new PPLResponse(List.of(), List.of());

        XContentBuilder builder = LakehouseSqlRestAction.buildResponseJson(channel, "SELECT 1 WHERE FALSE", response);
        String json = builder.toString();
        assertTrue("Should contain total=0, got: " + json, json.contains("\"total\":0"));
    }
}
