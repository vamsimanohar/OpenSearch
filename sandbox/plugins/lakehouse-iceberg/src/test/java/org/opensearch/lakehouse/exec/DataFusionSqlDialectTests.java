/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.opensearch.test.OpenSearchTestCase;

public class DataFusionSqlDialectTests extends OpenSearchTestCase {

    public void testSingletonInstance() {
        DataFusionSqlDialect dialect = DataFusionSqlDialect.DEFAULT;
        assertNotNull(dialect);
    }

    public void testSupportsWindowFunctions() {
        assertTrue(DataFusionSqlDialect.DEFAULT.supportsWindowFunctions());
    }

    public void testDoesNotSupportAggregateFunctionFilter() {
        assertFalse(DataFusionSqlDialect.DEFAULT.supportsAggregateFunctionFilter());
    }

    public void testDefaultContextConfiguration() {
        SqlDialect.Context ctx = DataFusionSqlDialect.DEFAULT_CONTEXT;
        assertNotNull(ctx);
        assertEquals("DataFusion", ctx.databaseProductName());
        assertEquals("\"", ctx.identifierQuoteString());
    }

    public void testUnparseOffsetFetchUsesLimit() {
        SqlDialect dialect = DataFusionSqlDialect.DEFAULT;
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        // Test that offset/fetch uses LIMIT syntax
        org.apache.calcite.rex.RexBuilder rexBuilder = new org.apache.calcite.rex.RexBuilder(typeFactory);
        SqlNode fetchNode = org.apache.calcite.sql.SqlLiteral.createExactNumeric("10", org.apache.calcite.sql.parser.SqlParserPos.ZERO);
        SqlNode offsetNode = org.apache.calcite.sql.SqlLiteral.createExactNumeric("5", org.apache.calcite.sql.parser.SqlParserPos.ZERO);

        dialect.unparseOffsetFetch(writer, offsetNode, fetchNode);
        String result = writer.toString();
        assertTrue("Should use LIMIT syntax, got: " + result, result.contains("LIMIT"));
    }
}
