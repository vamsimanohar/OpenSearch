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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.opensearch.test.OpenSearchTestCase;

public class DataFusionSqlDialectTests extends OpenSearchTestCase {

    private static final DataFusionSqlDialect DIALECT = DataFusionSqlDialect.DEFAULT;
    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();

    public void testSingletonInstance() {
        assertNotNull(DIALECT);
    }

    public void testSupportsWindowFunctions() {
        assertTrue(DIALECT.supportsWindowFunctions());
    }

    public void testDoesNotSupportAggregateFunctionFilter() {
        assertFalse(DIALECT.supportsAggregateFunctionFilter());
    }

    public void testDefaultContextConfiguration() {
        SqlDialect.Context ctx = DataFusionSqlDialect.DEFAULT_CONTEXT;
        assertNotNull(ctx);
        assertEquals("DataFusion", ctx.databaseProductName());
        assertEquals("\"", ctx.identifierQuoteString());
    }

    public void testUnparseOffsetFetchUsesLimit() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode fetchNode = SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO);
        SqlNode offsetNode = SqlLiteral.createExactNumeric("5", SqlParserPos.ZERO);
        DIALECT.unparseOffsetFetch(writer, offsetNode, fetchNode);
        String result = writer.toString();
        assertTrue("Should use LIMIT syntax, got: " + result, result.contains("LIMIT"));
    }

    public void testUnparseLimitOnly() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode fetchNode = SqlLiteral.createExactNumeric("20", SqlParserPos.ZERO);
        DIALECT.unparseOffsetFetch(writer, null, fetchNode);
        String result = writer.toString();
        assertTrue("Should contain LIMIT, got: " + result, result.contains("LIMIT"));
        assertTrue("Should contain 20, got: " + result, result.contains("20"));
    }

    // ── unparseCall: function renames ──

    public void testUnparseCallSignToSignum() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createExactNumeric("42", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.SIGN.createCall(SqlParserPos.ZERO, arg);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use SIGNUM, got: " + sql, sql.contains("SIGNUM"));
    }

    public void testUnparseCallTruncateToTrunc() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createExactNumeric("3.14", SqlParserPos.ZERO);
        SqlNode scale = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.TRUNCATE.createCall(SqlParserPos.ZERO, arg, scale);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use TRUNC, got: " + sql, sql.contains("TRUNC"));
    }

    // ── unparseCall: binary operator functions ──

    public void testUnparseCallModToBinaryOp() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode left = SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO);
        SqlNode right = SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.MOD.createCall(SqlParserPos.ZERO, left, right);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use % operator, got: " + sql, sql.contains("%"));
    }

    // ── unparseCall: date_part functions ──

    public void testUnparseCallYearToDatePart() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createCharString("2024-01-15", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.YEAR.createCall(SqlParserPos.ZERO, arg);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use date_part('year', ...), got: " + sql, sql.contains("date_part('year'"));
    }

    public void testUnparseCallMonthToDatePart() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createCharString("2024-01-15", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.MONTH.createCall(SqlParserPos.ZERO, arg);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use date_part('month', ...), got: " + sql, sql.contains("date_part('month'"));
    }

    public void testUnparseCallDayOfWeekToDatePart() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createCharString("2024-01-15", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.DAYOFWEEK.createCall(SqlParserPos.ZERO, arg);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use date_part('dow', ...), got: " + sql, sql.contains("date_part('dow'"));
    }

    public void testUnparseCallHourToDatePart() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createCharString("2024-01-15 10:30:00", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.HOUR.createCall(SqlParserPos.ZERO, arg);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use date_part('hour', ...), got: " + sql, sql.contains("date_part('hour'"));
    }

    public void testUnparseCallMinuteToDatePart() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createCharString("2024-01-15 10:30:00", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.MINUTE.createCall(SqlParserPos.ZERO, arg);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use date_part('minute', ...), got: " + sql, sql.contains("date_part('minute'"));
    }

    public void testUnparseCallSecondToDatePart() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createCharString("2024-01-15 10:30:45", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.SECOND.createCall(SqlParserPos.ZERO, arg);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use date_part('second', ...), got: " + sql, sql.contains("date_part('second'"));
    }

    // ── unparseCall: REINTERPRET ──

    public void testUnparseCallReinterpretWithMinus() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode left = SqlLiteral.createCharString("2024-01-15", SqlParserPos.ZERO);
        SqlNode right = SqlLiteral.createCharString("2024-01-01", SqlParserPos.ZERO);
        SqlNode minus = SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, left, right);
        SqlNode reinterpret = SqlStdOperatorTable.REINTERPRET.createCall(SqlParserPos.ZERO, minus);
        DIALECT.unparseCall(writer, (org.apache.calcite.sql.SqlCall) reinterpret, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use epoch arithmetic, got: " + sql, sql.contains("date_part('epoch'"));
    }

    public void testUnparseCallReinterpretWithoutMinus() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createCharString("2024-01-15", SqlParserPos.ZERO);
        SqlNode reinterpret = SqlStdOperatorTable.REINTERPRET.createCall(SqlParserPos.ZERO, arg);
        DIALECT.unparseCall(writer, (org.apache.calcite.sql.SqlCall) reinterpret, 0, 0);
        String sql = writer.toString();
        assertTrue("Should use epoch arithmetic, got: " + sql, sql.contains("date_part('epoch'"));
        assertTrue("Should multiply by 1000, got: " + sql, sql.contains("* 1000"));
    }

    // ── unparseCall: ANY_VALUE ──

    public void testUnparseCallAnyValueToMin() {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        SqlNode arg = SqlLiteral.createCharString("col", SqlParserPos.ZERO);
        SqlNode call = SqlStdOperatorTable.ANY_VALUE.createCall(SqlParserPos.ZERO, arg);
        call.unparse(writer, 0, 0);
        String sql = writer.toString();
        assertTrue("Should rewrite ANY_VALUE to MIN, got: " + sql, sql.contains("MIN("));
        assertFalse("Should not emit ANY_VALUE literal, got: " + sql, sql.contains("ANY_VALUE"));
    }

    // ── Constructor ──

    public void testCustomContext() {
        SqlDialect.Context ctx = DataFusionSqlDialect.DEFAULT_CONTEXT;
        DataFusionSqlDialect custom = new DataFusionSqlDialect(ctx);
        assertNotNull(custom);
        assertTrue(custom.supportsWindowFunctions());
    }
}
