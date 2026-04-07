/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.scan;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link CalciteToIcebergPredicateConverter}.
 */
public class CalciteToIcebergPredicateConverterTests extends OpenSearchTestCase {

    private RexBuilder rexBuilder;
    private RelDataType rowType;
    private RelDataTypeFactory typeFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);

        // Build a row type: (id INTEGER, name VARCHAR, value DOUBLE)
        rowType = typeFactory.builder()
            .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("value", typeFactory.createSqlType(SqlTypeName.DOUBLE))
            .build();
    }

    public void testEqualsProducesIcebergEqual() {
        // id = 42
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal42 = rexBuilder.makeLiteral(42, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode eq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, idRef, literal42);

        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);

        assertEquals(Operation.EQ, result.op());
        assertEquals(Expressions.equal("id", 42).toString(), result.toString());
    }

    public void testNotEqualsProducesIcebergNotEqual() {
        // id != 42
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal42 = rexBuilder.makeLiteral(42, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode neq = rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, idRef, literal42);

        Expression result = CalciteToIcebergPredicateConverter.convert(neq, rowType);

        assertEquals(Operation.NOT_EQ, result.op());
        assertEquals(Expressions.notEqual("id", 42).toString(), result.toString());
    }

    public void testGreaterThanProducesIcebergGreaterThan() {
        // value > 10.0
        RexNode valueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal10 = rexBuilder.makeLiteral(10.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode gt = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, valueRef, literal10);

        Expression result = CalciteToIcebergPredicateConverter.convert(gt, rowType);

        assertEquals(Operation.GT, result.op());
    }

    public void testGreaterThanOrEqualProducesIcebergGreaterThanOrEqual() {
        // value >= 10.0
        RexNode valueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal10 = rexBuilder.makeLiteral(10.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode gte = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, valueRef, literal10);

        Expression result = CalciteToIcebergPredicateConverter.convert(gte, rowType);

        assertEquals(Operation.GT_EQ, result.op());
    }

    public void testLessThanProducesIcebergLessThan() {
        // value < 10.0
        RexNode valueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal10 = rexBuilder.makeLiteral(10.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode lt = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, valueRef, literal10);

        Expression result = CalciteToIcebergPredicateConverter.convert(lt, rowType);

        assertEquals(Operation.LT, result.op());
    }

    public void testLessThanOrEqualProducesIcebergLessThanOrEqual() {
        // value <= 10.0
        RexNode valueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal10 = rexBuilder.makeLiteral(10.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode lte = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, valueRef, literal10);

        Expression result = CalciteToIcebergPredicateConverter.convert(lte, rowType);

        assertEquals(Operation.LT_EQ, result.op());
    }

    public void testAndCombinesTwoPredicates() {
        // id = 1 AND value > 5.0
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode eq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, idRef, literal1);

        RexNode valueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal5 = rexBuilder.makeLiteral(5.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode gt = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, valueRef, literal5);

        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, eq, gt);

        Expression result = CalciteToIcebergPredicateConverter.convert(and, rowType);

        assertEquals(Operation.AND, result.op());
    }

    public void testOrCombinesTwoPredicates() {
        // id = 1 OR id = 2
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode eq1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, idRef, literal1);

        RexNode literal2 = rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode eq2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, idRef, literal2);

        RexNode or = rexBuilder.makeCall(SqlStdOperatorTable.OR, eq1, eq2);

        Expression result = CalciteToIcebergPredicateConverter.convert(or, rowType);

        assertEquals(Operation.OR, result.op());
    }

    public void testNotWrapsExpression() {
        // NOT (id = 1)
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode eq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, idRef, literal1);
        RexNode not = rexBuilder.makeCall(SqlStdOperatorTable.NOT, eq);

        Expression result = CalciteToIcebergPredicateConverter.convert(not, rowType);

        assertEquals(Operation.NOT, result.op());
    }

    public void testIsNullProducesIcebergIsNull() {
        // name IS NULL
        RexNode nameRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode isNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, nameRef);

        Expression result = CalciteToIcebergPredicateConverter.convert(isNull, rowType);

        assertEquals(Operation.IS_NULL, result.op());
    }

    public void testIsNotNullProducesIcebergIsNotNull() {
        // name IS NOT NULL
        RexNode nameRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode isNotNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, nameRef);

        Expression result = CalciteToIcebergPredicateConverter.convert(isNotNull, rowType);

        assertEquals(Operation.NOT_NULL, result.op());
    }

    public void testUnsupportedReturnsTrueFallback() {
        // LIKE is unsupported — should return alwaysTrue()
        RexNode nameRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode pattern = rexBuilder.makeLiteral("foo%");
        RexNode like = rexBuilder.makeCall(SqlStdOperatorTable.LIKE, nameRef, pattern);

        Expression result = CalciteToIcebergPredicateConverter.convert(like, rowType);

        assertEquals(Operation.TRUE, result.op());
    }

    public void testNotLikeReturnsTrueFallback() {
        // NOT LIKE is unsupported — should return alwaysTrue(), NOT alwaysFalse()
        RexNode nameRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode pattern = rexBuilder.makeLiteral("%.google.%");
        RexNode like = rexBuilder.makeCall(SqlStdOperatorTable.LIKE, nameRef, pattern);
        RexNode notLike = rexBuilder.makeCall(SqlStdOperatorTable.NOT, like);

        Expression result = CalciteToIcebergPredicateConverter.convert(notLike, rowType);

        // NOT(unsupported) must still be alwaysTrue, not alwaysFalse
        assertEquals(Operation.TRUE, result.op());
    }

    public void testNonRexCallReturnsTrueFallback() {
        // A bare RexInputRef is not a RexCall, should return alwaysTrue()
        RexNode bareRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

        Expression result = CalciteToIcebergPredicateConverter.convert(bareRef, rowType);

        assertEquals(Operation.TRUE, result.op());
    }

    public void testLiteralNodeReturnsTrueFallback() {
        // A bare RexLiteral (not wrapped in a call) should return alwaysTrue()
        RexNode literal = rexBuilder.makeLiteral(true);

        Expression result = CalciteToIcebergPredicateConverter.convert(literal, rowType);

        assertEquals(Operation.TRUE, result.op());
    }
}
