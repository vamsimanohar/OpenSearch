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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.expressions.Expression;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CalciteToIcebergPredicateConverterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelDataType rowType;
    private RexBuilder rexBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rowType = typeFactory.builder()
            .add("id", SqlTypeName.BIGINT)
            .add("name", SqlTypeName.VARCHAR)
            .add("price", SqlTypeName.DOUBLE)
            .add("qty", SqlTypeName.INTEGER)
            .add("score", SqlTypeName.FLOAT)
            .build();
        rexBuilder = new RexBuilder(typeFactory);
    }

    // -- Non-RexCall input -> alwaysTrue --

    public void testNonRexCallReturnsAlwaysTrue() {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);
        Expression result = CalciteToIcebergPredicateConverter.convert(ref, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    public void testLiteralInputReturnsAlwaysTrue() {
        RexLiteral literal = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42));
        Expression result = CalciteToIcebergPredicateConverter.convert(literal, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- Comparison operators --

    public void testEquals() {
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testNotEquals() {
        RexNode ne = rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(10))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(ne, rowType);
        assertEquals(Expression.Operation.NOT_EQ, result.op());
    }

    public void testGreaterThan() {
        RexNode gt = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(100))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(gt, rowType);
        assertEquals(Expression.Operation.GT, result.op());
    }

    public void testGreaterThanOrEqual() {
        RexNode gte = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(50))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(gte, rowType);
        assertEquals(Expression.Operation.GT_EQ, result.op());
    }

    public void testLessThan() {
        RexNode lt = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(200))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(lt, rowType);
        assertEquals(Expression.Operation.LT, result.op());
    }

    public void testLessThanOrEqual() {
        RexNode lte = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(300))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(lte, rowType);
        assertEquals(Expression.Operation.LT_EQ, result.op());
    }

    // -- Flipped operands (literal op column) --

    public void testFlippedOperandsGreaterThan() {
        // literal > column -> column < literal
        RexNode gt = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(100)),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(gt, rowType);
        assertEquals(Expression.Operation.LT, result.op());
    }

    public void testFlippedOperandsGreaterThanOrEqual() {
        // literal >= column -> column <= literal
        RexNode gte = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(50)),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(gte, rowType);
        assertEquals(Expression.Operation.LT_EQ, result.op());
    }

    public void testFlippedOperandsLessThan() {
        // literal < column -> column > literal
        RexNode lt = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(10)),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(lt, rowType);
        assertEquals(Expression.Operation.GT, result.op());
    }

    public void testFlippedOperandsLessThanOrEqual() {
        // literal <= column -> column >= literal
        RexNode lte = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(5)),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(lte, rowType);
        assertEquals(Expression.Operation.GT_EQ, result.op());
    }

    public void testFlippedOperandsEquals() {
        // literal = column -> column = literal (symmetric, flipOp returns same op)
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42)),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testFlippedOperandsNotEquals() {
        // literal != column -> column != literal (symmetric, flipOp returns same op)
        RexNode ne = rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(7)),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(ne, rowType);
        assertEquals(Expression.Operation.NOT_EQ, result.op());
    }

    // -- Both operands not column/literal -> alwaysTrue --

    public void testBothOperandsLiteralReturnsAlwaysTrue() {
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(1)),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(2))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    public void testBothOperandsColumnReturnsAlwaysTrue() {
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 3)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- AND / OR (logical operators) --

    public void testAnd() {
        RexNode left = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(10))
        );
        RexNode right = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(100))
        );
        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, left, right);

        Expression result = CalciteToIcebergPredicateConverter.convert(and, rowType);
        assertEquals(Expression.Operation.AND, result.op());
    }

    public void testOr() {
        RexNode left = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(1))
        );
        RexNode right = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(2))
        );
        RexNode or = rexBuilder.makeCall(SqlStdOperatorTable.OR, left, right);

        Expression result = CalciteToIcebergPredicateConverter.convert(or, rowType);
        assertEquals(Expression.Operation.OR, result.op());
    }

    public void testAndWithThreeOperands() {
        RexNode a = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(1))
        );
        RexNode b = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 3),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(2))
        );
        RexNode c = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2),
            rexBuilder.makeLiteral(BigDecimal.valueOf(3.0), typeFactory.createSqlType(SqlTypeName.DOUBLE), false)
        );
        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, a, b, c);

        Expression result = CalciteToIcebergPredicateConverter.convert(and, rowType);
        assertEquals(Expression.Operation.AND, result.op());
    }

    // -- NOT --

    public void testNotWithConvertibleInner() {
        RexNode inner = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42))
        );
        RexNode not = rexBuilder.makeCall(SqlStdOperatorTable.NOT, inner);

        Expression result = CalciteToIcebergPredicateConverter.convert(not, rowType);
        assertEquals(Expression.Operation.NOT, result.op());
    }

    public void testNotWithUnconvertibleInnerReturnsAlwaysTrue() {
        // An inner expression that can't be converted (two literals) -> alwaysTrue.
        // NOT(alwaysTrue) should return alwaysTrue, NOT alwaysFalse.
        RexNode inner = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(1)),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(2))
        );
        RexNode not = rexBuilder.makeCall(SqlStdOperatorTable.NOT, inner);

        Expression result = CalciteToIcebergPredicateConverter.convert(not, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- IS_NULL / IS_NOT_NULL --

    public void testIsNull() {
        RexNode isNull = rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(isNull, rowType);
        assertEquals(Expression.Operation.IS_NULL, result.op());
    }

    public void testIsNotNull() {
        RexNode isNotNull = rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NOT_NULL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(isNotNull, rowType);
        assertEquals(Expression.Operation.NOT_NULL, result.op());
    }

    public void testIsNullWithNonColumnReturnsAlwaysTrue() {
        RexNode isNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42)));
        Expression result = CalciteToIcebergPredicateConverter.convert(isNull, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    public void testIsNotNullWithNonColumnReturnsAlwaysTrue() {
        RexNode isNotNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42)));
        Expression result = CalciteToIcebergPredicateConverter.convert(isNotNull, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- IN (using mock RexCall since Calcite's IN operator has strict arity validation) --

    public void testInWithMultipleValues() {
        RexCall inCall = mockRexCall(
            SqlKind.IN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(1)),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(2)),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(3))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(inCall, rowType);
        // IN(col, 1, 2, 3) -> EQ(col,1) OR EQ(col,2) OR EQ(col,3)
        assertEquals(Expression.Operation.OR, result.op());
    }

    public void testInWithSingleValue() {
        RexCall inCall = mockRexCall(
            SqlKind.IN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(inCall, rowType);
        // IN with one value -> single EQ
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testInWithNonLiteralValueReturnsAlwaysTrue() {
        // IN(col, col2) -- second operand is not a literal
        RexCall inCall = mockRexCall(
            SqlKind.IN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 3)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(inCall, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    public void testInWithNonColumnFirstOperandReturnsAlwaysTrue() {
        RexCall inCall = mockRexCall(
            SqlKind.IN,
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(1)),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(2))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(inCall, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    public void testInWithTooFewOperandsReturnsAlwaysTrue() {
        // IN with only one operand (the column, no values) -> operands.size() < 2
        RexCall inCall = mockRexCall(SqlKind.IN, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0));
        Expression result = CalciteToIcebergPredicateConverter.convert(inCall, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- Unsupported SqlKind -> alwaysTrue --

    public void testUnsupportedSqlKindReturnsAlwaysTrue() {
        // LIKE is an unsupported SqlKind in this converter
        RexNode like = rexBuilder.makeCall(
            SqlStdOperatorTable.LIKE,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
            rexBuilder.makeLiteral("%foo%")
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(like, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- BigDecimal coercion --

    public void testCoercionToInteger() {
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 3),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testCoercionToBigint() {
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(999999999999L))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testCoercionToFloat() {
        // Use makeBigintLiteral to guarantee a BigDecimal value, then compare against FLOAT column "score"
        // This forces the coerceValue FLOAT/REAL branch to convert BigDecimal -> float
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.FLOAT), 4),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(3))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testCoercionToDouble() {
        // Use makeBigintLiteral to guarantee a BigDecimal value, then compare against DOUBLE column "price"
        // This forces the coerceValue DOUBLE branch to convert BigDecimal -> double
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(99))
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    // -- String literal extraction (CHAR/VARCHAR) --

    public void testStringLiteralChar() {
        // makeLiteral("hello") produces a CHAR literal
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
            rexBuilder.makeLiteral("hello")
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testStringLiteralVarchar() {
        // Explicitly create a VARCHAR literal to hit the VARCHAR branch in extractLiteralValue
        RexNode varcharLiteral = rexBuilder.makeLiteral("world", typeFactory.createSqlType(SqlTypeName.VARCHAR, 10), false);
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
            varcharLiteral
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    // -- Temporal literal extraction --

    public void testTimestampLiteral() {
        RelDataType tsRowType = typeFactory.builder().add("ts", SqlTypeName.TIMESTAMP).build();
        // Mock a TIMESTAMP literal that returns epoch millis via getValueAs(Long.class)
        RexLiteral tsLiteral = mock(RexLiteral.class);
        when(tsLiteral.getTypeName()).thenReturn(SqlTypeName.TIMESTAMP);
        when(tsLiteral.getValueAs(Long.class)).thenReturn(1705312200000L); // 2024-01-15 10:30:00 UTC millis

        RexCall eqCall = mockRexCall(
            SqlKind.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0),
            tsLiteral
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eqCall, tsRowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testTimestampWithLocalTimeZoneLiteral() {
        RelDataType tstzRowType = typeFactory.builder()
            .add("tstz", typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
            .build();
        RexLiteral tstzLiteral = mock(RexLiteral.class);
        when(tstzLiteral.getTypeName()).thenReturn(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        when(tstzLiteral.getValueAs(Long.class)).thenReturn(1705312200000L);

        RexCall eqCall = mockRexCall(
            SqlKind.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE), 0),
            tstzLiteral
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eqCall, tstzRowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testDateLiteral() {
        RelDataType dateRowType = typeFactory.builder().add("dt", SqlTypeName.DATE).build();
        // Mock a DATE literal that returns days since epoch via getValueAs(Integer.class)
        RexLiteral dateLiteral = mock(RexLiteral.class);
        when(dateLiteral.getTypeName()).thenReturn(SqlTypeName.DATE);
        when(dateLiteral.getValueAs(Integer.class)).thenReturn(19738); // 2024-01-15 days since epoch

        RexCall eqCall = mockRexCall(SqlKind.EQUALS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0), dateLiteral);
        Expression result = CalciteToIcebergPredicateConverter.convert(eqCall, dateRowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testTimeLiteral() {
        RelDataType timeRowType = typeFactory.builder().add("t", SqlTypeName.TIME).build();
        // Mock a TIME literal that returns millis since midnight via getValueAs(Integer.class)
        RexLiteral timeLiteral = mock(RexLiteral.class);
        when(timeLiteral.getTypeName()).thenReturn(SqlTypeName.TIME);
        when(timeLiteral.getValueAs(Integer.class)).thenReturn(37800000); // 10:30:00 millis since midnight

        RexCall eqCall = mockRexCall(SqlKind.EQUALS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIME), 0), timeLiteral);
        Expression result = CalciteToIcebergPredicateConverter.convert(eqCall, timeRowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testTimestampNullValueReturnsAlwaysTrue() {
        RelDataType tsRowType = typeFactory.builder().add("ts", SqlTypeName.TIMESTAMP).build();
        RexLiteral tsLiteral = mock(RexLiteral.class);
        when(tsLiteral.getTypeName()).thenReturn(SqlTypeName.TIMESTAMP);
        when(tsLiteral.getValueAs(Long.class)).thenReturn(null);

        RexCall eqCall = mockRexCall(
            SqlKind.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0),
            tsLiteral
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eqCall, tsRowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    public void testTimeNullValueReturnsAlwaysTrue() {
        RelDataType timeRowType = typeFactory.builder().add("t", SqlTypeName.TIME).build();
        RexLiteral timeLiteral = mock(RexLiteral.class);
        when(timeLiteral.getTypeName()).thenReturn(SqlTypeName.TIME);
        when(timeLiteral.getValueAs(Integer.class)).thenReturn(null);

        RexCall eqCall = mockRexCall(SqlKind.EQUALS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIME), 0), timeLiteral);
        Expression result = CalciteToIcebergPredicateConverter.convert(eqCall, timeRowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- DECIMAL scale coercion --

    public void testDecimalScaleCoercion() {
        // DECIMAL(10,2) column with a BigDecimal that has scale 0 -> should be coerced to scale 2
        RelDataType decimalRowType = typeFactory.builder().add("amount", SqlTypeName.DECIMAL, 10, 2).build();
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(100))  // scale=0, should become 100.00
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, decimalRowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    // -- CAST(literal) unwrapping --

    public void testCastLiteralUnwrapping() {
        // Use makeCast with different types to force an actual CAST node (not simplified away)
        RexNode innerLiteral = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42));
        RexNode cast = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.DOUBLE), innerLiteral);
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2),
            cast
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    public void testCastLiteralUnwrappingViaMock() {
        // Mock a CAST RexCall wrapping a literal to guarantee extractLiteralValue CAST branch
        RexLiteral innerLiteral = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42));
        RexCall castCall = mock(RexCall.class);
        when(castCall.getKind()).thenReturn(SqlKind.CAST);
        when(castCall.getOperands()).thenReturn(List.of(innerLiteral));

        // Build: column = CAST(42)
        RexCall eqCall = mockRexCall(SqlKind.EQUALS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0), castCall);
        Expression result = CalciteToIcebergPredicateConverter.convert(eqCall, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    // -- Empty operand list for logical operators --
    // Calcite won't allow constructing these via makeCall, so use mock RexCall.

    public void testAndWithEmptyOperandsReturnsAlwaysTrue() {
        RexCall emptyAnd = mockRexCall(SqlKind.AND);
        Expression result = CalciteToIcebergPredicateConverter.convert(emptyAnd, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    public void testOrWithEmptyOperandsReturnsAlwaysTrue() {
        RexCall emptyOr = mockRexCall(SqlKind.OR);
        Expression result = CalciteToIcebergPredicateConverter.convert(emptyOr, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- Empty operand list for IS_NULL / IS_NOT_NULL --

    public void testIsNullWithEmptyOperandsReturnsAlwaysTrue() {
        RexCall emptyIsNull = mockRexCall(SqlKind.IS_NULL);
        Expression result = CalciteToIcebergPredicateConverter.convert(emptyIsNull, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    public void testIsNotNullWithEmptyOperandsReturnsAlwaysTrue() {
        RexCall emptyIsNotNull = mockRexCall(SqlKind.IS_NOT_NULL);
        Expression result = CalciteToIcebergPredicateConverter.convert(emptyIsNotNull, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- Empty operand list for comparison --

    public void testComparisonWithEmptyOperandsReturnsAlwaysTrue() {
        RexCall emptyEq = mockRexCall(SqlKind.EQUALS);
        Expression result = CalciteToIcebergPredicateConverter.convert(emptyEq, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- Column name that doesn't match any field (coerceValue default) --

    public void testCoerceValueColumnNotFoundReturnsOriginal() {
        RelDataType smallRowType = typeFactory.builder().add("other_col", SqlTypeName.VARCHAR).build();

        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(42))
        );
        // "other_col" is VARCHAR; coerceValue hits default branch for VARCHAR type.
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, smallRowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    // -- extractLiteralValue with non-literal, non-CAST RexCall --

    public void testNonCastRexCallAsLiteralReturnsAlwaysTrue() {
        // Build col = (col + col) -- the right side is a RexCall but not CAST
        RexNode plus = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0)
        );
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
            plus
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- coerceValue with non-BigDecimal value passes through --

    public void testCoerceValueNonBigDecimalPassesThrough() {
        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
            rexBuilder.makeLiteral("test_string")
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, rowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    // -- coerceValue BigDecimal for a column whose type has no special case --

    public void testCoerceValueBigDecimalUnknownTypeFallsThrough() {
        // DECIMAL column -- not INTEGER, BIGINT, FLOAT, or DOUBLE
        RelDataType decimalRowType = typeFactory.builder().add("amount", SqlTypeName.DECIMAL, 10, 2).build();

        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2), 0),
            rexBuilder.makeLiteral(BigDecimal.valueOf(19.99), typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2), false)
        );
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, decimalRowType);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    // -- coerceValue BigDecimal for column not found in row type --

    public void testCoerceValueBigDecimalColumnNotInRowType() {
        RelDataType singleRow = typeFactory.builder().add("alpha", SqlTypeName.VARCHAR).build();

        RexNode eq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeBigintLiteral(BigDecimal.valueOf(7))
        );
        // Column name is "alpha" (VARCHAR), literal is BigDecimal.
        // coerceValue iterates: "alpha" is VARCHAR -> hits default case -> returns BigDecimal as-is.
        Expression result = CalciteToIcebergPredicateConverter.convert(eq, singleRow);
        assertEquals(Expression.Operation.EQ, result.op());
    }

    // -- CAST with wrong number of operands --

    public void testCastWithMultipleOperandsReturnsAlwaysTrue() {
        // Mock a CAST RexCall with 2 operands to hit the size != 1 branch
        RexLiteral lit1 = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(1));
        RexLiteral lit2 = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(2));
        RexCall castCall = mock(RexCall.class);
        when(castCall.getKind()).thenReturn(SqlKind.CAST);
        when(castCall.getOperands()).thenReturn(List.of(lit1, lit2));

        // column = CAST(1, 2) -- extractLiteralValue returns null since size != 1
        RexCall eqCall = mockRexCall(SqlKind.EQUALS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0), castCall);
        Expression result = CalciteToIcebergPredicateConverter.convert(eqCall, rowType);
        // Both flipped/non-flipped fail to extract literal -> alwaysTrue
        assertEquals(Expression.Operation.TRUE, result.op());
    }

    // -- Helper to build mock RexCall with arbitrary operands --

    private static RexCall mockRexCall(SqlKind kind, RexNode... operands) {
        RexCall call = mock(RexCall.class);
        when(call.getKind()).thenReturn(kind);
        when(call.getOperands()).thenReturn(List.of(operands));
        return call;
    }
}
