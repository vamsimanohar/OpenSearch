/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.scan;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import java.math.BigDecimal;
import java.util.List;

/**
 * Converts Calcite {@link RexNode} filter expressions into Iceberg {@link Expression} predicates
 * for predicate pushdown into Iceberg table scans.
 *
 * <p>Unsupported or unrecognized predicates safely fall back to {@link Expressions#alwaysTrue()},
 * which ensures no data is incorrectly skipped.</p>
 */
public final class CalciteToIcebergPredicateConverter {

    private CalciteToIcebergPredicateConverter() {}

    /**
     * Converts a Calcite filter expression into an Iceberg predicate.
     *
     * @param rexNode the Calcite filter expression
     * @param rowType the row type used to resolve column names from {@link RexInputRef} indices
     * @return an Iceberg {@link Expression}, or {@link Expressions#alwaysTrue()} for unsupported predicates
     */
    public static Expression convert(RexNode rexNode, RelDataType rowType) {
        if (!(rexNode instanceof RexCall)) {
            return Expressions.alwaysTrue();
        }
        RexCall call = (RexCall) rexNode;
        switch (call.getKind()) {
            case EQUALS:
                return convertComparison(call, rowType, ComparisonOp.EQ);
            case NOT_EQUALS:
                return convertComparison(call, rowType, ComparisonOp.NOT_EQ);
            case GREATER_THAN:
                return convertComparison(call, rowType, ComparisonOp.GT);
            case GREATER_THAN_OR_EQUAL:
                return convertComparison(call, rowType, ComparisonOp.GT_EQ);
            case LESS_THAN:
                return convertComparison(call, rowType, ComparisonOp.LT);
            case LESS_THAN_OR_EQUAL:
                return convertComparison(call, rowType, ComparisonOp.LT_EQ);
            case AND:
                return convertLogical(call, rowType, true);
            case OR:
                return convertLogical(call, rowType, false);
            case NOT:
                return Expressions.not(convert(call.getOperands().get(0), rowType));
            case IS_NULL:
                return convertIsNull(call, rowType);
            case IS_NOT_NULL:
                return convertIsNotNull(call, rowType);
            case IN:
                return convertIn(call, rowType);
            default:
                return Expressions.alwaysTrue();
        }
    }

    private enum ComparisonOp {
        EQ,
        NOT_EQ,
        GT,
        GT_EQ,
        LT,
        LT_EQ
    }

    private static Expression convertComparison(RexCall call, RelDataType rowType, ComparisonOp op) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() != 2) {
            return Expressions.alwaysTrue();
        }

        String colName = extractColumnName(operands.get(0), rowType);
        Object value = extractLiteralValue(operands.get(1));

        // Try flipped operands: literal op column
        if (colName == null || value == null) {
            colName = extractColumnName(operands.get(1), rowType);
            value = extractLiteralValue(operands.get(0));
            if (colName == null || value == null) {
                return Expressions.alwaysTrue();
            }
            // Flip the comparison direction since operands are swapped
            op = flipOp(op);
        }

        // Coerce BigDecimal to the column's expected Java type
        value = coerceValue(value, colName, rowType);

        switch (op) {
            case EQ:
                return Expressions.equal(colName, value);
            case NOT_EQ:
                return Expressions.notEqual(colName, value);
            case GT:
                return Expressions.greaterThan(colName, value);
            case GT_EQ:
                return Expressions.greaterThanOrEqual(colName, value);
            case LT:
                return Expressions.lessThan(colName, value);
            case LT_EQ:
                return Expressions.lessThanOrEqual(colName, value);
            default:
                return Expressions.alwaysTrue();
        }
    }

    private static ComparisonOp flipOp(ComparisonOp op) {
        switch (op) {
            case GT:
                return ComparisonOp.LT;
            case GT_EQ:
                return ComparisonOp.LT_EQ;
            case LT:
                return ComparisonOp.GT;
            case LT_EQ:
                return ComparisonOp.GT_EQ;
            default:
                return op;
        }
    }

    private static Expression convertLogical(RexCall call, RelDataType rowType, boolean isAnd) {
        List<RexNode> operands = call.getOperands();
        if (operands.isEmpty()) {
            return Expressions.alwaysTrue();
        }
        Expression result = convert(operands.get(0), rowType);
        for (int i = 1; i < operands.size(); i++) {
            Expression next = convert(operands.get(i), rowType);
            result = isAnd ? Expressions.and(result, next) : Expressions.or(result, next);
        }
        return result;
    }

    private static Expression convertIsNull(RexCall call, RelDataType rowType) {
        if (call.getOperands().size() != 1) {
            return Expressions.alwaysTrue();
        }
        String colName = extractColumnName(call.getOperands().get(0), rowType);
        if (colName == null) {
            return Expressions.alwaysTrue();
        }
        return Expressions.isNull(colName);
    }

    private static Expression convertIsNotNull(RexCall call, RelDataType rowType) {
        if (call.getOperands().size() != 1) {
            return Expressions.alwaysTrue();
        }
        String colName = extractColumnName(call.getOperands().get(0), rowType);
        if (colName == null) {
            return Expressions.alwaysTrue();
        }
        return Expressions.notNull(colName);
    }

    private static Expression convertIn(RexCall call, RelDataType rowType) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() < 2) {
            return Expressions.alwaysTrue();
        }
        String colName = extractColumnName(operands.get(0), rowType);
        if (colName == null) {
            return Expressions.alwaysTrue();
        }
        // Build OR chain of equals for each value in the IN list
        Expression result = null;
        for (int i = 1; i < operands.size(); i++) {
            Object value = extractLiteralValue(operands.get(i));
            if (value == null) {
                return Expressions.alwaysTrue();
            }
            Expression eq = Expressions.equal(colName, value);
            result = (result == null) ? eq : Expressions.or(result, eq);
        }
        return result != null ? result : Expressions.alwaysTrue();
    }

    private static String extractColumnName(RexNode node, RelDataType rowType) {
        if (node instanceof RexInputRef) {
            int index = ((RexInputRef) node).getIndex();
            return rowType.getFieldList().get(index).getName();
        }
        return null;
    }

    /**
     * Coerces a literal value (typically BigDecimal from Calcite) to the Java type
     * expected by Iceberg for the given column.
     */
    private static Object coerceValue(Object value, String colName, RelDataType rowType) {
        if (!(value instanceof BigDecimal)) {
            return value;
        }
        BigDecimal bd = (BigDecimal) value;
        // Find the column's SQL type
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getFieldList().get(i).getName().equals(colName)) {
                SqlTypeName typeName = rowType.getFieldList().get(i).getType().getSqlTypeName();
                switch (typeName) {
                    case INTEGER:
                        return bd.intValue();
                    case BIGINT:
                        return bd.longValue();
                    case FLOAT:
                    case REAL:
                        return bd.floatValue();
                    case DOUBLE:
                        return bd.doubleValue();
                    default:
                        return value;
                }
            }
        }
        return value;
    }

    private static Object extractLiteralValue(RexNode node) {
        if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            // For string types, use getValueAs(String.class) to unwrap NlsString
            // which Iceberg's Literals.from() cannot handle directly.
            SqlTypeName typeName = literal.getTypeName();
            if (typeName == SqlTypeName.CHAR || typeName == SqlTypeName.VARCHAR) {
                return literal.getValueAs(String.class);
            }
            return literal.getValueAs(Comparable.class);
        }
        // Handle CAST(literal) — Calcite wraps literals in CAST when types differ
        // e.g., CAST(20):DOUBLE NOT NULL for integer literal compared to double column
        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            if (call.getKind() == org.apache.calcite.sql.SqlKind.CAST && call.getOperands().size() == 1) {
                return extractLiteralValue(call.getOperands().get(0));
            }
        }
        return null;
    }
}
