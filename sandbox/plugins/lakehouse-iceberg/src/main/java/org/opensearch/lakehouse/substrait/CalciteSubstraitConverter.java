/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.substrait;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.substrait.proto.AggregateRel;
import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import io.substrait.proto.SortField;
import io.substrait.proto.SortRel;
import io.substrait.proto.Type;

/**
 * Converts a Calcite {@link RelNode} tree to Substrait protobuf bytes.
 * DataFusion consumes these bytes via its datafusion-substrait crate.
 *
 * <p>Supported Calcite nodes:
 * <ul>
 *   <li>{@link TableScan} (including LogicalTableScan) &rarr; {@link ReadRel} (NamedTable)</li>
 *   <li>{@link LogicalFilter} &rarr; {@link FilterRel}</li>
 *   <li>{@link LogicalProject} &rarr; {@link ProjectRel}</li>
 *   <li>{@link LogicalAggregate} &rarr; {@link AggregateRel}</li>
 *   <li>{@link LogicalSort} &rarr; {@link SortRel} and/or {@link FetchRel}</li>
 * </ul>
 *
 * <p>Unsupported node types throw {@link UnsupportedOperationException}.
 */
public final class CalciteSubstraitConverter {

    /** URI anchor for the Substrait comparison functions extension. */
    private static final int FUNCTIONS_URI_ANCHOR = 1;

    /** URI for Substrait's standard comparison functions. */
    private static final String FUNCTIONS_COMPARISON_URI =
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml";

    /** URI for Substrait's standard boolean functions. */
    private static final String FUNCTIONS_BOOLEAN_URI =
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_boolean.yaml";

    private static final int BOOLEAN_URI_ANCHOR = 2;

    private CalciteSubstraitConverter() {}

    /**
     * Converts a Calcite RelNode tree to Substrait Plan bytes.
     *
     * @param relNode the Calcite relational plan
     * @return serialized Substrait Plan as byte array
     * @throws IOException if serialization fails
     */
    public static byte[] toSubstrait(RelNode relNode) throws IOException {
        ConversionContext ctx = new ConversionContext();
        Rel rel = convertRel(relNode, ctx);

        // Build output field names from the top-level row type
        RelRoot.Builder rootBuilder = RelRoot.newBuilder().setInput(rel);
        for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
            rootBuilder.addNames(field.getName());
        }

        Plan.Builder planBuilder = Plan.newBuilder()
            .addRelations(PlanRel.newBuilder().setRoot(rootBuilder.build()));

        // Add extension URIs and function declarations
        for (Map.Entry<Integer, String> entry : ctx.extensionUris.entrySet()) {
            planBuilder.addExtensionUris(
                SimpleExtensionURI.newBuilder()
                    .setExtensionUriAnchor(entry.getKey())
                    .setUri(entry.getValue())
                    .build()
            );
        }
        for (SimpleExtensionDeclaration decl : ctx.extensionDeclarations) {
            planBuilder.addExtensions(decl);
        }

        return planBuilder.build().toByteArray();
    }

    /**
     * Recursively converts a Calcite RelNode to a Substrait Rel.
     */
    static Rel convertRel(RelNode relNode, ConversionContext ctx) {
        if (relNode instanceof TableScan) {
            return convertTableScan((TableScan) relNode);
        } else if (relNode instanceof LogicalFilter) {
            return convertFilter((LogicalFilter) relNode, ctx);
        } else if (relNode instanceof LogicalProject) {
            return convertProject((LogicalProject) relNode, ctx);
        } else if (relNode instanceof LogicalAggregate) {
            return convertAggregate((LogicalAggregate) relNode, ctx);
        } else if (relNode instanceof LogicalSort) {
            return convertSort((LogicalSort) relNode, ctx);
        }
        throw new UnsupportedOperationException("Unsupported RelNode type: " + relNode.getClass().getSimpleName());
    }

    /**
     * Converts a TableScan to a Substrait ReadRel with NamedTable.
     */
    private static Rel convertTableScan(TableScan scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();

        ReadRel.Builder readBuilder = ReadRel.newBuilder()
            .setCommon(directEmit())
            .setBaseSchema(convertNamedStruct(scan.getRowType()))
            .setNamedTable(
                ReadRel.NamedTable.newBuilder()
                    .addAllNames(qualifiedName)
                    .build()
            );

        return Rel.newBuilder().setRead(readBuilder.build()).build();
    }

    /**
     * Converts a LogicalFilter to a Substrait FilterRel.
     */
    private static Rel convertFilter(LogicalFilter filter, ConversionContext ctx) {
        Rel input = convertRel(filter.getInput(), ctx);
        Expression condition = convertRexNode(filter.getCondition(), filter.getInput().getRowType(), ctx);

        FilterRel filterRel = FilterRel.newBuilder()
            .setCommon(directEmit())
            .setInput(input)
            .setCondition(condition)
            .build();

        return Rel.newBuilder().setFilter(filterRel).build();
    }

    /**
     * Converts a LogicalProject to a Substrait ProjectRel.
     */
    private static Rel convertProject(LogicalProject project, ConversionContext ctx) {
        Rel input = convertRel(project.getInput(), ctx);
        RelDataType inputRowType = project.getInput().getRowType();
        int inputFieldCount = inputRowType.getFieldCount();
        int projectCount = project.getProjects().size();

        // Substrait ProjectRel output with direct emit = [input_fields..., new_expressions...].
        // We need an emit mapping to select only the projected expressions (indices after input fields).
        RelCommon.Emit.Builder emitBuilder = RelCommon.Emit.newBuilder();
        for (int i = 0; i < projectCount; i++) {
            emitBuilder.addOutputMapping(inputFieldCount + i);
        }

        ProjectRel.Builder projectBuilder = ProjectRel.newBuilder()
            .setCommon(RelCommon.newBuilder().setEmit(emitBuilder.build()).build())
            .setInput(input);

        for (RexNode expr : project.getProjects()) {
            projectBuilder.addExpressions(convertRexNode(expr, inputRowType, ctx));
        }

        return Rel.newBuilder().setProject(projectBuilder.build()).build();
    }

    /**
     * Converts a LogicalAggregate to a Substrait AggregateRel.
     * <p>
     * Group-by keys are represented as grouping expressions (field references).
     * Aggregate functions are represented as measures with stub function references.
     */
    private static Rel convertAggregate(LogicalAggregate aggregate, ConversionContext ctx) {
        Rel input = convertRel(aggregate.getInput(), ctx);

        AggregateRel.Builder aggBuilder = AggregateRel.newBuilder()
            .setCommon(directEmit())
            .setInput(input);

        // Build grouping: each group key is a field reference expression
        if (!aggregate.getGroupSet().isEmpty()) {
            AggregateRel.Grouping.Builder grouping = AggregateRel.Grouping.newBuilder();
            for (int groupKey : aggregate.getGroupSet()) {
                grouping.addGroupingExpressions(makeFieldReference(groupKey));
            }
            aggBuilder.addGroupings(grouping.build());
        }

        // Build measures from aggregate calls
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            // Use the aggregation function's name (e.g., "COUNT", "SUM") rather than
            // the call's alias (getName()), which may be null for unnamed calls.
            String funcName = aggCall.getAggregation().getName().toLowerCase();
            int funcRef = ctx.registerFunction(FUNCTIONS_URI_ANCHOR, FUNCTIONS_COMPARISON_URI, funcName);

            io.substrait.proto.AggregateFunction.Builder aggFunc = io.substrait.proto.AggregateFunction.newBuilder()
                .setFunctionReference(funcRef)
                .setOutputType(convertType(aggCall.getType()));

            // Add argument field references
            for (int argIdx : aggCall.getArgList()) {
                aggFunc.addArguments(
                    FunctionArgument.newBuilder()
                        .setValue(makeFieldReference(argIdx))
                        .build()
                );
            }

            aggBuilder.addMeasures(
                AggregateRel.Measure.newBuilder()
                    .setMeasure(aggFunc.build())
                    .build()
            );
        }

        return Rel.newBuilder().setAggregate(aggBuilder.build()).build();
    }

    /**
     * Converts a LogicalSort to Substrait SortRel and/or FetchRel.
     * <p>
     * In Calcite, LogicalSort represents both ORDER BY and LIMIT/OFFSET.
     * <ul>
     *   <li>If collations are present, a SortRel is generated.</li>
     *   <li>If fetch (LIMIT) or offset is present, a FetchRel wraps the result.</li>
     * </ul>
     */
    private static Rel convertSort(LogicalSort sort, ConversionContext ctx) {
        Rel input = convertRel(sort.getInput(), ctx);

        // If there are sort collations, wrap in SortRel
        if (!sort.getCollation().getFieldCollations().isEmpty()) {
            SortRel.Builder sortBuilder = SortRel.newBuilder()
                .setCommon(directEmit())
                .setInput(input);

            for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
                SortField.Builder sortField = SortField.newBuilder()
                    .setExpr(makeFieldReference(fieldCollation.getFieldIndex()))
                    .setDirection(convertSortDirection(fieldCollation));

                sortBuilder.addSorts(sortField.build());
            }

            input = Rel.newBuilder().setSort(sortBuilder.build()).build();
        }

        // If LIMIT/OFFSET present, wrap in FetchRel
        if (sort.fetch != null || sort.offset != null) {
            long offset = 0;
            long count = -1; // -1 means no limit in Substrait

            if (sort.offset != null && sort.offset instanceof RexLiteral) {
                offset = ((Number) ((RexLiteral) sort.offset).getValue()).longValue();
            }
            if (sort.fetch != null && sort.fetch instanceof RexLiteral) {
                count = ((Number) ((RexLiteral) sort.fetch).getValue()).longValue();
            }

            FetchRel.Builder fetchBuilder = FetchRel.newBuilder()
                .setCommon(directEmit())
                .setInput(input)
                .setOffset(offset)
                .setCount(count);

            return Rel.newBuilder().setFetch(fetchBuilder.build()).build();
        }

        return input;
    }

    // ---- RexNode to Expression conversion ----

    /**
     * Converts a Calcite RexNode to a Substrait Expression.
     */
    static Expression convertRexNode(RexNode rexNode, RelDataType inputRowType, ConversionContext ctx) {
        if (rexNode instanceof RexInputRef) {
            return makeFieldReference(((RexInputRef) rexNode).getIndex());
        } else if (rexNode instanceof RexLiteral) {
            return convertLiteral((RexLiteral) rexNode);
        } else if (rexNode instanceof RexCall) {
            return convertRexCall((RexCall) rexNode, inputRowType, ctx);
        }
        throw new UnsupportedOperationException("Unsupported RexNode type: " + rexNode.getClass().getSimpleName());
    }

    /**
     * Converts a Calcite RexCall to a Substrait ScalarFunction expression.
     */
    private static Expression convertRexCall(RexCall call, RelDataType inputRowType, ConversionContext ctx) {
        String funcName = mapSqlKindToSubstraitFunction(call.getKind());
        int uriAnchor;
        String uri;

        if (isBooleanFunction(call.getKind())) {
            uriAnchor = BOOLEAN_URI_ANCHOR;
            uri = FUNCTIONS_BOOLEAN_URI;
        } else {
            uriAnchor = FUNCTIONS_URI_ANCHOR;
            uri = FUNCTIONS_COMPARISON_URI;
        }

        int funcRef = ctx.registerFunction(uriAnchor, uri, funcName);

        Expression.ScalarFunction.Builder scalarFunc = Expression.ScalarFunction.newBuilder()
            .setFunctionReference(funcRef)
            .setOutputType(convertType(call.getType()));

        for (RexNode operand : call.getOperands()) {
            scalarFunc.addArguments(
                FunctionArgument.newBuilder()
                    .setValue(convertRexNode(operand, inputRowType, ctx))
                    .build()
            );
        }

        return Expression.newBuilder().setScalarFunction(scalarFunc.build()).build();
    }

    /**
     * Converts a Calcite RexLiteral to a Substrait Literal expression.
     */
    private static Expression convertLiteral(RexLiteral literal) {
        Expression.Literal.Builder litBuilder = Expression.Literal.newBuilder();

        if (literal.isNull()) {
            // Null literal: use setNull with the appropriate Substrait type
            litBuilder.setNull(convertType(literal.getType()));
            return Expression.newBuilder().setLiteral(litBuilder.build()).build();
        }

        SqlTypeName typeName = literal.getTypeName();
        switch (typeName) {
            case BOOLEAN:
                litBuilder.setBoolean(literal.getValueAs(Boolean.class));
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                litBuilder.setI32(literal.getValueAs(Number.class).intValue());
                break;
            case BIGINT:
                litBuilder.setI64(literal.getValueAs(Number.class).longValue());
                break;
            case FLOAT:
            case REAL:
                litBuilder.setFp32(literal.getValueAs(Number.class).floatValue());
                break;
            case DOUBLE:
                litBuilder.setFp64(literal.getValueAs(Number.class).doubleValue());
                break;
            case CHAR:
            case VARCHAR:
                litBuilder.setString(literal.getValueAs(String.class));
                break;
            default:
                // For unsupported literal types, use string representation
                litBuilder.setString(literal.getValue().toString());
                break;
        }

        return Expression.newBuilder().setLiteral(litBuilder.build()).build();
    }

    // ---- Type conversion ----

    /**
     * Converts a Calcite RelDataType to a Substrait Type.
     */
    static Type convertType(RelDataType dataType) {
        Type.Nullability nullability = dataType.isNullable()
            ? Type.Nullability.NULLABILITY_NULLABLE
            : Type.Nullability.NULLABILITY_REQUIRED;

        switch (dataType.getSqlTypeName()) {
            case BOOLEAN:
                return Type.newBuilder()
                    .setBool(Type.Boolean.newBuilder().setNullability(nullability))
                    .build();
            case TINYINT:
                return Type.newBuilder()
                    .setI8(Type.I8.newBuilder().setNullability(nullability))
                    .build();
            case SMALLINT:
                return Type.newBuilder()
                    .setI16(Type.I16.newBuilder().setNullability(nullability))
                    .build();
            case INTEGER:
                return Type.newBuilder()
                    .setI32(Type.I32.newBuilder().setNullability(nullability))
                    .build();
            case BIGINT:
                return Type.newBuilder()
                    .setI64(Type.I64.newBuilder().setNullability(nullability))
                    .build();
            case FLOAT:
            case REAL:
                return Type.newBuilder()
                    .setFp32(Type.FP32.newBuilder().setNullability(nullability))
                    .build();
            case DOUBLE:
                return Type.newBuilder()
                    .setFp64(Type.FP64.newBuilder().setNullability(nullability))
                    .build();
            case VARCHAR:
                return Type.newBuilder()
                    .setString(Type.String.newBuilder().setNullability(nullability))
                    .build();
            case VARBINARY:
                return Type.newBuilder()
                    .setBinary(Type.Binary.newBuilder().setNullability(nullability))
                    .build();
            case DATE:
                return Type.newBuilder()
                    .setDate(Type.Date.newBuilder().setNullability(nullability))
                    .build();
            case TIME:
                return Type.newBuilder()
                    .setTime(Type.Time.newBuilder().setNullability(nullability))
                    .build();
            case TIMESTAMP:
                return Type.newBuilder()
                    .setTimestamp(Type.Timestamp.newBuilder().setNullability(nullability))
                    .build();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Type.newBuilder()
                    .setTimestampTz(Type.TimestampTZ.newBuilder().setNullability(nullability))
                    .build();
            case DECIMAL:
                return Type.newBuilder()
                    .setDecimal(Type.Decimal.newBuilder()
                        .setPrecision(dataType.getPrecision())
                        .setScale(dataType.getScale())
                        .setNullability(nullability))
                    .build();
            default:
                // Default to string for unsupported types
                return Type.newBuilder()
                    .setString(Type.String.newBuilder().setNullability(nullability))
                    .build();
        }
    }

    // ---- Helper methods ----

    /**
     * Builds a NamedStruct from a Calcite row type.
     */
    private static NamedStruct convertNamedStruct(RelDataType rowType) {
        NamedStruct.Builder builder = NamedStruct.newBuilder();
        Type.Struct.Builder structBuilder = Type.Struct.newBuilder();

        for (RelDataTypeField field : rowType.getFieldList()) {
            builder.addNames(field.getName());
            structBuilder.addTypes(convertType(field.getType()));
        }

        structBuilder.setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        builder.setStruct(structBuilder.build());
        return builder.build();
    }

    /**
     * Creates a direct-reference field reference Expression for the given field index.
     */
    private static Expression makeFieldReference(int fieldIndex) {
        return Expression.newBuilder()
            .setSelection(
                Expression.FieldReference.newBuilder()
                    .setDirectReference(
                        Expression.ReferenceSegment.newBuilder()
                            .setStructField(
                                Expression.ReferenceSegment.StructField.newBuilder()
                                    .setField(fieldIndex)
                                    .build()
                            )
                            .build()
                    )
                    .setRootReference(Expression.FieldReference.RootReference.newBuilder().build())
                    .build()
            )
            .build();
    }

    /**
     * Creates a RelCommon with direct emit (passes through all columns).
     */
    private static RelCommon directEmit() {
        return RelCommon.newBuilder()
            .setDirect(RelCommon.Direct.newBuilder().build())
            .build();
    }

    /**
     * Maps a Calcite SqlKind to the corresponding Substrait function name.
     */
    private static String mapSqlKindToSubstraitFunction(SqlKind kind) {
        switch (kind) {
            case EQUALS:
                return "equal:any_any";
            case NOT_EQUALS:
                return "not_equal:any_any";
            case GREATER_THAN:
                return "gt:any_any";
            case GREATER_THAN_OR_EQUAL:
                return "gte:any_any";
            case LESS_THAN:
                return "lt:any_any";
            case LESS_THAN_OR_EQUAL:
                return "lte:any_any";
            case AND:
                return "and:bool";
            case OR:
                return "or:bool";
            case NOT:
                return "not:bool";
            case IS_NULL:
                return "is_null:any";
            case IS_NOT_NULL:
                return "is_not_null:any";
            default:
                return kind.name().toLowerCase();
        }
    }

    /**
     * Returns true if the SqlKind represents a boolean logic function (AND/OR/NOT).
     */
    private static boolean isBooleanFunction(SqlKind kind) {
        return kind == SqlKind.AND || kind == SqlKind.OR || kind == SqlKind.NOT;
    }

    /**
     * Converts a Calcite RelFieldCollation to a Substrait SortDirection.
     */
    private static SortField.SortDirection convertSortDirection(RelFieldCollation collation) {
        switch (collation.getDirection()) {
            case ASCENDING:
                return collation.nullDirection == RelFieldCollation.NullDirection.FIRST
                    ? SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST
                    : SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST;
            case DESCENDING:
                return collation.nullDirection == RelFieldCollation.NullDirection.LAST
                    ? SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST
                    : SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST;
            default:
                return SortField.SortDirection.SORT_DIRECTION_UNSPECIFIED;
        }
    }

    /**
     * Tracks extension URIs and function declarations during conversion.
     * Functions referenced in expressions need to be declared at the Plan level.
     */
    static final class ConversionContext {
        final Map<Integer, String> extensionUris = new HashMap<>();
        final List<SimpleExtensionDeclaration> extensionDeclarations = new ArrayList<>();
        private final Map<String, Integer> functionAnchors = new HashMap<>();
        private int nextFunctionAnchor = 0;

        /**
         * Registers a function and returns its anchor reference.
         * If the function is already registered, returns the existing anchor.
         */
        int registerFunction(int uriAnchor, String uri, String functionName) {
            // Ensure the URI is registered
            extensionUris.putIfAbsent(uriAnchor, uri);

            // Check if function already registered
            String key = uriAnchor + ":" + functionName;
            if (functionAnchors.containsKey(key)) {
                return functionAnchors.get(key);
            }

            int anchor = nextFunctionAnchor++;
            functionAnchors.put(key, anchor);
            extensionDeclarations.add(
                SimpleExtensionDeclaration.newBuilder()
                    .setExtensionFunction(
                        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                            .setExtensionUriReference(uriAnchor)
                            .setFunctionAnchor(anchor)
                            .setName(functionName)
                            .build()
                    )
                    .build()
            );
            return anchor;
        }
    }
}
