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
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import io.substrait.proto.SetRel;
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
 *   <li>{@link LogicalWindow} &rarr; {@link ProjectRel} with {@code Expression.WindowFunction}</li>
 * </ul>
 *
 * <p>Unsupported node types throw {@link UnsupportedOperationException}.
 */
public final class CalciteSubstraitConverter {

    private static final Logger logger = LogManager.getLogger(CalciteSubstraitConverter.class);

    /** URI anchor for the Substrait comparison functions extension. */
    private static final int FUNCTIONS_URI_ANCHOR = 1;

    /** URI for Substrait's standard comparison functions. */
    private static final String FUNCTIONS_COMPARISON_URI =
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml";

    /** URI for Substrait's standard boolean functions. */
    private static final String FUNCTIONS_BOOLEAN_URI =
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_boolean.yaml";

    private static final int BOOLEAN_URI_ANCHOR = 2;

    /** URI for Substrait's standard arithmetic functions. */
    private static final String FUNCTIONS_ARITHMETIC_URI =
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml";

    private static final int ARITHMETIC_URI_ANCHOR = 3;

    /** URI for Substrait's standard string functions. */
    private static final String FUNCTIONS_STRING_URI =
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_string.yaml";

    private static final int STRING_URI_ANCHOR = 4;

    /** URI for Substrait's standard datetime functions. */
    private static final String FUNCTIONS_DATETIME_URI =
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_datetime.yaml";

    private static final int DATETIME_URI_ANCHOR = 5;

    private CalciteSubstraitConverter() {}

    /**
     * Converts a Calcite RelNode tree to Substrait Plan bytes.
     *
     * @param relNode the Calcite relational plan
     * @return serialized Substrait Plan as byte array
     * @throws IOException if serialization fails
     */
    public static byte[] toSubstrait(RelNode relNode) throws IOException {
        logger.debug("[SubstraitConverter] Converting Calcite plan to Substrait. Root node: {}", relNode.getClass().getSimpleName());
        ConversionContext ctx = new ConversionContext();
        ctx.rexBuilder = relNode.getCluster().getRexBuilder();
        Rel rel = convertRel(relNode, ctx);

        // Build output field names from the top-level row type
        RelRoot.Builder rootBuilder = RelRoot.newBuilder().setInput(rel);
        List<String> outputFields = new ArrayList<>();
        for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
            rootBuilder.addNames(field.getName());
            outputFields.add(field.getName());
        }
        logger.debug("[SubstraitConverter] Output fields: {}", outputFields);

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

        Plan plan = planBuilder.build();
        byte[] bytes = plan.toByteArray();
        logger.debug("[SubstraitConverter] Substrait plan: {} bytes, {} extension URIs, {} function declarations",
            bytes.length, ctx.extensionUris.size(), ctx.extensionDeclarations.size());
        if (logger.isDebugEnabled()) {
            logger.debug("[SubstraitConverter] Readable Substrait plan:\n{}", plan);
        }
        return bytes;
    }

    /**
     * Recursively converts a Calcite RelNode to a Substrait Rel.
     */
    static Rel convertRel(RelNode relNode, ConversionContext ctx) {
        logger.debug("[SubstraitConverter] Converting node: {} -> {}", relNode.getClass().getSimpleName(),
            relNode.getRowType());
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
        } else if (relNode instanceof LogicalWindow) {
            return convertWindow((LogicalWindow) relNode, ctx);
        } else if (relNode instanceof LogicalUnion) {
            return convertSetOp((LogicalUnion) relNode, ctx);
        } else if (relNode instanceof LogicalIntersect) {
            return convertSetOp((LogicalIntersect) relNode, ctx);
        } else if (relNode instanceof LogicalMinus) {
            return convertSetOp((LogicalMinus) relNode, ctx);
        }
        throw new UnsupportedOperationException("Unsupported RelNode type: " + relNode.getClass().getSimpleName());
    }

    /**
     * Converts a TableScan to a Substrait ReadRel with NamedTable.
     */
    private static Rel convertTableScan(TableScan scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        // Use only the leaf table name — DataFusion resolves tables by name, not by
        // Calcite schema path. PPL wraps the schema under "opensearch" (producing
        // ["opensearch", "nyc_taxi"]) while SQL uses root-level names (["nyc_taxi"]).
        String tableName = qualifiedName.get(qualifiedName.size() - 1);

        ReadRel.Builder readBuilder = ReadRel.newBuilder()
            .setCommon(directEmit())
            .setBaseSchema(convertNamedStruct(scan.getRowType()))
            .setNamedTable(
                ReadRel.NamedTable.newBuilder()
                    .addNames(tableName)
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

    /**
     * Converts a Calcite set operation (UNION, INTERSECT, EXCEPT) to a Substrait SetRel.
     */
    private static Rel convertSetOp(SetOp setOp, ConversionContext ctx) {
        SetRel.Builder setBuilder = SetRel.newBuilder()
            .setCommon(directEmit());

        // Convert each input
        for (org.apache.calcite.rel.RelNode input : setOp.getInputs()) {
            setBuilder.addInputs(convertRel(input, ctx));
        }

        // Map Calcite set op kind + all flag to Substrait SetOp
        if (setOp instanceof LogicalUnion) {
            setBuilder.setOp(setOp.all
                ? SetRel.SetOp.SET_OP_UNION_ALL
                : SetRel.SetOp.SET_OP_UNION_DISTINCT);
        } else if (setOp instanceof LogicalIntersect) {
            setBuilder.setOp(setOp.all
                ? SetRel.SetOp.SET_OP_INTERSECTION_MULTISET
                : SetRel.SetOp.SET_OP_INTERSECTION_PRIMARY);
        } else if (setOp instanceof LogicalMinus) {
            setBuilder.setOp(setOp.all
                ? SetRel.SetOp.SET_OP_MINUS_MULTISET
                : SetRel.SetOp.SET_OP_MINUS_PRIMARY);
        }

        return Rel.newBuilder().setSet(setBuilder.build()).build();
    }

    /**
     * Converts a LogicalWindow to a Substrait ProjectRel containing
     * Expression.WindowFunction expressions.
     * <p>
     * DataFusion's Substrait consumer expects window functions as
     * Expression.WindowFunction inside a ProjectRel (NOT ConsistentPartitionWindowRel,
     * which returns not_impl_err). The ProjectRel contains:
     * <ul>
     *   <li>Field references for all input columns (pass-through)</li>
     *   <li>Expression.WindowFunction for each window aggregate call</li>
     * </ul>
     * An emit mapping selects only the output fields (input refs + window results).
     */
    private static Rel convertWindow(LogicalWindow window, ConversionContext ctx) {
        Rel input = convertRel(window.getInput(), ctx);
        RelDataType inputRowType = window.getInput().getRowType();
        int inputFieldCount = inputRowType.getFieldCount();

        // Build expressions: first, field refs for all input columns
        List<Expression> expressions = new ArrayList<>();
        for (int i = 0; i < inputFieldCount; i++) {
            expressions.add(makeFieldReference(i));
        }

        // Then, window function expressions from each Window.Group
        // Access groups/aggCalls reflectively to avoid compile-time Guava dependency
        // (Window.groups is ImmutableList<Group>, which implements List<Group>)
        for (Window.Group group : getWindowGroups(window)) {
            for (Window.RexWinAggCall aggCall : getGroupAggCalls(group)) {
                expressions.add(convertWindowFunction(aggCall, group, inputRowType, ctx));
            }
        }

        // The output row type of LogicalWindow = input fields + window function results.
        // DataFusion's ProjectRel output = [input_fields..., expressions...].
        // With emit mapping, we select the expressions that match the output row type.
        int outputFieldCount = window.getRowType().getFieldCount();
        RelCommon.Emit.Builder emitBuilder = RelCommon.Emit.newBuilder();
        // First inputFieldCount expressions are the pass-through field refs (indices: inputFieldCount .. 2*inputFieldCount-1)
        // Window function expressions follow (indices: 2*inputFieldCount .. end)
        // We want to output: input fields, then window function results
        for (int i = 0; i < outputFieldCount; i++) {
            emitBuilder.addOutputMapping(inputFieldCount + i);
        }

        ProjectRel.Builder projectBuilder = ProjectRel.newBuilder()
            .setCommon(RelCommon.newBuilder().setEmit(emitBuilder.build()).build())
            .setInput(input);

        for (Expression expr : expressions) {
            projectBuilder.addExpressions(expr);
        }

        return Rel.newBuilder().setProject(projectBuilder.build()).build();
    }

    /**
     * Converts a single Window.RexWinAggCall to a Substrait Expression.WindowFunction.
     */
    private static Expression convertWindowFunction(
        Window.RexWinAggCall aggCall,
        Window.Group group,
        RelDataType inputRowType,
        ConversionContext ctx
    ) {
        // Register the window function
        String funcName = resolveWindowFunctionName(aggCall.getOperator().getName().toLowerCase());
        int funcRef = ctx.registerFunction(FUNCTIONS_URI_ANCHOR, FUNCTIONS_COMPARISON_URI, funcName);

        Expression.WindowFunction.Builder winFunc = Expression.WindowFunction.newBuilder()
            .setFunctionReference(funcRef)
            .setOutputType(convertType(aggCall.getType()));

        // Add function arguments (e.g., column for SUM, offset for LAG/LEAD, buckets for NTILE)
        for (RexNode operand : aggCall.getOperands()) {
            winFunc.addArguments(
                FunctionArgument.newBuilder()
                    .setValue(convertRexNode(operand, inputRowType, ctx))
                    .build()
            );
        }

        // Partition by — convert group.keys bit set to field reference expressions
        for (int key : group.keys) {
            winFunc.addPartitions(makeFieldReference(key));
        }

        // Order by — convert group.orderKeys collation to SortField
        for (RelFieldCollation fc : group.orderKeys.getFieldCollations()) {
            winFunc.addSorts(
                SortField.newBuilder()
                    .setExpr(makeFieldReference(fc.getFieldIndex()))
                    .setDirection(convertSortDirection(fc))
                    .build()
            );
        }

        // Window frame bounds — use UNSPECIFIED when no sorts to let DataFusion default
        if (group.orderKeys.getFieldCollations().isEmpty()) {
            winFunc.setBoundsType(Expression.WindowFunction.BoundsType.BOUNDS_TYPE_UNSPECIFIED);
        } else {
            winFunc.setBoundsType(group.isRows
                ? Expression.WindowFunction.BoundsType.BOUNDS_TYPE_ROWS
                : Expression.WindowFunction.BoundsType.BOUNDS_TYPE_RANGE);
        }
        winFunc.setLowerBound(convertWindowBound(group.lowerBound, true));
        winFunc.setUpperBound(convertWindowBound(group.upperBound, false));

        return Expression.newBuilder().setWindowFunction(winFunc.build()).build();
    }

    /**
     * Converts a RexOver (window function expression in a projection) to a
     * Substrait Expression.WindowFunction.
     * <p>
     * When Calcite hasn't decomposed the plan into a LogicalWindow node,
     * window functions appear as RexOver nodes within a LogicalProject.
     * RexOver extends RexCall and contains a RexWindow with partition/order/bounds.
     */
    private static Expression convertRexOver(RexOver over, RelDataType inputRowType, ConversionContext ctx) {
        String funcName = resolveWindowFunctionName(over.getAggOperator().getName().toLowerCase());
        int funcRef = ctx.registerFunction(FUNCTIONS_URI_ANCHOR, FUNCTIONS_COMPARISON_URI, funcName);

        Expression.WindowFunction.Builder winFunc = Expression.WindowFunction.newBuilder()
            .setFunctionReference(funcRef)
            .setOutputType(convertType(over.getType()));

        // Add function arguments (e.g., column for SUM, offset for LAG/LEAD, buckets for NTILE)
        for (RexNode operand : over.getOperands()) {
            winFunc.addArguments(
                FunctionArgument.newBuilder()
                    .setValue(convertRexNode(operand, inputRowType, ctx))
                    .build()
            );
        }

        RexWindow window = over.getWindow();

        // Partition by
        @SuppressWarnings("unchecked")
        List<RexNode> partitionKeys = (List<RexNode>) getFieldViaReflection(window, "partitionKeys");
        for (RexNode key : partitionKeys) {
            winFunc.addPartitions(convertRexNode(key, inputRowType, ctx));
        }

        // Order by — RexFieldCollation is Pair<RexNode, ImmutableSet<SqlKind>>
        @SuppressWarnings("unchecked")
        List<Object> orderKeys = (List<Object>) getFieldViaReflection(window, "orderKeys");
        for (Object rfc : orderKeys) {
            // RexFieldCollation extends Pair<RexNode, ImmutableSet<SqlKind>>
            org.apache.calcite.rex.RexFieldCollation fieldColl = (org.apache.calcite.rex.RexFieldCollation) rfc;
            RexNode sortExpr = fieldColl.left;
            Expression sortExprSubstrait = convertRexNode(sortExpr, inputRowType, ctx);

            SortField.SortDirection dir;
            if (fieldColl.getDirection() == RelFieldCollation.Direction.DESCENDING) {
                dir = fieldColl.getNullDirection() == RelFieldCollation.NullDirection.LAST
                    ? SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST
                    : SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST;
            } else {
                dir = fieldColl.getNullDirection() == RelFieldCollation.NullDirection.FIRST
                    ? SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST
                    : SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST;
            }

            winFunc.addSorts(
                SortField.newBuilder()
                    .setExpr(sortExprSubstrait)
                    .setDirection(dir)
                    .build()
            );
        }

        // Window frame bounds
        RexWindowBound lowerBound = window.getLowerBound();
        RexWindowBound upperBound = window.getUpperBound();
        boolean isRows = (boolean) getFieldViaReflection(window, "isRows");

        // When there are no ORDER BY sorts, use UNSPECIFIED to let DataFusion
        // default to ROWS (safe for partitioned windows without ordering).
        // Using RANGE with no sorts causes DataFusion to inject a dummy ORDER BY.
        if (orderKeys.isEmpty()) {
            winFunc.setBoundsType(Expression.WindowFunction.BoundsType.BOUNDS_TYPE_UNSPECIFIED);
        } else {
            winFunc.setBoundsType(isRows
                ? Expression.WindowFunction.BoundsType.BOUNDS_TYPE_ROWS
                : Expression.WindowFunction.BoundsType.BOUNDS_TYPE_RANGE);
        }
        winFunc.setLowerBound(convertWindowBound(lowerBound, true));
        winFunc.setUpperBound(convertWindowBound(upperBound, false));

        return Expression.newBuilder().setWindowFunction(winFunc.build()).build();
    }

    /**
     * Converts a RexSubQuery to a Substrait Expression.Subquery.
     * Handles scalar, IN, and EXISTS subqueries.
     */
    private static Expression convertRexSubQuery(RexSubQuery subQuery, RelDataType inputRowType, ConversionContext ctx) {
        SqlKind kind = subQuery.getKind();
        Rel subqueryRel = convertRel(subQuery.rel, ctx);

        if (kind == SqlKind.SCALAR_QUERY) {
            return Expression.newBuilder()
                .setSubquery(Expression.Subquery.newBuilder()
                    .setScalar(Expression.Subquery.Scalar.newBuilder()
                        .setInput(subqueryRel)
                        .build())
                    .build())
                .build();
        } else if (kind == SqlKind.IN) {
            // IN subquery: operands[0] is the needle (left-hand expression)
            Expression.Subquery.InPredicate.Builder inBuilder = Expression.Subquery.InPredicate.newBuilder()
                .setHaystack(subqueryRel);
            for (RexNode operand : subQuery.getOperands()) {
                inBuilder.addNeedles(convertRexNode(operand, inputRowType, ctx));
            }
            return Expression.newBuilder()
                .setSubquery(Expression.Subquery.newBuilder()
                    .setInPredicate(inBuilder.build())
                    .build())
                .build();
        } else if (kind == SqlKind.EXISTS) {
            return Expression.newBuilder()
                .setSubquery(Expression.Subquery.newBuilder()
                    .setSetPredicate(Expression.Subquery.SetPredicate.newBuilder()
                        .setPredicateOp(Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_EXISTS)
                        .setTuples(subqueryRel)
                        .build())
                    .build())
                .build();
        }
        throw new UnsupportedOperationException("Unsupported subquery kind: " + kind);
    }

    /**
     * Converts a RexFieldAccess to a Substrait field reference.
     * Handles correlated variable references (e.g., t.vendorid in a correlated subquery).
     */
    private static Expression convertRexFieldAccess(RexFieldAccess fieldAccess, RelDataType inputRowType, ConversionContext ctx) {
        if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
            // Correlated reference — use OuterReference
            int fieldIndex = fieldAccess.getField().getIndex();
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
                        .setOuterReference(Expression.FieldReference.OuterReference.newBuilder()
                            .setStepsOut(1)
                            .build())
                        .build()
                )
                .build();
        }
        // Non-correlated field access — treat as regular field reference
        return makeFieldReference(fieldAccess.getField().getIndex());
    }

    /**
     * Reflectively accesses a field to avoid compile-time Guava ImmutableList dependency.
     */
    private static Object getFieldViaReflection(Object obj, String fieldName) {
        try {
            java.lang.reflect.Field f = obj.getClass().getField(fieldName);
            return f.get(obj);
        } catch (ReflectiveOperationException e) {
            // Try getDeclaredField for private fields
            try {
                java.lang.reflect.Field f = obj.getClass().getDeclaredField(fieldName);
                f.setAccessible(true);
                return f.get(obj);
            } catch (ReflectiveOperationException e2) {
                throw new RuntimeException("Failed to access field: " + fieldName, e2);
            }
        }
    }

    /**
     * Maps Calcite window function names to names DataFusion recognizes.
     * DataFusion looks up window functions first as UDWF, then as UDAF.
     */
    private static String resolveWindowFunctionName(String calciteName) {
        switch (calciteName) {
            case "row_number": return "row_number";
            case "rank": return "rank";
            case "dense_rank": return "dense_rank";
            case "lag": return "lag";
            case "lead": return "lead";
            case "ntile": return "ntile";
            case "sum": case "sum0": case "$sum0": return "sum";
            case "avg": return "avg";
            case "count": return "count";
            case "min": return "min";
            case "max": return "max";
            case "first_value": return "first_value";
            case "last_value": return "last_value";
            case "cume_dist": return "cume_dist";
            case "percent_rank": return "percent_rank";
            default:
                throw new UnsupportedOperationException("Unsupported window function: " + calciteName);
        }
    }

    /**
     * Converts a Calcite RexWindowBound to a Substrait WindowFunction.Bound.
     */
    private static Expression.WindowFunction.Bound convertWindowBound(RexWindowBound bound, boolean isLower) {
        Expression.WindowFunction.Bound.Builder b = Expression.WindowFunction.Bound.newBuilder();
        if (bound.isUnbounded()) {
            b.setUnbounded(Expression.WindowFunction.Bound.Unbounded.newBuilder().build());
        } else if (bound.isCurrentRow()) {
            b.setCurrentRow(Expression.WindowFunction.Bound.CurrentRow.newBuilder().build());
        } else if (bound.isPreceding()) {
            long offset = extractBoundOffset(bound);
            b.setPreceding(Expression.WindowFunction.Bound.Preceding.newBuilder().setOffset(offset).build());
        } else if (bound.isFollowing()) {
            long offset = extractBoundOffset(bound);
            b.setFollowing(Expression.WindowFunction.Bound.Following.newBuilder().setOffset(offset).build());
        } else {
            // Default: unbounded
            b.setUnbounded(Expression.WindowFunction.Bound.Unbounded.newBuilder().build());
        }
        return b.build();
    }

    /**
     * Extracts the numeric offset from a RexWindowBound.
     */
    private static long extractBoundOffset(RexWindowBound bound) {
        RexNode offset = bound.getOffset();
        if (offset instanceof RexLiteral) {
            RexLiteral lit = (RexLiteral) offset;
            Number val = lit.getValueAs(Number.class);
            return val != null ? val.longValue() : 1;
        }
        return 1; // default offset
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
        } else if (rexNode instanceof RexSubQuery) {
            return convertRexSubQuery((RexSubQuery) rexNode, inputRowType, ctx);
        } else if (rexNode instanceof RexFieldAccess) {
            return convertRexFieldAccess((RexFieldAccess) rexNode, inputRowType, ctx);
        } else if (rexNode instanceof RexOver) {
            // Window function expression (e.g., ROW_NUMBER() OVER (...))
            // Must be checked before RexCall since RexOver extends RexCall
            return convertRexOver((RexOver) rexNode, inputRowType, ctx);
        } else if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            // CASE/WHEN → Substrait IfThen
            if (call.getKind() == SqlKind.CASE) {
                return convertCase(call, inputRowType, ctx);
            }
            // SEARCH(field, Sarg) → expand to OR/AND chain
            if (call.getKind() == SqlKind.SEARCH) {
                return convertSearch(call, inputRowType, ctx);
            }
            return convertRexCall(call, inputRowType, ctx);
        }
        throw new UnsupportedOperationException("Unsupported RexNode type: " + rexNode.getClass().getSimpleName());
    }

    /**
     * Converts a Calcite RexCall to a Substrait ScalarFunction expression.
     */
    private static Expression convertRexCall(RexCall call, RelDataType inputRowType, ConversionContext ctx) {
        SqlKind kind = call.getKind();

        // CAST is not a scalar function — use Substrait's native Cast expression
        if (kind == SqlKind.CAST || kind == SqlKind.SAFE_CAST) {
            return convertCast(call, inputRowType, ctx);
        }

        // REINTERPRET converts an INTERVAL to its numeric value (used by TIMESTAMPDIFF).
        // Treat as a cast to the output type (typically INTEGER/BIGINT).
        if (kind == SqlKind.REINTERPRET) {
            return convertCast(call, inputRowType, ctx);
        }

        // DATE(expr) is equivalent to CAST(expr AS DATE) — treat as a cast
        if (kind == SqlKind.OTHER_FUNCTION && "DATE".equalsIgnoreCase(call.getOperator().getName())) {
            return convertCast(call, inputRowType, ctx);
        }

        // TRIM(FLAG, trimChar, string) — map to trim/ltrim/rtrim based on flag
        if (kind == SqlKind.TRIM) {
            return convertTrim(call, inputRowType, ctx);
        }

        // EXTRACT(YEAR FROM x) — convert to Substrait extract function
        if (kind == SqlKind.EXTRACT) {
            return convertExtract(call, inputRowType, ctx);
        }

        String funcName;
        int uriAnchor;
        String uri;

        if (kind == SqlKind.OTHER_FUNCTION) {
            // Named functions (UPPER, ABS, etc.) — resolve by operator name
            String opName = call.getOperator().getName().toLowerCase();
            // DateTime extract functions need special handling (two args: component + datetime)
            if (isDateTimeExtract(opName)) {
                return convertDateTimeExtract(call, inputRowType, ctx, opName);
            }
            funcName = resolveNamedFunction(call);
            int[] uriInfo = resolveNamedFunctionUri(call);
            uriAnchor = uriInfo[0];
            uri = resolveNamedFunctionUriString(uriInfo[0]);
        } else if (isBooleanFunction(kind)) {
            funcName = mapSqlKindToSubstraitFunction(kind);
            uriAnchor = BOOLEAN_URI_ANCHOR;
            uri = FUNCTIONS_BOOLEAN_URI;
        } else if (isArithmeticFunction(kind)) {
            funcName = mapSqlKindToSubstraitFunction(kind);
            uriAnchor = ARITHMETIC_URI_ANCHOR;
            uri = FUNCTIONS_ARITHMETIC_URI;
        } else if (kind == SqlKind.LIKE) {
            funcName = "like:str_str";
            uriAnchor = STRING_URI_ANCHOR;
            uri = FUNCTIONS_STRING_URI;
        } else if (kind == SqlKind.OTHER) {
            // Generic "OTHER" kind — check operator name for specific handling
            String opName = call.getOperator().getName();
            if ("||".equals(opName)) {
                funcName = "str_concat:str_str";
                uriAnchor = STRING_URI_ANCHOR;
                uri = FUNCTIONS_STRING_URI;
            } else {
                throw new UnsupportedOperationException("Unsupported OTHER operator: " + opName);
            }
        } else {
            funcName = mapSqlKindToSubstraitFunction(kind);
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
            case DECIMAL:
                // Calcite represents integer literals (e.g., 5) as DECIMAL(1,0).
                // Convert to the most appropriate numeric type.
                if (literal.getType().getScale() == 0) {
                    long val = literal.getValueAs(Number.class).longValue();
                    if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                        litBuilder.setI32((int) val);
                    } else {
                        litBuilder.setI64(val);
                    }
                } else {
                    litBuilder.setFp64(literal.getValueAs(Number.class).doubleValue());
                }
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
            case PLUS:
                return "add:any_any";
            case MINUS:
                return "subtract:any_any";
            case TIMES:
                return "multiply:any_any";
            case DIVIDE:
                return "divide:any_any";
            case MOD:
                return "modulus:any_any";
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
     * Returns true if the SqlKind represents an arithmetic function.
     */
    private static boolean isArithmeticFunction(SqlKind kind) {
        return kind == SqlKind.PLUS || kind == SqlKind.MINUS
            || kind == SqlKind.TIMES || kind == SqlKind.DIVIDE || kind == SqlKind.MOD;
    }

    /**
     * Resolves a named function (SqlKind.OTHER_FUNCTION) to a Substrait function name.
     * These are functions like UPPER, LOWER, ABS, SQRT, etc.
     */
    private static String resolveNamedFunction(RexCall call) {
        String opName = call.getOperator().getName().toLowerCase();
        switch (opName) {
            // String functions
            case "upper": return "upper:str";
            case "lower": return "lower:str";
            case "substring": return "substring:str_i32_i32";
            case "concat": return "concat:str_str";
            case "replace": return "replace:str_str_str";
            case "regexp_replace": return "regexp_replace:str_str_str";
            case "trim": return "trim:str";
            case "ltrim": return "ltrim:str";
            case "rtrim": return "rtrim:str";
            case "char_length":
            case "character_length":
            case "length": return "char_length:str";
            case "overlay": return "overlay:str_str_i32_i32";
            case "initcap": return "initcap:str";
            // Math functions
            case "abs": return "abs:any";
            case "sqrt": return "sqrt:any";
            case "exp": return "exp:any";
            case "ln":
            case "log": return "ln:any";
            case "log10": return "log10:any";
            case "power":
            case "pow": return "power:any_any";
            case "round": return "round:any_i32";
            case "truncate": return "trunc:any_i32";
            case "sign": return "signum:any";
            case "mod": return "modulus:any_any";
            case "divide": return "divide:any_any";
            case "ceil":
            case "ceiling": return "ceil:any";
            case "floor": return "floor:any";
            // Conditional
            case "coalesce": return "coalesce:any";
            case "nullif": return "nullif:any_any";
            case "if": return "if:bool_any_any";
            // Date/time (YEAR, MONTH, DAY, HOUR, MINUTE, DAYOFWEEK handled by convertDateTimeExtract)
            case "now":
            case "current_timestamp": return "current_timestamp:";
            case "current_date": return "current_date:";
            default:
                throw new UnsupportedOperationException("Unsupported named function: " + opName);
        }
    }

    /**
     * Returns the URI anchor for a named function based on its operator name.
     */
    private static int[] resolveNamedFunctionUri(RexCall call) {
        String opName = call.getOperator().getName().toLowerCase();
        switch (opName) {
            case "upper": case "lower": case "substring": case "concat":
            case "replace": case "regexp_replace": case "trim": case "ltrim": case "rtrim":
            case "char_length": case "character_length": case "length":
            case "overlay": case "initcap":
                return new int[]{STRING_URI_ANCHOR};
            case "abs": case "sqrt": case "exp": case "ln": case "log":
            case "log10": case "power": case "pow": case "round":
            case "truncate": case "sign": case "mod": case "divide": case "ceil":
            case "ceiling": case "floor":
                return new int[]{ARITHMETIC_URI_ANCHOR};
            case "now": case "current_timestamp": case "current_date":
                return new int[]{DATETIME_URI_ANCHOR};
            default:
                return new int[]{FUNCTIONS_URI_ANCHOR};
        }
    }

    /**
     * Maps a URI anchor to its URI string.
     */
    private static String resolveNamedFunctionUriString(int anchor) {
        switch (anchor) {
            case STRING_URI_ANCHOR: return FUNCTIONS_STRING_URI;
            case ARITHMETIC_URI_ANCHOR: return FUNCTIONS_ARITHMETIC_URI;
            case DATETIME_URI_ANCHOR: return FUNCTIONS_DATETIME_URI;
            case BOOLEAN_URI_ANCHOR: return FUNCTIONS_BOOLEAN_URI;
            default: return FUNCTIONS_COMPARISON_URI;
        }
    }

    /**
     * Converts a CAST/SAFE_CAST RexCall to a Substrait Cast expression.
     */
    private static Expression convertCast(RexCall call, RelDataType inputRowType, ConversionContext ctx) {
        RexNode operand = call.getOperands().get(0);
        Expression input = convertRexNode(operand, inputRowType, ctx);
        Type outputType = convertType(call.getType());

        Expression.Cast.Builder castBuilder = Expression.Cast.newBuilder()
            .setInput(input)
            .setType(outputType);

        if (call.getKind() == SqlKind.SAFE_CAST) {
            castBuilder.setFailureBehavior(Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_RETURN_NULL);
        } else {
            castBuilder.setFailureBehavior(Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_THROW_EXCEPTION);
        }

        return Expression.newBuilder().setCast(castBuilder.build()).build();
    }

    /**
     * Converts a CASE/WHEN expression to Substrait IfThen.
     * Calcite represents CASE as: CASE WHEN cond1 THEN val1 WHEN cond2 THEN val2 ... ELSE default END
     * with operands: [cond1, val1, cond2, val2, ..., default]
     */
    private static Expression convertCase(RexCall call, RelDataType inputRowType, ConversionContext ctx) {
        List<RexNode> operands = call.getOperands();
        Expression.IfThen.Builder ifThenBuilder = Expression.IfThen.newBuilder();

        // Operands are pairs of (condition, value) with an optional trailing else
        int i = 0;
        while (i < operands.size() - 1) {
            Expression condition = convertRexNode(operands.get(i), inputRowType, ctx);
            Expression then = convertRexNode(operands.get(i + 1), inputRowType, ctx);
            ifThenBuilder.addIfs(
                Expression.IfThen.IfClause.newBuilder()
                    .setIf(condition)
                    .setThen(then)
                    .build()
            );
            i += 2;
        }

        // Last operand is the ELSE clause if count is odd
        if (i < operands.size()) {
            ifThenBuilder.setElse(convertRexNode(operands.get(i), inputRowType, ctx));
        }

        return Expression.newBuilder().setIfThen(ifThenBuilder.build()).build();
    }

    /**
     * Converts a SEARCH(field, Sarg) expression by expanding it back to
     * standard comparisons using Calcite's RexUtil.expandSearch, then converting
     * the expanded expression to Substrait.
     */
    private static Expression convertSearch(RexCall call, RelDataType inputRowType, ConversionContext ctx) {
        RexNode expanded = RexUtil.expandSearch(ctx.rexBuilder, null, call);
        return convertRexNode(expanded, inputRowType, ctx);
    }

    /**
     * Converts a TRIM(FLAG, trimChar, string) to the appropriate Substrait function
     * (trim/ltrim/rtrim) based on the trim flag.
     */
    private static Expression convertTrim(RexCall call, RelDataType inputRowType, ConversionContext ctx) {
        // Calcite TRIM operands: [flag, trimChar, string]
        RexLiteral flagLiteral = (RexLiteral) call.getOperands().get(0);
        String flagStr = flagLiteral.getValue().toString();
        RexNode trimChar = call.getOperands().get(1);
        RexNode stringExpr = call.getOperands().get(2);

        String funcName;
        if ("LEADING".equalsIgnoreCase(flagStr)) {
            funcName = "ltrim:str";
        } else if ("TRAILING".equalsIgnoreCase(flagStr)) {
            funcName = "rtrim:str";
        } else {
            funcName = "trim:str";
        }

        int funcRef = ctx.registerFunction(STRING_URI_ANCHOR, FUNCTIONS_STRING_URI, funcName);

        Expression.ScalarFunction.Builder scalarFunc = Expression.ScalarFunction.newBuilder()
            .setFunctionReference(funcRef)
            .setOutputType(convertType(call.getType()));

        // Add the string argument
        scalarFunc.addArguments(
            FunctionArgument.newBuilder()
                .setValue(convertRexNode(stringExpr, inputRowType, ctx))
                .build()
        );

        // If the trim character is not a space, add it as a second argument
        if (trimChar instanceof RexLiteral) {
            String trimCharStr = ((RexLiteral) trimChar).getValueAs(String.class);
            if (trimCharStr != null && !" ".equals(trimCharStr)) {
                scalarFunc.addArguments(
                    FunctionArgument.newBuilder()
                        .setValue(convertRexNode(trimChar, inputRowType, ctx))
                        .build()
                );
            }
        }

        return Expression.newBuilder().setScalarFunction(scalarFunc.build()).build();
    }

    /**
     * Converts a SQL EXTRACT(unit FROM datetime) to a Substrait extract ScalarFunction.
     * Calcite represents EXTRACT with SqlKind.EXTRACT, operand[0] is a RexLiteral
     * with a TimeUnitRange value, operand[1] is the datetime expression.
     */
    private static Expression convertExtract(RexCall call, RelDataType inputRowType, ConversionContext ctx) {
        RexLiteral unitLiteral = (RexLiteral) call.getOperands().get(0);
        String unit = unitLiteral.getValue().toString().toUpperCase();

        int funcRef = ctx.registerFunction(DATETIME_URI_ANCHOR, FUNCTIONS_DATETIME_URI, "date_part:str_ts");

        Expression componentLiteral = Expression.newBuilder()
            .setLiteral(Expression.Literal.newBuilder().setString(unit))
            .build();
        Expression datetimeArg = convertRexNode(call.getOperands().get(1), inputRowType, ctx);

        Expression.ScalarFunction.Builder scalarFunc = Expression.ScalarFunction.newBuilder()
            .setFunctionReference(funcRef)
            .setOutputType(convertType(call.getType()))
            .addArguments(FunctionArgument.newBuilder().setValue(componentLiteral))
            .addArguments(FunctionArgument.newBuilder().setValue(datetimeArg));

        return Expression.newBuilder().setScalarFunction(scalarFunc.build()).build();
    }

    /**
     * Returns true if the named function is a datetime extraction (YEAR, MONTH, etc.)
     * that needs special two-argument handling (component + datetime).
     */
    private static boolean isDateTimeExtract(String opName) {
        switch (opName) {
            case "year": case "month": case "day":
            case "hour": case "minute": case "second":
            case "dayofweek": case "day_of_week":
                return true;
            default:
                return false;
        }
    }

    /**
     * Converts a datetime extract function (YEAR(x), MONTH(x), etc.) to a Substrait
     * extract ScalarFunction with two arguments: the component name as a string literal,
     * and the datetime expression.
     */
    private static Expression convertDateTimeExtract(RexCall call, RelDataType inputRowType,
                                                     ConversionContext ctx, String opName) {
        int funcRef = ctx.registerFunction(DATETIME_URI_ANCHOR, FUNCTIONS_DATETIME_URI, "date_part:str_ts");

        // Map function name to DataFusion-compatible component string
        String component;
        switch (opName) {
            case "year": component = "YEAR"; break;
            case "month": component = "MONTH"; break;
            case "day": component = "DAY"; break;
            case "hour": component = "HOUR"; break;
            case "minute": component = "MINUTE"; break;
            case "second": component = "SECOND"; break;
            case "dayofweek":
            case "day_of_week": component = "DOW"; break;
            default: component = opName.toUpperCase(); break;
        }

        Expression componentLiteral = Expression.newBuilder()
            .setLiteral(Expression.Literal.newBuilder().setString(component))
            .build();
        Expression datetimeArg = convertRexNode(call.getOperands().get(0), inputRowType, ctx);

        Expression.ScalarFunction.Builder scalarFunc = Expression.ScalarFunction.newBuilder()
            .setFunctionReference(funcRef)
            .setOutputType(convertType(call.getType()))
            .addArguments(FunctionArgument.newBuilder().setValue(componentLiteral))
            .addArguments(FunctionArgument.newBuilder().setValue(datetimeArg));

        return Expression.newBuilder().setScalarFunction(scalarFunc.build()).build();
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
     * Reflectively accesses Window.groups to avoid compile-time Guava ImmutableList dependency.
     */
    @SuppressWarnings("unchecked")
    private static List<Window.Group> getWindowGroups(Window window) {
        try {
            java.lang.reflect.Field f = Window.class.getField("groups");
            return (List<Window.Group>) f.get(window);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to access Window.groups", e);
        }
    }

    /**
     * Reflectively accesses Window.Group.aggCalls to avoid compile-time Guava ImmutableList dependency.
     */
    @SuppressWarnings("unchecked")
    private static List<Window.RexWinAggCall> getGroupAggCalls(Window.Group group) {
        try {
            java.lang.reflect.Field f = Window.Group.class.getField("aggCalls");
            return (List<Window.RexWinAggCall>) f.get(group);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to access Window.Group.aggCalls", e);
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
        RexBuilder rexBuilder;

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
