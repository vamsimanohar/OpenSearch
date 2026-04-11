/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.EngineContext;
import org.opensearch.ppl.compiler.OpenSearchQueryCompiler;
import org.opensearch.ppl.planner.PushDownPlanner;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * Core orchestrator that ties together PushDownPlanner
 * and OpenSearchQueryCompiler into a single execution pipeline.
 *
 * <p>Pipeline: query text → RelNode → push-down optimization → compile → execute → response.
 * Supports both PPL and SQL via {@link QueryType}.
 *
 * @opensearch.internal
 */
public class UnifiedQueryService {

    private static final Logger logger = LogManager.getLogger(UnifiedQueryService.class);
    private static final String DEFAULT_CATALOG = "opensearch";

    private final PushDownPlanner pushDownPlanner;
    private final EngineContext engineContext;

    /** Creates a query service with the given planner and engine context.
     * @param pushDownPlanner the push-down planner
     * @param engineContext the engine context
     */
    public UnifiedQueryService(PushDownPlanner pushDownPlanner, EngineContext engineContext) {
        this.pushDownPlanner = pushDownPlanner;
        this.engineContext = engineContext;
    }

    /**
     * Executes a PPL query through the full pipeline.
     *
     * @param pplText the PPL query text
     * @return a PPLResponse containing column names and result rows
     */
    public PPLResponse execute(String pplText) {
        return executeInternal(pplText, QueryType.PPL);
    }

    /**
     * Executes a SQL query through the full pipeline.
     *
     * @param sqlText the SQL query text
     * @return a PPLResponse containing column names and result rows
     */
    public PPLResponse executeSql(String sqlText) {
        return executeInternal(sqlText, QueryType.SQL);
    }

    /**
     * Executes a query through the full pipeline with the specified language.
     *
     * @param queryText the query text (PPL or SQL)
     * @param queryType the query language type
     * @return a PPLResponse containing column names and result rows
     */
    private PPLResponse executeInternal(String queryText, QueryType queryType) {
        long t0 = System.currentTimeMillis();
        SchemaPlus schemaPlus = engineContext.getSchema();

        UnifiedQueryContext context = UnifiedQueryContext.builder()
            .language(queryType)
            .catalog(DEFAULT_CATALOG, schemaPlus)
            .defaultNamespace(DEFAULT_CATALOG)
            .build();

        try {
            long t1 = System.currentTimeMillis();
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
            RelNode logicalPlan = planner.plan(queryText);
            long t2 = System.currentTimeMillis();
            logger.info("[PERF] Parse+plan: {}ms", t2 - t1);

            RelNode mixedPlan = pushDownPlanner.plan(logicalPlan);
            long t3 = System.currentTimeMillis();
            logger.info("[PERF] PushDown: {}ms", t3 - t2);

            // Fast path: when all operators are absorbed into the BoundaryTableScan,
            // execute directly via the backend without Volcano planner + Janino compilation.
            if (mixedPlan instanceof OpenSearchBoundaryTableScan) {
                logger.info("[UnifiedQueryService] Fast path: all operators absorbed into boundary — skipping Janino/Volcano");
                PPLResponse response = executeDirectly((OpenSearchBoundaryTableScan) mixedPlan);
                logger.info("[PERF] Total query: {}ms", System.currentTimeMillis() - t0);
                return response;
            }

            // Full path: Volcano planner + Janino code generation for mixed plans
            // (some operators above the boundary that Calcite must execute)
            logger.info("[UnifiedQueryService] Full path: Volcano + Janino compilation");
            PreparedStatement statement = compileAndPrepare(context, mixedPlan);
            try (statement) {
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                List<String> columns = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    columns.add(metaData.getColumnName(i));
                }

                List<Object[]> rows = new ArrayList<>();
                while (rs.next()) {
                    Object[] row = new Object[columnCount];
                    for (int i = 1; i <= columnCount; i++) {
                        row[i - 1] = rs.getObject(i);
                    }
                    rows.add(row);
                }

                return new PPLResponse(columns, rows);
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Failed to execute " + queryType + " query: " + e.getMessage(), e);
        } finally {
            try {
                context.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
    }

    /**
     * Executes a fully-absorbed boundary scan directly, bypassing Volcano planner
     * and Janino code generation. Used when all operators (filter, project, aggregate,
     * sort) have been pushed into the boundary node's logical fragment.
     */
    @SuppressWarnings("unchecked")
    private PPLResponse executeDirectly(OpenSearchBoundaryTableScan boundary) {
        List<String> columns = boundary.getRowType().getFieldNames();
        Iterable<Object[]> result = (Iterable<Object[]>) boundary.getEngineExecutor()
            .execute(boundary.getLogicalFragment(), null);
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : result) {
            rows.add(row);
        }
        return new PPLResponse(columns, rows);
    }

    /**
     * Compiles the mixed plan into a PreparedStatement. Protected for testability.
     *
     * @param context the unified query context
     * @param mixedPlan the plan to compile
     * @return a compiled PreparedStatement
     */
    protected PreparedStatement compileAndPrepare(UnifiedQueryContext context, RelNode mixedPlan) throws Exception {
        OpenSearchQueryCompiler compiler = new OpenSearchQueryCompiler(context);
        return compiler.compile(mixedPlan);
    }
}
