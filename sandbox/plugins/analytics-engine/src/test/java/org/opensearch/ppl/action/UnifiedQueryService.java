/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
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
 * <p>Pipeline: PPL text → RelNode → push-down optimization → compile → execute → response.
 */
public class UnifiedQueryService {

    private static final String DEFAULT_CATALOG = "opensearch";

    private final PushDownPlanner pushDownPlanner;
    private final EngineContext engineContext;

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
        SchemaPlus schemaPlus = engineContext.getSchema();

        UnifiedQueryContext context = UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog(DEFAULT_CATALOG, schemaPlus)
            .defaultNamespace(DEFAULT_CATALOG)
            .build();

        try {
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
            RelNode logicalPlan = planner.plan(pplText);
            RelNode mixedPlan = pushDownPlanner.plan(logicalPlan);

            // When the entire plan is absorbed into a boundary scan, execute directly
            // to avoid Avatica cursor format issues (Object[] vs scalar for single-column results).
            if (mixedPlan instanceof OpenSearchBoundaryTableScan) {
                return executeDirectly((OpenSearchBoundaryTableScan) mixedPlan);
            }

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
            throw new RuntimeException("Failed to execute PPL query: " + e.getMessage(), e);
        } finally {
            try {
                context.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
    }

    /**
     * Executes a fully-absorbed boundary scan directly, bypassing Avatica.
     */
    private PPLResponse executeDirectly(OpenSearchBoundaryTableScan boundary) {
        RelDataType rowType = boundary.getLogicalFragment().getRowType();
        List<String> columns = new ArrayList<>();
        for (RelDataTypeField field : rowType.getFieldList()) {
            columns.add(field.getName());
        }

        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : boundary.execute()) {
            rows.add(row);
        }
        return new PPLResponse(columns, rows);
    }

    /**
     * Compiles the mixed plan into a PreparedStatement. Protected for testability.
     */
    protected PreparedStatement compileAndPrepare(UnifiedQueryContext context, RelNode mixedPlan) throws Exception {
        OpenSearchQueryCompiler compiler = new OpenSearchQueryCompiler(context);
        return compiler.compile(mixedPlan);
    }
}
