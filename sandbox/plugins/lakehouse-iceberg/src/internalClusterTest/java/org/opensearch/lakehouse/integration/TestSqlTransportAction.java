/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Transport action that executes SQL queries via Calcite + analytics engine.
 * Mirrors the logic of SqlQueryAction (REST handler) as a transport action for integration tests.
 */
public class TestSqlTransportAction extends HandledTransportAction<SqlRequest, SqlResponse> {

    private static final Logger logger = LogManager.getLogger(TestSqlTransportAction.class);

    private final EngineContext engineContext;
    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;

    @Inject
    public TestSqlTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContext engineContext,
        QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor
    ) {
        super(TestSqlAction.NAME, transportService, actionFilters, SqlRequest::new);
        this.engineContext = engineContext;
        this.planExecutor = planExecutor;
    }

    @Override
    protected void doExecute(Task task, SqlRequest request, ActionListener<SqlResponse> listener) {
        try {
            String sql = request.getSql();
            SchemaPlus schema = engineContext.getSchema();

            logger.info("[TestSqlAction] Schema tables: {}", schema.getTableNames());
            logger.info("[TestSqlAction] Schema sub-schemas: {}", schema.getSubSchemaNames());

            // Parse SQL with Lex.JAVA (preserves identifier casing)
            SqlParser parser = SqlParser.create(sql, SqlParser.config()
                .withCaseSensitive(false)
                .withLex(Lex.JAVA));
            SqlNode sqlNode = parser.parseQuery();

            // Set up Calcite infrastructure
            RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            CalciteSchema rootSchema = CalciteSchema.from(schema);
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            props.setProperty("unquotedCasing", "UNCHANGED");
            CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema, Collections.singletonList(""), typeFactory,
                new CalciteConnectionConfigImpl(props)
            );

            // Validate
            SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(), catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT.withIdentifierExpansion(true)
            );
            SqlNode validated = validator.validate(sqlNode);

            // Convert to RelNode
            HepPlanner planner = new HepPlanner(HepProgram.builder().build());
            RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
            SqlToRelConverter converter = new SqlToRelConverter(
                null, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE, SqlToRelConverter.config()
            );
            RelRoot relRoot = converter.convertQuery(validated, false, true);
            RelNode relNode = relRoot.rel;

            logger.info("[TestSqlAction] Executing SQL: {}", sql);

            // Execute
            Iterable<Object[]> resultRows = planExecutor.execute(relNode, null);

            // Build response
            RelDataType rowType = relNode.getRowType();
            List<RelDataTypeField> fields = rowType.getFieldList();
            List<String> columns = new ArrayList<>();
            List<String> columnTypes = new ArrayList<>();
            for (RelDataTypeField field : fields) {
                columns.add(field.getName());
                columnTypes.add(field.getType().getSqlTypeName().getName());
            }

            List<Object[]> rows = new ArrayList<>();
            for (Object[] row : resultRows) {
                rows.add(row);
            }

            listener.onResponse(new SqlResponse(columns, columnTypes, rows));
        } catch (Exception e) {
            logger.error("[TestSqlAction] SQL execution failed", e);
            listener.onFailure(e);
        }
    }
}
