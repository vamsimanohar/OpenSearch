/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
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
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for {@code POST _analytics/sql}.
 * Parses SQL, converts to Calcite RelNode, and executes via the analytics engine.
 */
public class SqlQueryAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(SqlQueryAction.class);

    private static final AtomicReference<EngineContext> ENGINE_CONTEXT = new AtomicReference<>();
    private static final AtomicReference<QueryPlanExecutor<RelNode, Iterable<Object[]>>> PLAN_EXECUTOR = new AtomicReference<>();

    /** Called by {@link TransportDslExecuteAction} after Guice injection to share components. */
    static void setEngineComponents(EngineContext ctx, QueryPlanExecutor<RelNode, Iterable<Object[]>> executor) {
        ENGINE_CONTEXT.set(ctx);
        PLAN_EXECUTOR.set(executor);
    }

    /** Creates the SQL query handler. */
    public SqlQueryAction() {}

    @Override
    public String getName() {
        return "analytics_sql_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_analytics/sql"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String sql;
        try (XContentParser parser = request.contentParser()) {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected JSON object");
            }
            String fieldName = null;
            sql = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if ("query".equals(fieldName)) {
                    sql = parser.text();
                }
            }
        }

        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("Missing 'query' field in request body");
        }

        final String query = sql;
        return channel -> {
            try {
                EngineContext engineContext = ENGINE_CONTEXT.get();
                QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor = PLAN_EXECUTOR.get();
                if (engineContext == null || planExecutor == null) {
                    throw new IllegalStateException("Analytics engine not initialized");
                }

                SchemaPlus schema = engineContext.getSchema();

                // Parse SQL
                // Parse SQL — Lex.JAVA preserves identifier casing (no uppercasing)
                SqlParser sqlParser = SqlParser.create(query, SqlParser.config()
                    .withCaseSensitive(false)
                    .withLex(org.apache.calcite.config.Lex.JAVA));
                SqlNode sqlNode = sqlParser.parseQuery();

                // Set up Calcite infrastructure
                RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
                CalciteSchema rootSchema = CalciteSchema.from(schema);
                Properties connProps = new Properties();
                connProps.setProperty("caseSensitive", "false");
                connProps.setProperty("unquotedCasing", "UNCHANGED");
                CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                    rootSchema,
                    Collections.singletonList(""),
                    typeFactory,
                    new CalciteConnectionConfigImpl(connProps)
                );

                // Validate
                SqlValidator validator = SqlValidatorUtil.newValidator(
                    SqlStdOperatorTable.instance(),
                    catalogReader,
                    typeFactory,
                    SqlValidator.Config.DEFAULT.withIdentifierExpansion(true)
                );
                SqlNode validatedSql = validator.validate(sqlNode);

                // Convert to RelNode
                HepPlanner planner = new HepPlanner(HepProgram.builder().build());
                RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
                SqlToRelConverter relConverter = new SqlToRelConverter(
                    null,
                    validator,
                    catalogReader,
                    cluster,
                    StandardConvertletTable.INSTANCE,
                    SqlToRelConverter.config()
                );
                RelRoot relRoot = relConverter.convertQuery(validatedSql, false, true);
                RelNode relNode = relRoot.rel;

                logger.info("[SqlQueryAction] Executing SQL: {}", query);
                logger.info("[SqlQueryAction] Plan:\n{}", relNode.explain());

                // Execute
                Iterable<Object[]> rows = planExecutor.execute(relNode, null);

                // Build JSON response
                RelDataType rowType = relNode.getRowType();
                List<RelDataTypeField> fields = rowType.getFieldList();

                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field("query", query);

                builder.startArray("schema");
                for (RelDataTypeField field : fields) {
                    builder.startObject();
                    builder.field("name", field.getName());
                    builder.field("type", field.getType().getSqlTypeName().getName());
                    builder.endObject();
                }
                builder.endArray();

                builder.startArray("rows");
                int rowCount = 0;
                int numFields = fields.size();
                for (Object[] row : rows) {
                    builder.startArray();
                    int colCount = Math.min(row.length, numFields);
                    for (int i = 0; i < colCount; i++) {
                        builder.value(row[i]);
                    }
                    builder.endArray();
                    rowCount++;
                }
                builder.endArray();

                builder.field("total", rowCount);
                builder.endObject();

                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                logger.error("[SqlQueryAction] SQL execution failed", e);
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            }
        };
    }
}
