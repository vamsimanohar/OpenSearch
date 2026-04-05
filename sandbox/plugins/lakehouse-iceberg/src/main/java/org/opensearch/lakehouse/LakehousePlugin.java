/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.lakehouse.scan.CalciteToIcebergPredicateConverter;
import org.opensearch.analytics.exec.ExternalTableExecutor;
import org.opensearch.analytics.schema.ExternalTable;
import org.opensearch.analytics.schema.SchemaContributor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.lakehouse.action.RegisterCatalogAction;
import org.opensearch.lakehouse.action.RegisterTableAction;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.CatalogType;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.cluster.LakehouseMetadata;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.lakehouse.schema.IcebergSchemaEnricher;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Plugin for reading external Apache Iceberg tables via the analytics engine.
 * Implements {@link ExternalTableExecutor} so that the analytics-engine hub can
 * route queries referencing Iceberg tables to this plugin.
 */
public class LakehousePlugin extends Plugin implements ActionPlugin, ExternalTableExecutor, SchemaContributor {

    private static final Logger logger = LogManager.getLogger(LakehousePlugin.class);

    private ClusterService clusterService;
    private final IcebergCatalogConnector catalogConnector = new IcebergCatalogConnector();

    /** Creates a new lakehouse plugin instance. */
    public LakehousePlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.clusterService = clusterService;
        return Collections.emptyList();
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalPlan, ExternalTable externalTable) {
        if (!(externalTable instanceof IcebergCalciteTable)) {
            throw new IllegalArgumentException("Expected IcebergCalciteTable but got: " + externalTable.getClass().getSimpleName());
        }
        IcebergCalciteTable icebergTable = (IcebergCalciteTable) externalTable;
        Table table = icebergTable.getIcebergTable();
        logger.info("[LakehousePlugin] Executing query against Iceberg table: {}", table.name());

        // Walk the RelNode tree to extract projection and filter
        List<String> projectedColumns = extractProjectedColumns(logicalPlan, table);
        Expression filterExpr = extractFilterExpression(logicalPlan);

        logger.info("[LakehousePlugin] Projected columns: {}, Filter: {}", projectedColumns, filterExpr);

        // Build Iceberg read with pushdown
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);
        if (!projectedColumns.isEmpty()) {
            scanBuilder = scanBuilder.select(projectedColumns.toArray(new String[0]));
        }
        if (filterExpr != null) {
            scanBuilder = scanBuilder.where(filterExpr);
        }

        // Determine column order for output rows
        List<String> outputColumns = projectedColumns.isEmpty()
            ? table.schema().columns().stream().map(Types.NestedField::name).toList()
            : projectedColumns;

        List<Object[]> results = new ArrayList<>();
        try (CloseableIterable<Record> records = scanBuilder.build()) {
            for (Record record : records) {
                Object[] row = new Object[outputColumns.size()];
                for (int i = 0; i < outputColumns.size(); i++) {
                    row[i] = record.getField(outputColumns.get(i));
                }
                results.add(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read Iceberg table: " + table.name(), e);
        }
        logger.info("[LakehousePlugin] Read {} rows from Iceberg table: {}", results.size(), table.name());

        // Apply in-memory aggregation if the plan has a LogicalAggregate
        Aggregate aggregate = findNode(logicalPlan, Aggregate.class);
        if (aggregate != null) {
            results = applyAggregate(aggregate, results);
            logger.info("[LakehousePlugin] Aggregated to {} rows", results.size());
        }

        return results;
    }

    /**
     * Applies in-memory aggregation on the scanned rows.
     * Supports GROUP BY with COUNT, SUM, MIN, MAX.
     */
    private List<Object[]> applyAggregate(Aggregate aggregate, List<Object[]> rows) {
        List<Integer> groupKeys = aggregate.getGroupSet().asList();
        List<AggregateCall> aggCalls = aggregate.getAggCallList();

        // Group rows by key columns
        Map<List<Object>, List<Object[]>> groups = new java.util.LinkedHashMap<>();
        for (Object[] row : rows) {
            List<Object> key = new ArrayList<>();
            for (int idx : groupKeys) {
                key.add(idx < row.length ? row[idx] : null);
            }
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        // If no group keys, treat all rows as one group
        if (groupKeys.isEmpty() && !rows.isEmpty()) {
            groups.put(Collections.emptyList(), rows);
        } else if (groupKeys.isEmpty() && rows.isEmpty()) {
            groups.put(Collections.emptyList(), Collections.emptyList());
        }

        List<Object[]> result = new ArrayList<>();
        for (Map.Entry<List<Object>, List<Object[]>> entry : groups.entrySet()) {
            List<Object> key = entry.getKey();
            List<Object[]> groupRows = entry.getValue();

            Object[] outputRow = new Object[groupKeys.size() + aggCalls.size()];

            // Copy group key columns
            for (int i = 0; i < key.size(); i++) {
                outputRow[i] = key.get(i);
            }

            // Compute aggregate functions
            for (int i = 0; i < aggCalls.size(); i++) {
                outputRow[groupKeys.size() + i] = computeAggregation(aggCalls.get(i), groupRows);
            }

            result.add(outputRow);
        }

        return result;
    }

    /**
     * Computes a single aggregate function over a group of rows.
     */
    private Object computeAggregation(AggregateCall aggCall, List<Object[]> rows) {
        SqlKind kind = aggCall.getAggregation().getKind();
        List<Integer> argList = aggCall.getArgList();

        switch (kind) {
            case COUNT:
                return (long) rows.size();
            case SUM:
            case SUM0: {
                if (argList.isEmpty()) return 0.0;
                int col = argList.get(0);
                double sum = 0;
                for (Object[] row : rows) {
                    if (col < row.length && row[col] instanceof Number) {
                        sum += ((Number) row[col]).doubleValue();
                    }
                }
                return sum;
            }
            case MIN: {
                if (argList.isEmpty()) return null;
                int col = argList.get(0);
                return minMax(rows, col, true);
            }
            case MAX: {
                if (argList.isEmpty()) return null;
                int col = argList.get(0);
                return minMax(rows, col, false);
            }
            default:
                logger.warn("[LakehousePlugin] Unsupported aggregate function: {}", kind);
                return null;
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Object minMax(List<Object[]> rows, int col, boolean isMin) {
        Comparable result = null;
        for (Object[] row : rows) {
            if (col < row.length && row[col] instanceof Comparable) {
                Comparable val = (Comparable) row[col];
                if (result == null || (isMin ? val.compareTo(result) < 0 : val.compareTo(result) > 0)) {
                    result = val;
                }
            }
        }
        return result;
    }

    /**
     * Extracts projected column names from the RelNode tree.
     * Walks through Project nodes to find column references mapped back to the table schema.
     */
    private List<String> extractProjectedColumns(RelNode node, Table table) {
        List<Types.NestedField> tableColumns = table.schema().columns();

        // Find the outermost Project node
        Project project = findNode(node, Project.class);
        if (project == null) {
            return Collections.emptyList(); // SELECT * — no projection
        }

        // Get the row type of the TableScan (the input to filter/project)
        RelDataType scanRowType = findScanRowType(node);
        if (scanRowType == null) {
            return Collections.emptyList();
        }

        List<String> columns = new ArrayList<>();
        for (RexNode expr : project.getProjects()) {
            if (expr instanceof RexInputRef) {
                int index = ((RexInputRef) expr).getIndex();
                if (index < scanRowType.getFieldCount()) {
                    columns.add(scanRowType.getFieldList().get(index).getName());
                }
            }
        }
        return columns.isEmpty() ? Collections.emptyList() : columns;
    }

    /**
     * Extracts filter expression from the RelNode tree and converts to Iceberg Expression.
     */
    private Expression extractFilterExpression(RelNode node) {
        Filter filter = findNode(node, Filter.class);
        if (filter == null) {
            return null;
        }
        RelDataType inputRowType = filter.getInput().getRowType();
        return CalciteToIcebergPredicateConverter.convert(filter.getCondition(), inputRowType);
    }

    /**
     * Finds the first node of the given type in the RelNode tree (depth-first).
     */
    @SuppressWarnings("unchecked")
    private <T> T findNode(RelNode node, Class<T> clazz) {
        if (clazz.isInstance(node)) {
            return (T) node;
        }
        for (RelNode input : node.getInputs()) {
            T found = findNode(input, clazz);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * Finds the row type of the TableScan at the bottom of the RelNode tree.
     */
    private RelDataType findScanRowType(RelNode node) {
        if (node.getInputs().isEmpty()) {
            return node.getRowType(); // TableScan
        }
        for (RelNode input : node.getInputs()) {
            RelDataType found = findScanRowType(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Override
    public void contributeSchema(SchemaPlus schema, ClusterState clusterState) {
        LakehouseMetadata metadata = clusterState.metadata().custom(LakehouseMetadata.TYPE);
        if (metadata == null) {
            return;
        }

        // Ensure catalogs are registered in the connector
        for (Map.Entry<String, Map<String, String>> entry : metadata.catalogs().entrySet()) {
            String name = entry.getKey();
            if (!catalogConnector.listCatalogs().contains(name)) {
                Map<String, String> config = entry.getValue();
                try {
                    CatalogConfig catalogConfig = new CatalogConfig(
                        name,
                        CatalogType.valueOf(config.getOrDefault("type", "GLUE").toUpperCase(java.util.Locale.ROOT)),
                        config.get("uri"),
                        config.get("warehouse"),
                        config.get("region"),
                        config.get("database"),
                        config.getOrDefault("credential_provider", "default"),
                        Duration.ofMinutes(5)
                    );
                    catalogConnector.registerCatalog(name, catalogConfig);
                } catch (Exception e) {
                    logger.warn("[LakehousePlugin] Failed to register catalog [{}]: {}", name, e.getMessage());
                    continue;
                }
            }
        }

        // Load and add each registered table
        Map<String, Table> icebergTables = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : metadata.tables().entrySet()) {
            String tableName = entry.getKey();
            Map<String, String> binding = entry.getValue();
            String catalogName = binding.get("catalog");
            String namespace = binding.getOrDefault("namespace", "default");
            String icebergTableName = binding.getOrDefault("table", tableName);

            try {
                Table table = catalogConnector.loadTable(catalogName, TableIdentifier.of(namespace, icebergTableName));
                icebergTables.put(tableName, table);
            } catch (Exception e) {
                logger.warn("[LakehousePlugin] Failed to load table [{}] from catalog [{}]: {}", tableName, catalogName, e.getMessage());
            }
        }

        if (!icebergTables.isEmpty()) {
            IcebergSchemaEnricher.enrich(schema, icebergTables);
        }
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RegisterCatalogAction(clusterService),
            new RegisterTableAction(clusterService)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, LakehouseMetadata.TYPE, LakehouseMetadata::new),
            new NamedWriteableRegistry.Entry(
                org.opensearch.cluster.NamedDiff.class,
                LakehouseMetadata.TYPE,
                LakehouseMetadata::readDiffFrom
            )
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new org.opensearch.core.ParseField(LakehouseMetadata.TYPE),
                parser -> LakehouseMetadata.fromXContent(parser)
            )
        );
    }
}
