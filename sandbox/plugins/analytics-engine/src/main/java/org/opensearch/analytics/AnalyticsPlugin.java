/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.analytics.exec.AnalyticsSearchService;
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.Scheduler;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.schema.SchemaContributor;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.ppl.action.PPLTransportAction;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Analytics engine hub. Implements {@link ExtensiblePlugin} to discover
 * and wire query back-end extensions via SPI.
 *
 * @opensearch.internal
 */
public class AnalyticsPlugin extends Plugin implements ExtensiblePlugin, ActionPlugin {

    private static final Logger logger = LogManager.getLogger(AnalyticsPlugin.class);

    /**
     * Creates a new analytics engine hub plugin.
     */
    public AnalyticsPlugin() {}

    private final List<AnalyticsSearchBackendPlugin> backEnds = new ArrayList<>();
    private final List<DataWarehouseQueryEngine> warehouseEngines = new ArrayList<>();
    private final List<SchemaContributor> schemaContributors = new ArrayList<>();
    private SqlOperatorTable operatorTable;
    private AnalyticsSearchService searchService;

    @SuppressWarnings("rawtypes")
    @Override
    public void loadExtensions(ExtensionLoader loader) {
        backEnds.addAll(loader.loadExtensions(AnalyticsSearchBackendPlugin.class));
        warehouseEngines.addAll(loader.loadExtensions(DataWarehouseQueryEngine.class));
        schemaContributors.addAll(loader.loadExtensions(SchemaContributor.class));
        operatorTable = aggregateOperatorTables();
    }

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
        DefaultEngineContext ctx = new DefaultEngineContext(clusterService, operatorTable, schemaContributors);
        CapabilityRegistry capabilityRegistry = new CapabilityRegistry(backEnds, FieldStorageResolver::new);

        Map<String, AnalyticsSearchBackendPlugin> backEndsByName = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin be : backEnds) {
            backEndsByName.put(be.name(), be);
        }
        searchService = new AnalyticsSearchService(backEndsByName);

        return List.of(searchService, ctx, capabilityRegistry);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(new TypeLiteral<QueryPlanExecutor<RelNode, Iterable<Object[]>>>() {
            }).to(DefaultPlanExecutor.class);
            b.bind(EngineContext.class).to(DefaultEngineContext.class);
            b.bind(Scheduler.class).to(QueryScheduler.class);
            if (!warehouseEngines.isEmpty()) {
                b.bind(DataWarehouseQueryEngine.class).toInstance(warehouseEngines.get(0));
            }
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(AnalyticsQueryAction.INSTANCE, DefaultPlanExecutor.class),
            new ActionHandler<>(UnifiedPPLExecuteAction.INSTANCE, PPLTransportAction.class)
        );
    }

    @Override
    public void close() {
        if (searchService != null) {
            searchService.close();
        }
    }

    private SqlOperatorTable aggregateOperatorTables() {
        return SqlStdOperatorTable.instance();
    }

    /**
     * Default implementation of {@link EngineContext}.
     */
    static record DefaultEngineContext(ClusterService clusterService, SqlOperatorTable operatorTable, List<
        SchemaContributor> schemaContributors) implements EngineContext {

        @Override
        public SchemaPlus getSchema() {
            ClusterState state = clusterService.state();
            Set<String> claimedIndices = new HashSet<>();
            for (SchemaContributor c : schemaContributors) {
                for (IndexMetadata idx : state.metadata().indices().values()) {
                    if (c.claims(idx)) {
                        claimedIndices.add(idx.getIndex().getName());
                    }
                }
            }
            SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(state, claimedIndices);
            for (SchemaContributor c : schemaContributors) {
                c.contributeSchema(schema, state);
            }
            return schema;
        }
    }
}
