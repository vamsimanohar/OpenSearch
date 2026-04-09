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
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.ExternalTableExecutor;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.schema.SchemaContributor;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Analytics engine hub. Implements {@link ExtensiblePlugin} to discover
 * and wire query back-end extensions via SPI.
 *
 * @opensearch.internal
 */
public class AnalyticsPlugin extends Plugin implements ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(AnalyticsPlugin.class);

    /**
     * Creates a new analytics engine hub plugin.
     */
    public AnalyticsPlugin() {}

    private final List<AnalyticsSearchBackendPlugin> backEnds = new ArrayList<>();
    private final List<ExternalTableExecutor> externalTableExecutors = new ArrayList<>();
    private final List<SchemaContributor> schemaContributors = new ArrayList<>();
    private SqlOperatorTable operatorTable;

    @SuppressWarnings("rawtypes")
    @Override
    public void loadExtensions(ExtensionLoader loader) {
        backEnds.addAll(loader.loadExtensions(AnalyticsSearchBackendPlugin.class));
        externalTableExecutors.addAll(loader.loadExtensions(ExternalTableExecutor.class));
        schemaContributors.addAll(loader.loadExtensions(SchemaContributor.class));
        logger.info(
            "[AnalyticsPlugin] loadExtensions: backends={}, externalExecutors={}, schemaContributors={}",
            backEnds.size(),
            externalTableExecutors.size(),
            schemaContributors.size()
        );
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
        ExternalTableExecutor externalExecutor = externalTableExecutors.isEmpty() ? null : externalTableExecutors.get(0);
        return List.of(
            new DefaultPlanExecutor(backEnds, null/* TODO: pass indices service */, clusterService, externalExecutor),
            new DefaultEngineContext(clusterService, operatorTable, schemaContributors)
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(new TypeLiteral<QueryPlanExecutor<RelNode, Iterable<Object[]>>>() {
            }).to(DefaultPlanExecutor.class);
            b.bind(EngineContext.class).to(DefaultEngineContext.class);
        });
    }

    private SqlOperatorTable aggregateOperatorTables() {
        // TODO: re-wire once operatorTable() is added back to AnalyticsSearchBackendPlugin
        return SqlOperatorTables.of();
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
