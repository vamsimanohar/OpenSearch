/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.schema.ExternalTable;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link QueryPlanExecutor} default implementation.
 * <p>
 * Acquires a composite reader, selects a {@link AnalyticsSearchBackendPlugin}, and
 * delegates query execution to it.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;
    private volatile IndicesService indicesService;
    private final ClusterService clusterService;
    private final ExternalTableExecutor externalTableExecutor;

    /**
     * Constructs a DefaultPlanExecutor.
     *
     * @param providers list of search execution engine providers
     * @param indicesService service for accessing index shards
     * @param clusterService service for accessing cluster state
     * @param externalTableExecutor executor for external (non-OpenSearch) tables, may be null
     */
    public DefaultPlanExecutor(
        List<AnalyticsSearchBackendPlugin> providers,
        IndicesService indicesService,
        ClusterService clusterService,
        ExternalTableExecutor externalTableExecutor
    ) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin provider : providers) {
            this.backEnds.put(provider.name(), provider);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.externalTableExecutor = externalTableExecutor;

        // Eagerly register the backend executor for distributed workers.
        // Each node creates DefaultPlanExecutor during plugin init, so every node
        // has the executor available before any distributed queries arrive.
        if (!backEnds.isEmpty()) {
            AnalyticsSearchBackendPlugin firstBackend = backEnds.values().iterator().next();
            ExternalScanContext.setGlobalBackendExecutor(firstBackend::executeRemoteQuery);
            logger.info("[DefaultPlanExecutor] Registered global backend executor [{}] for distributed workers", firstBackend.name());
        }
    }

    /**
     * Guice member injection — IndicesService is not available during createComponents(),
     * so it's injected after the Guice injector is built.
     *
     * @param indicesService the indices service to inject
     */
    @Inject
    public void setIndicesService(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        // Route external (non-OpenSearch) tables through the native backend
        logger.debug("[DefaultPlanExecutor] Executing plan:\n{}", logicalFragment.explain());
        ExternalTable externalTable = extractExternalTable(logicalFragment);
        if (externalTable != null) {
            logger.debug("[DefaultPlanExecutor] Detected external table: type={}", externalTable.getClass().getSimpleName());
            if (externalTableExecutor == null) {
                throw new IllegalStateException("Query references an external table but no ExternalTableExecutor is registered");
            }

            ExternalScanContext scanContext = externalTableExecutor.prepareScan(logicalFragment, externalTable);
            if (scanContext == null) {
                throw new IllegalStateException("ExternalTableExecutor.prepareScan() returned null for " + externalTable);
            }
            logger.debug("[DefaultPlanExecutor] ScanContext: table={}, files={}, sqlQuery={}, storageConfigKeys={}",
                scanContext.getTableName(),
                scanContext.getDataFilePaths() != null ? scanContext.getDataFilePaths().size() : 0,
                scanContext.getSqlQuery(),
                scanContext.getStorageConfig() != null ? scanContext.getStorageConfig().keySet() : "null");
            AnalyticsSearchBackendPlugin provider = selectBackEnd();
            if (provider == null) {
                throw new IllegalStateException("No analytics backend registered for remote query execution");
            }
            // If the scan context carries pre-computed results from distributed execution,
            // return them directly instead of delegating to the single-node backend.
            Iterable<Object[]> preComputed = scanContext.getPreComputedResults();
            if (preComputed != null) {
                logger.info("[DefaultPlanExecutor] Returning pre-computed distributed results for external table");
                return preComputed;
            }

            logger.info("[DefaultPlanExecutor] Routing external table to native backend [{}]", provider.name());
            return provider.executeRemoteQuery(scanContext);
        }

        String tableName = extractTableName(logicalFragment);
        AnalyticsSearchBackendPlugin provider = selectBackEnd();
        if (provider == null) {
            return new ArrayList<>();
        }

        IndexShard shard = resolveShard(tableName);
        DataFormatAwareEngine dataFormatAwareEngine = shard.getCompositeEngine();
        if (dataFormatAwareEngine == null) {
            throw new IllegalStateException("No CompositeEngine on shard [" + shard.shardId() + "]");
        }

        SearchShardTask task = null; // TODO: init task
        List<Object[]> rows = new ArrayList<>();
        try (var dataFormatAwareReader = dataFormatAwareEngine.acquireReader()) {
            ExecutionContext ctx = new ExecutionContext(tableName, task, dataFormatAwareReader.get());
            try (SearchExecEngine<ExecutionContext, EngineResultStream> engine = provider.createSearchExecEngine(ctx)) {
                logger.info("[DefaultPlanExecutor] Executing via [{}]", provider.name());
                try (EngineResultStream resultStream = engine.execute(ctx)) {
                    Iterator<EngineResultBatch> batchIterator = resultStream.iterator();
                    while (batchIterator.hasNext()) {
                        EngineResultBatch batch = batchIterator.next();
                        List<String> fieldNames = batch.getFieldNames();
                        for (int row = 0; row < batch.getRowCount(); row++) {
                            Object[] rowValues = new Object[fieldNames.size()];
                            for (int col = 0; col < fieldNames.size(); col++) {
                                rowValues[col] = batch.getFieldValue(fieldNames.get(col), row);
                            }
                            rows.add(rowValues);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Execution failed for [" + provider.name() + "]", e);
        }
        return rows;
    }

    static String extractTableName(RelNode node) {
        if (node instanceof TableScan) {
            List<String> qn = node.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        throw new IllegalArgumentException("No TableScan found in plan fragment");
    }

    /**
     * Walks the RelNode tree to find a TableScan whose underlying Calcite table
     * implements {@link ExternalTable}. Returns the first match, or {@code null}
     * if every table in the plan is a regular OpenSearch index table.
     */
    static ExternalTable extractExternalTable(RelNode node) {
        if (node instanceof TableScan) {
            Table table = node.getTable().unwrap(Table.class);
            if (table instanceof ExternalTable) {
                return (ExternalTable) table;
            }
        }
        for (RelNode input : node.getInputs()) {
            ExternalTable found = extractExternalTable(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private IndexShard resolveShard(String indexName) {
        IndexService indexService = indicesService.indexService(clusterService.state().metadata().index(indexName).getIndex());
        if (indexService == null) throw new IllegalStateException("Index [" + indexName + "] not on this node");
        Set<Integer> shardIds = indexService.shardIds();
        if (shardIds.isEmpty()) throw new IllegalStateException("No shards for [" + indexName + "]");
        return indexService.getShardOrNull(shardIds.iterator().next());
    }

    private AnalyticsSearchBackendPlugin selectBackEnd() {
        if (backEnds.isEmpty()) {
            logger.warn("No back-end plugins registered — queries will return empty results");
            return null;
        }
        // TODO: select based on data format available in the catalog snapshot
        return backEnds.values().iterator().next();
    }
}
