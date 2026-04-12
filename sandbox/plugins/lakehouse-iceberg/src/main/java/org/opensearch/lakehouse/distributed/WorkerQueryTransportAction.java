/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.analytics.exec.RemoteQueryBackendHolder;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.lakehouse.catalog.AwsCredentials;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport action that executes a distributed query fragment on a worker node.
 * <p>
 * Receives a {@link WorkerQueryRequest} containing SQL, file paths, and storage config,
 * builds an {@link ExternalScanContext}, and delegates to the DataFusion backend
 * via {@link AnalyticsSearchBackendPlugin#executeRemoteQuery(ExternalScanContext)}.
 * <p>
 * The backend provider is discovered via {@link RemoteQueryBackendHolder}, which is
 * set by the analytics-engine plugin during initialization.
 *
 * @opensearch.internal
 */
public class WorkerQueryTransportAction extends HandledTransportAction<WorkerQueryRequest, WorkerQueryResponse> {

    private static final Logger logger = LogManager.getLogger(WorkerQueryTransportAction.class);

    private static volatile AnalyticsSearchBackendPlugin backendProvider;

    private final ClusterService clusterService;

    /**
     * Sets the backend provider used to execute queries on worker nodes.
     * Visible for testing.
     *
     * @param provider the analytics search backend plugin (e.g., DataFusion)
     */
    public static void setBackendProvider(AnalyticsSearchBackendPlugin provider) {
        backendProvider = provider;
    }

    /** Returns the current backend provider, or null if not set. Visible for testing. */
    static AnalyticsSearchBackendPlugin getBackendProvider() {
        return backendProvider;
    }

    /**
     * Creates the transport action via Guice injection.
     * Initializes the distributed scan executor on {@link LakehouseState}
     * since this is the earliest point where TransportService and ClusterService
     * are available via Guice.
     *
     * @param transportService the transport service
     * @param actionFilters    the action filters
     * @param clusterService   the cluster service
     */
    @Inject
    public WorkerQueryTransportAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(WorkerQueryAction.NAME, transportService, actionFilters, WorkerQueryRequest::new, ThreadPool.Names.GENERIC);
        this.clusterService = clusterService;
        LakehouseState.instance().initDistributedExecutor(transportService, clusterService);
    }

    @Override
    @SuppressWarnings("removal")
    protected void doExecute(Task task, WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        try {
            AnalyticsSearchBackendPlugin provider = resolveBackend();

            // Resolve credentials locally on this worker (via IMDS/STS) instead of
            // receiving them from the coordinator. Each worker has the same instance
            // profile, so no secrets need to travel over the wire.
            Map<String, String> storageConfig = resolveLocalCredentials(request.getStorageConfig());

            ExternalScanContext scanContext = new ExternalScanContext(
                request.getTableName(),
                request.getFilePaths(),
                request.getFileSizes(),
                request.getSqlQuery(),
                storageConfig
            );

            logger.info(
                "[WorkerQuery] Executing on worker: table={}, files={}, sql={}",
                request.getTableName(),
                request.getFilePaths().size(),
                request.getSqlQuery()
            );

            long t0 = System.currentTimeMillis();
            Iterable<Object[]> rows = AccessController.doPrivileged(
                (PrivilegedAction<Iterable<Object[]>>) () -> provider.executeRemoteQuery(scanContext)
            );
            long t1 = System.currentTimeMillis();

            WorkerQueryResponse response = buildResponse(rows);
            logger.info("[PERF] Worker query: {}ms ({} rows)", t1 - t0, response.getRowCount());
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[WorkerQuery] Execution failed", e);
            listener.onFailure(e);
        }
    }

    /**
     * Resolves AWS credentials locally on this worker node using the index settings
     * from cluster state. The coordinator passes only the index name (no secrets);
     * each worker independently calls IMDS/STS/DefaultCredentialsProvider.
     *
     * @param original the storageConfig from the coordinator (contains region, bucket, indexName)
     * @return a new map with credentials added
     */
    @SuppressWarnings("removal")
    Map<String, String> resolveLocalCredentials(Map<String, String> original) {
        Map<String, String> config = new HashMap<>(original);
        String indexName = config.remove("indexName");
        if (indexName == null || "true".equals(config.get("localMode"))) {
            return config;
        }

        // For "default" auth, Rust's object_store uses IMDS directly on each worker.
        // No Java credential resolution needed — no secrets on the wire at all.
        String authType = config.getOrDefault("authType", "default");
        if ("default".equals(authType)) {
            logger.debug("[WorkerQuery] auth_type=default for index [{}], Rust will use IMDS directly", indexName);
            return config;
        }

        // For "role" and "keys" auth, resolve credentials locally from cluster state.
        try {
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata == null) {
                logger.warn("[WorkerQuery] Index [{}] not found in cluster state, skipping credential resolution", indexName);
                return config;
            }
            CatalogConfig catalogConfig = CatalogConfig.fromIndexSettings(indexMetadata);
            IcebergCatalogConnector connector = LakehouseState.instance().catalogConnector();
            AwsCredentials creds = AccessController.doPrivileged(
                (PrivilegedAction<AwsCredentials>) () -> connector.getCredentials(catalogConfig)
            );
            if (creds != null && creds.isComplete()) {
                config.put("s3AccessKeyId", creds.getAccessKeyId());
                config.put("s3SecretAccessKey", creds.getSecretAccessKey());
                if (creds.getSessionToken() != null) {
                    config.put("s3SessionToken", creds.getSessionToken());
                }
            }
        } catch (Exception e) {
            logger.warn("[WorkerQuery] Local credential resolution failed for index [{}]: {}", indexName, e.getMessage());
        }
        return config;
    }

    /**
     * Executes a worker query locally without going through transport serialization.
     * Called by {@link DistributedScanExecutor} for the local node to avoid
     * unnecessary serialization/deserialization overhead.
     *
     * @param request        the worker query request
     * @param clusterService the cluster service for credential resolution
     * @return the worker query response
     */
    @SuppressWarnings("removal")
    static WorkerQueryResponse executeLocally(WorkerQueryRequest request, ClusterService clusterService) {
        AnalyticsSearchBackendPlugin provider = resolveBackend();

        Map<String, String> storageConfig = resolveLocalCredentialsStatic(request.getStorageConfig(), clusterService);

        ExternalScanContext scanContext = new ExternalScanContext(
            request.getTableName(),
            request.getFilePaths(),
            request.getFileSizes(),
            request.getSqlQuery(),
            storageConfig
        );

        logger.info(
            "[WorkerQuery] Executing locally (no transport): table={}, files={}, sql={}",
            request.getTableName(),
            request.getFilePaths().size(),
            request.getSqlQuery()
        );

        long t0 = System.currentTimeMillis();
        Iterable<Object[]> rows = AccessController.doPrivileged(
            (PrivilegedAction<Iterable<Object[]>>) () -> provider.executeRemoteQuery(scanContext)
        );
        long t1 = System.currentTimeMillis();

        WorkerQueryResponse response = buildResponse(rows);
        logger.info("[PERF] Local worker query: {}ms ({} rows)", t1 - t0, response.getRowCount());
        return response;
    }

    /**
     * Static version of resolveLocalCredentials for use without an instance.
     */
    @SuppressWarnings("removal")
    private static Map<String, String> resolveLocalCredentialsStatic(Map<String, String> original, ClusterService clusterService) {
        Map<String, String> config = new HashMap<>(original);
        String indexName = config.remove("indexName");
        if (indexName == null || "true".equals(config.get("localMode"))) {
            return config;
        }

        String authType = config.getOrDefault("authType", "default");
        if ("default".equals(authType)) {
            logger.debug("[WorkerQuery] auth_type=default for index [{}], Rust will use IMDS directly", indexName);
            return config;
        }

        try {
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata == null) {
                logger.warn("[WorkerQuery] Index [{}] not found in cluster state, skipping credential resolution", indexName);
                return config;
            }
            CatalogConfig catalogConfig = CatalogConfig.fromIndexSettings(indexMetadata);
            IcebergCatalogConnector connector = LakehouseState.instance().catalogConnector();
            AwsCredentials creds = AccessController.doPrivileged(
                (PrivilegedAction<AwsCredentials>) () -> connector.getCredentials(catalogConfig)
            );
            if (creds != null && creds.isComplete()) {
                config.put("s3AccessKeyId", creds.getAccessKeyId());
                config.put("s3SecretAccessKey", creds.getSecretAccessKey());
                if (creds.getSessionToken() != null) {
                    config.put("s3SessionToken", creds.getSessionToken());
                }
            }
        } catch (Exception e) {
            logger.warn("[WorkerQuery] Local credential resolution failed for index [{}]: {}", indexName, e.getMessage());
        }
        return config;
    }

    /**
     * Resolves the backend provider from static field or RemoteQueryBackendHolder.
     */
    private static AnalyticsSearchBackendPlugin resolveBackend() {
        AnalyticsSearchBackendPlugin provider = backendProvider;
        if (provider == null) {
            provider = RemoteQueryBackendHolder.getProvider();
            if (provider != null) {
                backendProvider = provider;
            }
        }
        if (provider == null) {
            throw new IllegalStateException("No analytics backend registered for worker query execution");
        }
        return provider;
    }

    /**
     * Converts the row-oriented result from the backend into a column-oriented response.
     *
     * @param rows iterable of row arrays from executeRemoteQuery
     * @return column-oriented WorkerQueryResponse
     */
    static WorkerQueryResponse buildResponse(Iterable<Object[]> rows) {
        List<Object[]> rowList = new ArrayList<>();
        for (Object[] row : rows) {
            rowList.add(sanitizeRow(row));
        }

        if (rowList.isEmpty()) {
            return new WorkerQueryResponse(List.of(), List.of(), 0, new Object[0][]);
        }

        int numCols = rowList.get(0).length;
        int numRows = rowList.size();

        List<String> columnNames = new ArrayList<>(numCols);
        List<String> columnTypes = new ArrayList<>(numCols);
        Object[][] columnData = new Object[numCols][numRows];

        for (int col = 0; col < numCols; col++) {
            columnNames.add("col_" + col);
            for (int row = 0; row < numRows; row++) {
                columnData[col][row] = rowList.get(row)[col];
            }
            // Infer type from first non-null value
            String typeName = "UNKNOWN";
            for (int row = 0; row < numRows; row++) {
                if (columnData[col][row] != null) {
                    typeName = columnData[col][row].getClass().getSimpleName();
                    break;
                }
            }
            columnTypes.add(typeName);
        }

        return new WorkerQueryResponse(columnNames, columnTypes, numRows, columnData);
    }

    /**
     * Converts non-serializable types to types supported by StreamOutput.writeGenericValue().
     * LocalDateTime and LocalDate from DataFusion are converted to their ISO-8601 string form.
     */
    static Object[] sanitizeRow(Object[] row) {
        for (int i = 0; i < row.length; i++) {
            if (row[i] instanceof LocalDateTime) {
                row[i] = row[i].toString();
            } else if (row[i] instanceof LocalDate) {
                row[i] = row[i].toString();
            }
        }
        return row;
    }
}
