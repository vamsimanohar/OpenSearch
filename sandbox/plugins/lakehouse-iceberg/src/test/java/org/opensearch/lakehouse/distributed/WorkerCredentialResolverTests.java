/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.lakehouse.catalog.AwsCredentials;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class WorkerCredentialResolverTests extends OpenSearchTestCase {

    // ---- resolveWithConnector: early-return paths ----

    public void testNoIndexNameReturnsEarly() {
        ClusterService clusterService = mock(ClusterService.class);
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);

        Map<String, String> config = new HashMap<>();
        config.put("s3Region", "us-west-2");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        assertEquals("us-west-2", result.get("s3Region"));
        assertNull(result.get("s3AccessKeyId"));
        verifyNoInteractions(clusterService);
        verifyNoInteractions(connector);
    }

    public void testLocalModeReturnsEarly() {
        ClusterService clusterService = mock(ClusterService.class);
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("localMode", "true");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        assertEquals("true", result.get("localMode"));
        assertNull(result.get("s3AccessKeyId"));
        // indexName should NOT be removed when localMode early-returns
        // (it IS removed — config.remove("indexName") happens before the check)
        assertNull(result.get("indexName"));
        verifyNoInteractions(clusterService);
        verifyNoInteractions(connector);
    }

    public void testDefaultAuthExplicitReturnsEarly() {
        ClusterService clusterService = mock(ClusterService.class);
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("authType", "default");
        config.put("s3Region", "us-west-2");
        config.put("s3Bucket", "test-bucket");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        assertNull(result.get("s3AccessKeyId"));
        assertNull(result.get("s3SecretAccessKey"));
        assertNull(result.get("s3SessionToken"));
        assertEquals("us-west-2", result.get("s3Region"));
        assertEquals("test-bucket", result.get("s3Bucket"));
        assertEquals("default", result.get("authType"));
        assertNull(result.get("indexName"));
        verifyNoInteractions(clusterService);
        verifyNoInteractions(connector);
    }

    public void testMissingAuthTypeDefaultsToDefault() {
        ClusterService clusterService = mock(ClusterService.class);
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("s3Region", "us-west-2");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        assertNull(result.get("s3AccessKeyId"));
        verifyNoInteractions(clusterService);
        verifyNoInteractions(connector);
    }

    // ---- resolveWithConnector: role/keys auth paths ----

    public void testRoleAuthIndexNotFoundInClusterState() {
        ClusterService clusterService = mockClusterServiceWithIndex("test_index", null);
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("authType", "role");
        config.put("s3Region", "us-west-2");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        assertNull(result.get("s3AccessKeyId"));
        assertEquals("us-west-2", result.get("s3Region"));
        verifyNoInteractions(connector);
    }

    public void testRoleAuthCredentialsWithSessionToken() {
        IndexMetadata indexMetadata = buildIndexMetadata("test_index", Settings.builder()
            .put("index.lakehouse.type", "glue")
            .put("index.lakehouse.region", "us-west-2")
            .put("index.lakehouse.warehouse", "s3://bucket/warehouse")
            .put("index.lakehouse.namespace", "db")
            .put("index.lakehouse.table", "t")
            .put("index.lakehouse.auth_type", "role")
            .put("index.lakehouse.role_arn", "arn:aws:iam::123456789012:role/my-role")
        );
        ClusterService clusterService = mockClusterServiceWithIndex("test_index", indexMetadata);
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", "TOKEN");
        when(connector.getCredentials(any())).thenReturn(creds);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("authType", "role");
        config.put("s3Region", "us-west-2");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        assertEquals("AKID", result.get("s3AccessKeyId"));
        assertEquals("SECRET", result.get("s3SecretAccessKey"));
        assertEquals("TOKEN", result.get("s3SessionToken"));
        assertEquals("us-west-2", result.get("s3Region"));
        verify(connector).getCredentials(any());
    }

    public void testRoleAuthCredentialsWithoutSessionToken() {
        IndexMetadata indexMetadata = buildIndexMetadata("test_index", Settings.builder()
            .put("index.lakehouse.type", "glue")
            .put("index.lakehouse.region", "us-west-2")
            .put("index.lakehouse.warehouse", "s3://bucket/warehouse")
            .put("index.lakehouse.namespace", "db")
            .put("index.lakehouse.table", "t")
            .put("index.lakehouse.auth_type", "role")
        );
        ClusterService clusterService = mockClusterServiceWithIndex("test_index", indexMetadata);
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", null);
        when(connector.getCredentials(any())).thenReturn(creds);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("authType", "role");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        assertEquals("AKID", result.get("s3AccessKeyId"));
        assertEquals("SECRET", result.get("s3SecretAccessKey"));
        assertNull(result.get("s3SessionToken"));
    }

    public void testRoleAuthCredentialsNull() {
        IndexMetadata indexMetadata = buildIndexMetadata("test_index", Settings.builder()
            .put("index.lakehouse.type", "glue")
            .put("index.lakehouse.region", "us-west-2")
            .put("index.lakehouse.warehouse", "s3://bucket/warehouse")
            .put("index.lakehouse.namespace", "db")
            .put("index.lakehouse.table", "t")
            .put("index.lakehouse.auth_type", "role")
        );
        ClusterService clusterService = mockClusterServiceWithIndex("test_index", indexMetadata);
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);
        when(connector.getCredentials(any())).thenReturn(null);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("authType", "role");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        assertNull(result.get("s3AccessKeyId"));
        assertNull(result.get("s3SecretAccessKey"));
    }

    public void testRoleAuthCredentialsIncomplete() {
        IndexMetadata indexMetadata = buildIndexMetadata("test_index", Settings.builder()
            .put("index.lakehouse.type", "glue")
            .put("index.lakehouse.region", "us-west-2")
            .put("index.lakehouse.warehouse", "s3://bucket/warehouse")
            .put("index.lakehouse.namespace", "db")
            .put("index.lakehouse.table", "t")
            .put("index.lakehouse.auth_type", "role")
        );
        ClusterService clusterService = mockClusterServiceWithIndex("test_index", indexMetadata);
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);
        // Incomplete: missing secret key
        AwsCredentials creds = new AwsCredentials("AKID", "", "TOKEN");
        when(connector.getCredentials(any())).thenReturn(creds);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("authType", "role");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        // isComplete() returns false — credentials should NOT be added
        assertNull(result.get("s3AccessKeyId"));
        assertNull(result.get("s3SecretAccessKey"));
    }

    public void testRoleAuthExceptionHandled() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenThrow(new RuntimeException("cluster state unavailable"));
        IcebergCatalogConnector connector = mock(IcebergCatalogConnector.class);

        Map<String, String> config = new HashMap<>();
        config.put("indexName", "test_index");
        config.put("authType", "role");
        config.put("s3Region", "us-west-2");

        Map<String, String> result = WorkerCredentialResolver.resolveWithConnector(config, clusterService, connector);

        // Exception caught and logged — returns config without credentials
        assertNull(result.get("s3AccessKeyId"));
        assertEquals("us-west-2", result.get("s3Region"));
        verifyNoInteractions(connector);
    }

    // ---- resolve (public 2-arg): delegates via LakehouseState singleton ----

    public void testPublicResolveLocalModeReturnsEarly() {
        // The public resolve() calls LakehouseState.instance().catalogConnector() eagerly,
        // then delegates to resolveWithConnector. With localMode=true the connector is unused.
        ClusterService clusterService = mock(ClusterService.class);

        Map<String, String> config = new HashMap<>();
        config.put("localMode", "true");
        config.put("s3Region", "us-west-2");

        Map<String, String> result = WorkerCredentialResolver.resolve(config, clusterService);

        assertEquals("true", result.get("localMode"));
        assertEquals("us-west-2", result.get("s3Region"));
        verifyNoInteractions(clusterService);
    }

    // ---- helpers ----

    private static IndexMetadata buildIndexMetadata(String name, Settings.Builder extraSettings) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(extraSettings.build())
            .build();
        return IndexMetadata.builder(name).settings(settings).build();
    }

    private static ClusterService mockClusterServiceWithIndex(String indexName, IndexMetadata indexMetadata) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        if (indexMetadata != null) {
            metadataBuilder.put(indexMetadata, false);
        }
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(metadataBuilder)
            .build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }
}
