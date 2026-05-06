/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class CatalogConfigTests extends OpenSearchTestCase {

    public void testFromIndexSettings() {
        IndexMetadata indexMetadata = indexMetadata(
            "my_table",
            Settings.builder()
                .put("index.lakehouse.enabled", true)
                .put("index.lakehouse.type", "glue")
                .put("index.lakehouse.region", "us-west-2")
                .put("index.lakehouse.warehouse", "s3://my-bucket/warehouse")
                .put("index.lakehouse.namespace", "my_db")
                .put("index.lakehouse.table", "events")
                .put("index.lakehouse.auth_type", "role")
                .put("index.lakehouse.role_arn", "arn:aws:iam::123456789012:role/my-role")
                .put("index.lakehouse.credential_key", "my_cred")
        );

        CatalogConfig config = CatalogConfig.fromIndexSettings(indexMetadata);

        assertEquals("my_table", config.indexName());
        assertEquals(CatalogType.GLUE, config.catalogType());
        assertEquals("us-west-2", config.region());
        assertEquals("s3://my-bucket/warehouse", config.warehouse());
        assertEquals("my_db", config.namespace());
        assertEquals("events", config.tableName());
        assertEquals("role", config.authType());
        assertEquals("arn:aws:iam::123456789012:role/my-role", config.roleArn());
        assertEquals("my_cred", config.credentialKey());
    }

    public void testFromIndexSettingsDefaultAuthType() {
        IndexMetadata indexMetadata = indexMetadata(
            "my_table",
            Settings.builder()
                .put("index.lakehouse.type", "hadoop")
                .put("index.lakehouse.warehouse", "file:///tmp/warehouse")
                .put("index.lakehouse.namespace", "default")
                .put("index.lakehouse.table", "events")
        );

        CatalogConfig config = CatalogConfig.fromIndexSettings(indexMetadata);

        assertEquals("default", config.authType());
        assertEquals("", config.region());
        assertEquals("", config.roleArn());
    }

    public void testFromIndexSettingsMissingType() {
        IndexMetadata indexMetadata = indexMetadata(
            "my_table",
            Settings.builder()
                .put("index.lakehouse.warehouse", "s3://bucket")
                .put("index.lakehouse.namespace", "db")
                .put("index.lakehouse.table", "t")
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CatalogConfig.fromIndexSettings(indexMetadata));
        assertTrue(e.getMessage().contains("index.lakehouse.type"));
    }

    public void testFromIndexSettingsMissingWarehouse() {
        IndexMetadata indexMetadata = indexMetadata(
            "my_table",
            Settings.builder().put("index.lakehouse.type", "glue").put("index.lakehouse.namespace", "db").put("index.lakehouse.table", "t")
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CatalogConfig.fromIndexSettings(indexMetadata));
        assertTrue(e.getMessage().contains("index.lakehouse.warehouse"));
    }

    public void testFromIndexSettingsMissingNamespace() {
        IndexMetadata indexMetadata = indexMetadata(
            "my_table",
            Settings.builder()
                .put("index.lakehouse.type", "glue")
                .put("index.lakehouse.warehouse", "s3://bucket")
                .put("index.lakehouse.table", "t")
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CatalogConfig.fromIndexSettings(indexMetadata));
        assertTrue(e.getMessage().contains("index.lakehouse.namespace"));
    }

    public void testFromIndexSettingsMissingTable() {
        IndexMetadata indexMetadata = indexMetadata(
            "my_table",
            Settings.builder()
                .put("index.lakehouse.type", "glue")
                .put("index.lakehouse.warehouse", "s3://bucket")
                .put("index.lakehouse.namespace", "db")
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CatalogConfig.fromIndexSettings(indexMetadata));
        assertTrue(e.getMessage().contains("index.lakehouse.table"));
    }

    public void testConstructorNullIndexName() {
        expectThrows(
            NullPointerException.class,
            () -> new CatalogConfig(null, CatalogType.GLUE, "us-west-2", "s3://b", "ns", "t", "default", null, null)
        );
    }

    public void testConstructorNullCatalogType() {
        expectThrows(
            NullPointerException.class,
            () -> new CatalogConfig("idx", null, "us-west-2", "s3://b", "ns", "t", "default", null, null)
        );
    }

    public void testConstructorNullWarehouse() {
        expectThrows(
            NullPointerException.class,
            () -> new CatalogConfig("idx", CatalogType.GLUE, "us-west-2", null, "ns", "t", "default", null, null)
        );
    }

    public void testConstructorNullNamespace() {
        expectThrows(
            NullPointerException.class,
            () -> new CatalogConfig("idx", CatalogType.GLUE, "us-west-2", "s3://b", null, "t", "default", null, null)
        );
    }

    public void testConstructorNullTableName() {
        expectThrows(
            NullPointerException.class,
            () -> new CatalogConfig("idx", CatalogType.GLUE, "us-west-2", "s3://b", "ns", null, "default", null, null)
        );
    }

    public void testConstructorNullAuthTypeDefaultsToDefault() {
        CatalogConfig config = new CatalogConfig("idx", CatalogType.GLUE, "us-west-2", "s3://b", "ns", "t", null, null, null);
        assertEquals("default", config.authType());
    }

    public void testConstructorEmptyAuthTypeDefaultsToDefault() {
        CatalogConfig config = new CatalogConfig("idx", CatalogType.GLUE, "us-west-2", "s3://b", "ns", "t", "", null, null);
        assertEquals("default", config.authType());
    }

    public void testCredentialKeyAccessor() {
        CatalogConfig config = new CatalogConfig("idx", CatalogType.GLUE, "us-west-2", "s3://b", "ns", "t", "keys", null, "my_cred_key");
        assertEquals("my_cred_key", config.credentialKey());
    }

    public void testRegionCanBeNull() {
        CatalogConfig config = new CatalogConfig("idx", CatalogType.HADOOP, null, "file:///tmp", "ns", "t", "default", null, null);
        assertNull(config.region());
    }

    public void testRoleArnCanBeNull() {
        CatalogConfig config = new CatalogConfig("idx", CatalogType.GLUE, "us-west-2", "s3://b", "ns", "t", "default", null, null);
        assertNull(config.roleArn());
    }

    private static IndexMetadata indexMetadata(String name, Settings.Builder extraSettings) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(extraSettings.build())
            .build();
        return IndexMetadata.builder(name).settings(settings).build();
    }
}
