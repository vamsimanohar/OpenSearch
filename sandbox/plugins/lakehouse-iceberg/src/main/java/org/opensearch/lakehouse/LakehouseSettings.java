/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.core.common.settings.SecureString;

import java.util.List;

/**
 * Settings for the lakehouse plugin.
 * <p>
 * <b>Catalog settings</b> (node-scoped, from opensearch.yml / keystore):
 * <pre>
 *   lakehouse.catalog.{name}.type       — catalog type: glue, hadoop, rest
 *   lakehouse.catalog.{name}.region     — AWS region (for Glue/S3)
 *   lakehouse.catalog.{name}.warehouse  — warehouse path (s3:// or file://)
 *   lakehouse.catalog.{name}.access_key — AWS access key (keystore)
 *   lakehouse.catalog.{name}.secret_key — AWS secret key (keystore)
 *   lakehouse.catalog.{name}.session_token — STS session token (keystore)
 * </pre>
 * <p>
 * <b>Index settings</b> (per-index, set at index creation):
 * <pre>
 *   index.lakehouse.enabled   — marks this index as an external lakehouse table
 *   index.lakehouse.catalog   — references a catalog name from the catalog settings
 *   index.lakehouse.namespace — Iceberg namespace (e.g., Glue database name)
 *   index.lakehouse.table     — Iceberg table name within the namespace
 * </pre>
 */
public final class LakehouseSettings {

    private LakehouseSettings() {}

    // ── Catalog settings (node-scoped, from opensearch.yml / keystore) ──

    /** Catalog type: glue, hadoop, rest. */
    public static final Setting.AffixSetting<String> CATALOG_TYPE = Setting.affixKeySetting(
        "lakehouse.catalog.",
        "type",
        key -> Setting.simpleString(key, Property.NodeScope)
    );

    /** AWS region for Glue/S3 access. */
    public static final Setting.AffixSetting<String> CATALOG_REGION = Setting.affixKeySetting(
        "lakehouse.catalog.",
        "region",
        key -> Setting.simpleString(key, Property.NodeScope)
    );

    /** Warehouse location (s3://bucket/path or file:///path). */
    public static final Setting.AffixSetting<String> CATALOG_WAREHOUSE = Setting.affixKeySetting(
        "lakehouse.catalog.",
        "warehouse",
        key -> Setting.simpleString(key, Property.NodeScope)
    );

    // ── Catalog secure settings (keystore-backed) ──

    /** AWS access key ID (stored in opensearch-keystore). */
    public static final Setting.AffixSetting<SecureString> CATALOG_ACCESS_KEY = Setting.affixKeySetting(
        "lakehouse.catalog.",
        "access_key",
        key -> SecureSetting.secureString(key, null)
    );

    /** AWS secret access key (stored in opensearch-keystore). */
    public static final Setting.AffixSetting<SecureString> CATALOG_SECRET_KEY = Setting.affixKeySetting(
        "lakehouse.catalog.",
        "secret_key",
        key -> SecureSetting.secureString(key, null)
    );

    /** AWS STS session token (stored in opensearch-keystore, optional). */
    public static final Setting.AffixSetting<SecureString> CATALOG_SESSION_TOKEN = Setting.affixKeySetting(
        "lakehouse.catalog.",
        "session_token",
        key -> SecureSetting.secureString(key, null)
    );

    // ── Index settings (per-index, set at creation time) ──

    /** Whether this index represents an external lakehouse table. */
    public static final Setting<Boolean> INDEX_LAKEHOUSE_ENABLED = Setting.boolSetting(
        "index.lakehouse.enabled",
        false,
        Property.IndexScope,
        Property.Final
    );

    /** Name of the catalog this table belongs to. */
    public static final Setting<String> INDEX_LAKEHOUSE_CATALOG = Setting.simpleString(
        "index.lakehouse.catalog",
        Property.IndexScope,
        Property.Final
    );

    /** Iceberg namespace (e.g., Glue database name). */
    public static final Setting<String> INDEX_LAKEHOUSE_NAMESPACE = Setting.simpleString(
        "index.lakehouse.namespace",
        Property.IndexScope,
        Property.Final
    );

    /** Iceberg table name within the namespace. */
    public static final Setting<String> INDEX_LAKEHOUSE_TABLE = Setting.simpleString(
        "index.lakehouse.table",
        Property.IndexScope,
        Property.Final
    );

    /** Returns all settings to register with the plugin. */
    public static List<Setting<?>> all() {
        return List.of(
            CATALOG_TYPE,
            CATALOG_REGION,
            CATALOG_WAREHOUSE,
            CATALOG_ACCESS_KEY,
            CATALOG_SECRET_KEY,
            CATALOG_SESSION_TOKEN,
            INDEX_LAKEHOUSE_ENABLED,
            INDEX_LAKEHOUSE_CATALOG,
            INDEX_LAKEHOUSE_NAMESPACE,
            INDEX_LAKEHOUSE_TABLE
        );
    }
}
