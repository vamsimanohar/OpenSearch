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
 * <b>Index settings</b> (per-index, set at index creation time):
 * <pre>
 *   index.lakehouse.enabled        — marks this index as an external lakehouse table
 *   index.lakehouse.type           — catalog type: glue, hadoop, rest
 *   index.lakehouse.region         — AWS region (for Glue/S3)
 *   index.lakehouse.warehouse      — warehouse path (s3:// or file://)
 *   index.lakehouse.namespace      — Iceberg namespace (e.g., Glue database name)
 *   index.lakehouse.table          — Iceberg table name within the namespace
 *   index.lakehouse.auth_type      — authentication type: role, keys, default
 *   index.lakehouse.role_arn       — IAM role ARN (for auth_type=role)
 *   index.lakehouse.credential_key — keystore credential name (for auth_type=keys)
 * </pre>
 * <p>
 * <b>Keystore credentials</b> (node-scoped, for auth_type=keys):
 * <pre>
 *   lakehouse.credentials.{name}.access_key     — AWS access key (keystore)
 *   lakehouse.credentials.{name}.secret_key     — AWS secret key (keystore)
 *   lakehouse.credentials.{name}.session_token  — STS session token (keystore, optional)
 * </pre>
 */
public final class LakehouseSettings {

    private LakehouseSettings() {}

    // ── Index settings (per-index, immutable after creation) ──

    /** Whether this index represents an external lakehouse table. */
    public static final Setting<Boolean> INDEX_LAKEHOUSE_ENABLED = Setting.boolSetting(
        "index.lakehouse.enabled",
        false,
        Property.IndexScope,
        Property.Final
    );

    /** Catalog type: glue, hadoop, rest. */
    public static final Setting<String> INDEX_LAKEHOUSE_TYPE = Setting.simpleString(
        "index.lakehouse.type",
        Property.IndexScope,
        Property.Final
    );

    /** AWS region for Glue/S3 access. */
    public static final Setting<String> INDEX_LAKEHOUSE_REGION = Setting.simpleString(
        "index.lakehouse.region",
        Property.IndexScope,
        Property.Final
    );

    /** Warehouse location (s3://bucket/path or file:///path). */
    public static final Setting<String> INDEX_LAKEHOUSE_WAREHOUSE = Setting.simpleString(
        "index.lakehouse.warehouse",
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

    /** Authentication type: role, keys, or default. */
    public static final Setting<String> INDEX_LAKEHOUSE_AUTH_TYPE = Setting.simpleString(
        "index.lakehouse.auth_type",
        Property.IndexScope,
        Property.Final
    );

    /** IAM role ARN for assume-role authentication (auth_type=role). */
    public static final Setting<String> INDEX_LAKEHOUSE_ROLE_ARN = Setting.simpleString(
        "index.lakehouse.role_arn",
        Property.IndexScope,
        Property.Final
    );

    /** Keystore credential name for static key authentication (auth_type=keys). */
    public static final Setting<String> INDEX_LAKEHOUSE_CREDENTIAL_KEY = Setting.simpleString(
        "index.lakehouse.credential_key",
        Property.IndexScope,
        Property.Final
    );

    // ── Keystore credentials (node-scoped, for auth_type=keys) ──

    /** AWS access key ID (stored in opensearch-keystore). */
    public static final Setting.AffixSetting<SecureString> CREDENTIAL_ACCESS_KEY = Setting.affixKeySetting(
        "lakehouse.credentials.",
        "access_key",
        key -> SecureSetting.secureString(key, null)
    );

    /** AWS secret access key (stored in opensearch-keystore). */
    public static final Setting.AffixSetting<SecureString> CREDENTIAL_SECRET_KEY = Setting.affixKeySetting(
        "lakehouse.credentials.",
        "secret_key",
        key -> SecureSetting.secureString(key, null)
    );

    /** AWS STS session token (stored in opensearch-keystore, optional). */
    public static final Setting.AffixSetting<SecureString> CREDENTIAL_SESSION_TOKEN = Setting.affixKeySetting(
        "lakehouse.credentials.",
        "session_token",
        key -> SecureSetting.secureString(key, null)
    );

    /** Returns all settings to register with the plugin. */
    public static List<Setting<?>> all() {
        return List.of(
            INDEX_LAKEHOUSE_ENABLED,
            INDEX_LAKEHOUSE_TYPE,
            INDEX_LAKEHOUSE_REGION,
            INDEX_LAKEHOUSE_WAREHOUSE,
            INDEX_LAKEHOUSE_NAMESPACE,
            INDEX_LAKEHOUSE_TABLE,
            INDEX_LAKEHOUSE_AUTH_TYPE,
            INDEX_LAKEHOUSE_ROLE_ARN,
            INDEX_LAKEHOUSE_CREDENTIAL_KEY,
            CREDENTIAL_ACCESS_KEY,
            CREDENTIAL_SECRET_KEY,
            CREDENTIAL_SESSION_TOKEN
        );
    }
}
