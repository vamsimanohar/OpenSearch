/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import java.util.Locale;

/**
 * Supported Iceberg catalog types.
 */
public enum CatalogType {
    /** AWS Glue Data Catalog. */
    GLUE,
    /** Local Hadoop-based catalog (for testing). */
    HADOOP,
    /** Iceberg REST Catalog. */
    REST;

    /**
     * Parses a catalog type string (case-insensitive).
     *
     * @param value the string to parse
     * @return the matching CatalogType
     * @throws IllegalArgumentException if the value is not a valid catalog type
     */
    public static CatalogType fromString(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }
}
