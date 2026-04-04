/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

/**
 * Supported Iceberg catalog types.
 */
public enum CatalogType {
    GLUE,
    HIVE,
    REST
}
