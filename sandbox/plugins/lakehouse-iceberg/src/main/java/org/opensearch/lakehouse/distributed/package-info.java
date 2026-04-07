/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Distributed query execution for Iceberg lakehouse tables.
 *
 * <p>Splits queries into worker SQL (partial aggregation) and coordinator SQL
 * (final aggregation + sort + limit) for execution across multiple cluster nodes.
 */
package org.opensearch.lakehouse.distributed;
