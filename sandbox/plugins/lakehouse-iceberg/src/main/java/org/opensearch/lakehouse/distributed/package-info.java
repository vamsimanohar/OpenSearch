/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Distributed query execution for the lakehouse plugin.
 * <p>
 * Contains transport actions, request/response types, node discovery,
 * file partitioning, query analysis, result merging, and the distributed
 * scan executor for distributing Iceberg query execution across
 * multiple cluster nodes.
 */
package org.opensearch.lakehouse.distributed;
