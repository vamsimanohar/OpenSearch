/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Inter-stage data exchange: routes Arrow IPC batches between stages using hash, broadcast, or gather partitioning.
 */
package org.opensearch.lakehouse.distributed.exchange;
