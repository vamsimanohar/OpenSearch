/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;

/**
 * SPI for shard-level query execution backends that provide a full {@link SearchExecEngine}
 * with prepare/execute/stream semantics for the analytics query path.
 * <p>
 * Discovered via {@link org.opensearch.plugins.ExtensiblePlugin} and used by the
 * analytics executor for shard-level query lifecycle.
 *
 * @opensearch.internal
 */
public interface SearchExecEngineProvider {

    /**
     * Creates a search execution engine bound to the given execution context.
     * The context carries the reader snapshot and task metadata.
     */
    SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx);
}
