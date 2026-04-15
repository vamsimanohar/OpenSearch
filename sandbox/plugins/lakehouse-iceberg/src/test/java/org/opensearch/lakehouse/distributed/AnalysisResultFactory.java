/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.sql.SqlKind;
import org.opensearch.lakehouse.distributed.merge.MergeStrategy;

/**
 * Test factory for creating {@link QueryAnalyzer.AnalysisResult} instances
 * from outside the {@code distributed} package. The constructors are package-private,
 * so this factory (in the same package) provides public access for tests.
 */
public final class AnalysisResultFactory {

    private AnalysisResultFactory() {}

    public static QueryAnalyzer.AnalysisResult create(MergeStrategy strategy) {
        return new QueryAnalyzer.AnalysisResult(strategy);
    }

    public static QueryAnalyzer.AnalysisResult create(
        MergeStrategy strategy,
        SqlKind[] aggKinds,
        int[] sortColumns,
        boolean[] sortAsc,
        int limit
    ) {
        return new QueryAnalyzer.AnalysisResult(strategy, aggKinds, sortColumns, sortAsc, limit, null);
    }

    public static QueryAnalyzer.AnalysisResult create(
        MergeStrategy strategy,
        SqlKind[] aggKinds,
        int[] sortColumns,
        boolean[] sortAsc,
        int limit,
        boolean[] isGroupKey
    ) {
        return new QueryAnalyzer.AnalysisResult(strategy, aggKinds, sortColumns, sortAsc, limit, isGroupKey);
    }
}
