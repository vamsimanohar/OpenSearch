/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.engine;

import java.util.List;

/**
 * A complete distributed execution plan: an ordered list of {@link PlanFragment} stages.
 * <p>
 * Stages execute in order: stage 0 (leaf) runs first on all workers, its output
 * flows via the specified exchange to stage 1, and so on. The last stage runs on
 * the coordinator and produces the final result.
 * <p>
 * A plan with {@code singleNode=true} means the query cannot be distributed and
 * must execute on a single node. In this case, the fragment list is empty.
 *
 * @opensearch.internal
 */
public final class SubPlan {

    private final List<PlanFragment> stages;
    private final boolean singleNode;

    private SubPlan(List<PlanFragment> stages, boolean singleNode) {
        this.stages = stages;
        this.singleNode = singleNode;
    }

    /**
     * Creates a distributed plan with the given stages.
     *
     * @param stages ordered list of plan fragments (stage 0 = leaf, last = coordinator)
     */
    public static SubPlan distributed(List<PlanFragment> stages) {
        if (stages == null || stages.isEmpty()) {
            throw new IllegalArgumentException("Distributed plan requires at least one stage");
        }
        return new SubPlan(List.copyOf(stages), false);
    }

    /**
     * Creates a single-node plan indicating the query cannot be distributed.
     */
    public static SubPlan singleNode() {
        return new SubPlan(List.of(), true);
    }

    public List<PlanFragment> getStages() {
        return stages;
    }

    public boolean isSingleNode() {
        return singleNode;
    }

    /**
     * Returns the leaf (first) fragment, or null if single-node.
     */
    public PlanFragment getLeafStage() {
        return singleNode ? null : stages.get(0);
    }

    /**
     * Returns the final (coordinator) fragment, or null if single-node.
     */
    public PlanFragment getFinalStage() {
        return singleNode ? null : stages.get(stages.size() - 1);
    }

    /**
     * Returns the number of stages.
     */
    public int getStageCount() {
        return stages.size();
    }

    @Override
    public String toString() {
        if (singleNode) {
            return "SubPlan{SINGLE_NODE}";
        }
        return "SubPlan{stages=" + stages.size() + ", " + stages + "}";
    }
}
