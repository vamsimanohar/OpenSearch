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
 *
 * @opensearch.internal
 */
public final class SubPlan {

    private final List<PlanFragment> stages;

    private SubPlan(List<PlanFragment> stages) {
        this.stages = stages;
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
        return new SubPlan(List.copyOf(stages));
    }

    public List<PlanFragment> getStages() {
        return stages;
    }

    /**
     * Returns the leaf (first) fragment.
     */
    public PlanFragment getLeafStage() {
        return stages.get(0);
    }

    /**
     * Returns the final (coordinator) fragment.
     */
    public PlanFragment getFinalStage() {
        return stages.get(stages.size() - 1);
    }

    /**
     * Returns the number of stages.
     */
    public int getStageCount() {
        return stages.size();
    }

    @Override
    public String toString() {
        return "SubPlan{stages=" + stages.size() + ", " + stages + "}";
    }
}
