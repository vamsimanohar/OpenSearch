/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.stage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A directed acyclic graph of stages representing a distributed execution plan. */
public final class StageDAG {
    private final List<Stage> stages;
    private final StageId rootStageId;

    /**
     * Creates a new StageDAG with the given stages and root stage identifier.
     *
     * @param stages      the stages in this DAG
     * @param rootStageId the root stage identifier
     */
    public StageDAG(List<Stage> stages, StageId rootStageId) {
        this.stages = List.copyOf(stages);
        this.rootStageId = rootStageId;
    }

    /** Returns all stages in this DAG. */
    public List<Stage> getStages() { return stages; }
    /** Returns the root stage identifier. */
    public StageId getRootStageId() { return rootStageId; }
    /** Returns the root stage. */
    public Stage getRootStage() { return getStage(rootStageId); }
    /** Returns the number of stages in this DAG. */
    public int stageCount() { return stages.size(); }

    /**
     * Returns the stage with the given identifier.
     *
     * @param id the stage identifier to look up
     * @return the matching stage
     */
    public Stage getStage(StageId id) {
        return stages.stream().filter(s -> s.getId().equals(id)).findFirst()
            .orElseThrow(() -> new IllegalArgumentException("No stage: " + id));
    }

    /** Returns stages in topological order (leaves first, root last). */
    public List<Stage> topologicalOrder() {
        Map<StageId, Set<StageId>> deps = new LinkedHashMap<>();
        Map<StageId, Stage> stageMap = new LinkedHashMap<>();
        for (Stage s : stages) {
            stageMap.put(s.getId(), s);
            deps.put(s.getId(), new LinkedHashSet<>(s.getSourceStages()));
        }
        List<Stage> result = new ArrayList<>();
        while (!deps.isEmpty()) {
            StageId ready = deps.entrySet().stream()
                .filter(e -> e.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Cycle in stage DAG"));
            result.add(stageMap.get(ready));
            deps.remove(ready);
            for (Set<StageId> d : deps.values()) d.remove(ready);
        }
        return result;
    }

    /** Returns leaf stages (no dependencies — these start first). */
    public List<Stage> getLeafStages() {
        return stages.stream().filter(Stage::isLeaf).toList();
    }

    /** Returns JSON representation for the explain API. */
    public Map<String, Object> toExplainMap() {
        List<Map<String, Object>> stageList = new ArrayList<>();
        for (Stage s : topologicalOrder()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("stageId", s.getId().toString());
            m.put("type", s.getType().name());
            m.put("sql", s.getSql());
            m.put("input", s.getInputSpec().toString());
            m.put("output", s.getOutputPartitioning().toString());
            m.put("sources", s.getSourceStages().stream().map(StageId::toString).toList());
            stageList.add(m);
        }
        return Map.of("stages", stageList, "root", rootStageId.toString(), "stageCount", stages.size());
    }
}
