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

public final class StageDAG {
    private final List<Stage> stages;
    private final StageId rootStageId;

    public StageDAG(List<Stage> stages, StageId rootStageId) {
        this.stages = List.copyOf(stages);
        this.rootStageId = rootStageId;
    }

    public List<Stage> getStages() { return stages; }
    public StageId getRootStageId() { return rootStageId; }
    public Stage getRootStage() { return getStage(rootStageId); }
    public int stageCount() { return stages.size(); }

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
