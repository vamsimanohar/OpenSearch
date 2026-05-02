/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 */

package org.opensearch.lakehouse.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Top-level state machine for a distributed query.
 * <p>
 * Tracks query lifecycle and provides methods to wire stage state machines
 * for bottom-up cascade (stage done → query finishing) and top-down
 * control (query failed → abort all stages).
 */
public class QueryStateMachine {

    private static final Logger logger = LogManager.getLogger(QueryStateMachine.class);

    private final String queryId;
    private final StateMachine<QueryState> stateMachine;
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();
    private final List<StageStateMachine> stages = new CopyOnWriteArrayList<>();
    private final Executor executor;

    public QueryStateMachine(String queryId, Executor executor) {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.stateMachine = new StateMachine<>("query-" + queryId, executor, QueryState.PLANNING, QueryState.TERMINAL_STATES);
    }

    public String getQueryId() {
        return queryId;
    }

    public QueryState getState() {
        return stateMachine.get();
    }

    public Throwable getFailureCause() {
        return failureCause.get();
    }

    public Executor getExecutor() {
        return executor;
    }

    public void addStateChangeListener(StateChangeListener<QueryState> listener) {
        stateMachine.addStateChangeListener(listener);
    }

    /**
     * Registers a stage and wires its state changes to this query.
     * Stage FAILED → query FAILED. Stage ABORTED → query FAILED.
     */
    public void addStage(StageStateMachine stage) {
        stages.add(stage);
        stage.addStateChangeListener(stageState -> {
            if (stageState == StageState.FAILED) {
                transitionToFailed(stage.getFailureCause());
            } else if (stageState == StageState.ABORTED) {
                transitionToFailed(new RuntimeException("Stage " + stage.getStageId() + " was aborted"));
            }
        });
    }

    // ---- Transitions ----

    public boolean transitionToStarting() {
        return stateMachine.setIf(QueryState.STARTING, state -> state.ordinal() < QueryState.STARTING.ordinal());
    }

    public boolean transitionToRunning() {
        return stateMachine.setIf(QueryState.RUNNING, state -> state.ordinal() < QueryState.RUNNING.ordinal());
    }

    public boolean transitionToFinishing() {
        return stateMachine.setIf(QueryState.FINISHING, state -> state != QueryState.FINISHING && !state.isDone());
    }

    public boolean transitionToFinished() {
        return stateMachine.setIf(QueryState.FINISHED, state -> !state.isDone());
    }

    /**
     * Transitions to FAILED and aborts all stages.
     */
    public boolean transitionToFailed(Throwable cause) {
        requireNonNull(cause, "cause is null");
        failureCause.compareAndSet(null, cause);

        boolean changed = stateMachine.setIf(QueryState.FAILED, state -> !state.isDone());
        if (changed) {
            logger.error("Query {} failed", queryId, cause);
            abortAllStages();
        }
        return changed;
    }

    /**
     * Aborts all registered stages that haven't reached a terminal state.
     */
    private void abortAllStages() {
        for (StageStateMachine stage : stages) {
            stage.transitionToAborted();
        }
    }

    @Override
    public String toString() {
        return "QueryStateMachine{queryId=" + queryId + ", state=" + getState() + ", stages=" + stages.size() + "}";
    }
}
