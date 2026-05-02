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

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * State machine for a single stage in a distributed query.
 * <p>
 * Encodes valid transitions and provides typed methods for each transition.
 * Failure cause is captured before the state change so listeners can observe it.
 */
public class StageStateMachine {

    private static final Logger logger = LogManager.getLogger(StageStateMachine.class);

    private final int stageId;
    private final StateMachine<StageState> stateMachine;
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();

    public StageStateMachine(int stageId, Executor executor) {
        this.stageId = stageId;
        this.stateMachine = new StateMachine<>("stage-" + stageId, executor, StageState.PLANNED, StageState.TERMINAL_STATES);
    }

    public int getStageId() {
        return stageId;
    }

    public StageState getState() {
        return stateMachine.get();
    }

    public Throwable getFailureCause() {
        return failureCause.get();
    }

    public void addStateChangeListener(StateChangeListener<StageState> listener) {
        stateMachine.addStateChangeListener(listener);
    }

    /**
     * PLANNED → SCHEDULING
     */
    public boolean transitionToScheduling() {
        return stateMachine.compareAndSet(StageState.PLANNED, StageState.SCHEDULING);
    }

    /**
     * Any non-done → RUNNING
     */
    public boolean transitionToRunning() {
        return stateMachine.setIf(StageState.RUNNING, state -> state != StageState.RUNNING && !state.isDone());
    }

    /**
     * Any non-done → FINISHED
     */
    public boolean transitionToFinished() {
        return stateMachine.setIf(StageState.FINISHED, state -> !state.isDone());
    }

    /**
     * Any non-done → ABORTED (failure was in another stage)
     */
    public boolean transitionToAborted() {
        return stateMachine.setIf(StageState.ABORTED, state -> !state.isDone());
    }

    /**
     * Any non-done → FAILED (captures cause before transition)
     */
    public boolean transitionToFailed(Throwable cause) {
        requireNonNull(cause, "cause is null");
        failureCause.compareAndSet(null, cause);
        boolean changed = stateMachine.setIf(StageState.FAILED, state -> !state.isDone());
        if (changed) {
            logger.error("Stage {} failed", stageId, cause);
        }
        return changed;
    }

    @Override
    public String toString() {
        return "StageStateMachine{stageId=" + stageId + ", state=" + getState() + "}";
    }
}
