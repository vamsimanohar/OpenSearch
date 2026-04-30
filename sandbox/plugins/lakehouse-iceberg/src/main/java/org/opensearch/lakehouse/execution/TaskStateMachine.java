/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Derived from Trino (io.trino.execution.TaskStateMachine).
 * Adapted for OpenSearch lakehouse distributed query engine.
 */

package org.opensearch.lakehouse.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * State machine for a single task (one unit of work on one node).
 * <p>
 * Uses three-phase termination: a task enters a terminating state
 * (CANCELING/FAILING) first, then moves to the terminal state
 * (CANCELED/FAILED) when all in-flight work has drained.
 */
public class TaskStateMachine {

    private static final Logger logger = LogManager.getLogger(TaskStateMachine.class);

    private final String taskId;
    private final StateMachine<TaskState> stateMachine;
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();

    public TaskStateMachine(String taskId, Executor executor) {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.stateMachine = new StateMachine<>("task-" + taskId, executor, TaskState.RUNNING, TaskState.TERMINAL_STATES);
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskState getState() {
        return stateMachine.get();
    }

    public Throwable getFailureCause() {
        return failureCause.get();
    }

    public void addStateChangeListener(StateChangeListener<TaskState> listener) {
        stateMachine.addStateChangeListener(listener);
    }

    /**
     * RUNNING → FLUSHING (output is being drained)
     */
    public boolean transitionToFlushing() {
        return stateMachine.compareAndSet(TaskState.RUNNING, TaskState.FLUSHING);
    }

    /**
     * Non-terminating → FINISHED
     */
    public boolean finished() {
        return stateMachine.setIf(TaskState.FINISHED, state -> !state.isTerminatingOrDone());
    }

    /**
     * Non-terminating → CANCELING (begin graceful cancel)
     */
    public boolean cancel() {
        return stateMachine.setIf(TaskState.CANCELING, state -> !state.isTerminatingOrDone());
    }

    /**
     * Non-terminating → FAILING (begin graceful failure, captures cause)
     */
    public boolean fail(Throwable cause) {
        requireNonNull(cause, "cause is null");
        failureCause.compareAndSet(null, cause);
        boolean changed = stateMachine.setIf(TaskState.FAILING, state -> !state.isTerminatingOrDone());
        if (changed) {
            logger.error("Task {} failed", taskId, cause);
        }
        return changed;
    }

    /**
     * Completes the termination phase:
     * CANCELING → CANCELED, FAILING → FAILED
     * <p>
     * Called when all in-flight work has drained.
     */
    public void terminationComplete() {
        TaskState current = stateMachine.get();
        switch (current) {
            case CANCELING -> stateMachine.compareAndSet(TaskState.CANCELING, TaskState.CANCELED);
            case FAILING -> stateMachine.compareAndSet(TaskState.FAILING, TaskState.FAILED);
            default -> {
                // already terminal or not terminating — no-op
            }
        }
    }

    @Override
    public String toString() {
        return "TaskStateMachine{taskId=" + taskId + ", state=" + getState() + "}";
    }
}
