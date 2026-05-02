/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 */

package org.opensearch.lakehouse.execution;

import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Lifecycle states for a task (one unit of work on one node).
 * <p>
 * Uses a three-phase termination model: a task enters a terminating
 * state (CANCELING/FAILING) first, allowing in-flight work to drain,
 * then moves to the final terminal state (CANCELED/FAILED).
 */
public enum TaskState {
    /** Task is planned but has not been scheduled yet. */
    PLANNED(false, false),
    /** Task is running. */
    RUNNING(false, false),
    /** Task has finished executing; output is being flushed. */
    FLUSHING(false, false),
    /** Task completed successfully. */
    FINISHED(true, false),
    /** Task is being canceled (draining in-flight work). */
    CANCELING(false, true),
    /** Task was canceled. */
    CANCELED(true, false),
    /** Task is being aborted due to failure in another stage. */
    ABORTING(false, true),
    /** Task was aborted due to a failure in another stage. */
    ABORTED(true, false),
    /** Task is failing (draining in-flight work). */
    FAILING(false, true),
    /** Task execution failed. */
    FAILED(true, false);

    public static final Set<TaskState> TERMINAL_STATES = Stream.of(values())
        .filter(TaskState::isDone)
        .collect(toUnmodifiableSet());

    private final boolean doneState;
    private final boolean terminating;

    TaskState(boolean doneState, boolean terminating) {
        this.doneState = doneState;
        this.terminating = terminating;
    }

    public boolean isDone() {
        return doneState;
    }

    public boolean isTerminating() {
        return terminating;
    }

    public boolean isTerminatingOrDone() {
        return terminating || doneState;
    }
}
