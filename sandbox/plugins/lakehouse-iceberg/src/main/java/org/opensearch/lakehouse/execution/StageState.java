/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Derived from Trino (io.trino.execution.StageState).
 */

package org.opensearch.lakehouse.execution;

import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Lifecycle states for a stage in a distributed query.
 */
public enum StageState {
    /** Stage is planned but not yet scheduled. */
    PLANNED(false, false),
    /** Stage tasks are being scheduled on nodes. */
    SCHEDULING(false, false),
    /** Stage has at least one running task. */
    RUNNING(false, false),
    /** Stage has finished executing and all output consumed. */
    FINISHED(true, false),
    /** Stage was aborted due to a failure in another stage. */
    ABORTED(true, true),
    /** Stage execution failed. */
    FAILED(true, true);

    public static final Set<StageState> TERMINAL_STATES = Stream.of(values())
        .filter(StageState::isDone)
        .collect(toUnmodifiableSet());

    private final boolean doneState;
    private final boolean failureState;

    StageState(boolean doneState, boolean failureState) {
        this.doneState = doneState;
        this.failureState = failureState;
    }

    public boolean isDone() {
        return doneState;
    }

    public boolean isFailure() {
        return failureState;
    }
}
