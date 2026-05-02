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
 * Lifecycle states for a distributed query.
 */
public enum QueryState {
    /** Query has been accepted and is awaiting execution. */
    QUEUED(false),
    /** Query is waiting for required resources. */
    WAITING_FOR_RESOURCES(false),
    /** Query is being dispatched to a coordinator. */
    DISPATCHING(false),
    /** Query is being planned (SQL → SubPlan). */
    PLANNING(false),
    /** Query execution is being started (dispatching leaf stages). */
    STARTING(false),
    /** Query has at least one running stage. */
    RUNNING(false),
    /** Query is finishing (final gather, cleanup). */
    FINISHING(false),
    /** Query completed successfully. */
    FINISHED(true),
    /** Query execution failed. */
    FAILED(true);

    public static final Set<QueryState> TERMINAL_STATES = Stream.of(values())
        .filter(QueryState::isDone)
        .collect(toUnmodifiableSet());

    private final boolean doneState;

    QueryState(boolean doneState) {
        this.doneState = doneState;
    }

    public boolean isDone() {
        return doneState;
    }
}
