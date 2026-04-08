/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.scheduler;

import org.opensearch.lakehouse.distributed.stage.StageId;

/** Tracks the execution state and timing of a single stage. */
public final class StageExecution {
    /** The lifecycle state of a stage execution. */
    public enum State {
        /** Stage has been planned but not yet scheduled. */
        PLANNED,
        /** Stage is being scheduled on worker nodes. */
        SCHEDULING,
        /** Stage is actively executing on workers. */
        RUNNING,
        /** Stage completed successfully. */
        FINISHED,
        /** Stage failed with an error. */
        FAILED
    }

    private final StageId stageId;
    private volatile State state;
    private volatile long startTimeNanos;
    private volatile long endTimeNanos;
    private volatile Exception failureCause;

    /**
     * Creates a new StageExecution in the PLANNED state.
     *
     * @param stageId the stage identifier to track
     */
    public StageExecution(StageId stageId) {
        this.stageId = stageId;
        this.state = State.PLANNED;
    }

    /** Returns the stage identifier. */
    public StageId getStageId() { return stageId; }
    /** Returns the current execution state. */
    public State getState() { return state; }

    /**
     * Transitions this execution to the given state, updating timing as appropriate.
     *
     * @param newState the target state
     */
    public void transitionTo(State newState) {
        if (newState == State.RUNNING && state == State.PLANNED) {
            startTimeNanos = System.nanoTime();
        }
        if (newState == State.FINISHED || newState == State.FAILED) {
            endTimeNanos = System.nanoTime();
        }
        this.state = newState;
    }

    /**
     * Marks this execution as failed with the given cause.
     *
     * @param cause the exception that caused failure
     */
    public void fail(Exception cause) {
        this.failureCause = cause;
        transitionTo(State.FAILED);
    }

    /** Returns the elapsed time in milliseconds since the stage started running. */
    public long getElapsedMs() {
        long end = endTimeNanos > 0 ? endTimeNanos : System.nanoTime();
        return startTimeNanos > 0 ? (end - startTimeNanos) / 1_000_000 : 0;
    }

    /** Returns the exception that caused this stage to fail, or null if it has not failed. */
    public Exception getFailureCause() { return failureCause; }
    /** Returns true if this execution is in a terminal state (finished or failed). */
    public boolean isTerminal() { return state == State.FINISHED || state == State.FAILED; }
}
