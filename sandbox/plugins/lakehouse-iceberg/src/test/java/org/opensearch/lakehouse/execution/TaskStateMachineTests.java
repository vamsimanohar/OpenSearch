/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.execution;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TaskStateMachineTests extends OpenSearchTestCase {

    private ExecutorService executor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = Executors.newSingleThreadExecutor();
    }

    @Override
    public void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        super.tearDown();
    }

    private TaskStateMachine create() {
        return new TaskStateMachine("task-0", executor);
    }

    public void testInitialState() {
        TaskStateMachine sm = create();
        assertEquals(TaskState.RUNNING, sm.getState());
    }

    public void testRunningToFlushing() {
        TaskStateMachine sm = create();
        assertTrue(sm.transitionToFlushing());
        assertEquals(TaskState.FLUSHING, sm.getState());
    }

    public void testRunningToFinished() {
        TaskStateMachine sm = create();
        assertTrue(sm.finished());
        assertEquals(TaskState.FINISHED, sm.getState());
    }

    public void testRunningToCanceling() {
        TaskStateMachine sm = create();
        assertTrue(sm.cancel());
        assertEquals(TaskState.CANCELING, sm.getState());
    }

    public void testCancelingToCancel() {
        TaskStateMachine sm = create();
        sm.cancel();
        sm.terminationComplete();
        assertEquals(TaskState.CANCELED, sm.getState());
    }

    public void testRunningToFailing() {
        TaskStateMachine sm = create();
        RuntimeException cause = new RuntimeException("oom");
        assertTrue(sm.fail(cause));
        assertEquals(TaskState.FAILING, sm.getState());
        assertSame(cause, sm.getFailureCause());
    }

    public void testFailingToFailed() {
        TaskStateMachine sm = create();
        sm.fail(new RuntimeException("oom"));
        sm.terminationComplete();
        assertEquals(TaskState.FAILED, sm.getState());
    }

    public void testCannotCancelFromTerminating() {
        TaskStateMachine sm = create();
        sm.fail(new RuntimeException());
        assertFalse(sm.cancel());
        assertEquals(TaskState.FAILING, sm.getState());
    }

    public void testCannotFailFromTerminating() {
        TaskStateMachine sm = create();
        sm.cancel();
        assertFalse(sm.fail(new RuntimeException()));
        assertEquals(TaskState.CANCELING, sm.getState());
    }

    public void testCannotFinishFromTerminating() {
        TaskStateMachine sm = create();
        sm.cancel();
        assertFalse(sm.finished());
        assertEquals(TaskState.CANCELING, sm.getState());
    }

    public void testCannotTransitionFromTerminal() {
        TaskStateMachine sm = create();
        sm.finished();
        assertFalse(sm.cancel());
        assertFalse(sm.fail(new RuntimeException()));
        assertFalse(sm.transitionToFlushing());
        assertEquals(TaskState.FINISHED, sm.getState());
    }

    public void testTerminationCompleteOnNonTerminatingIsNoOp() {
        TaskStateMachine sm = create();
        sm.terminationComplete();
        assertEquals(TaskState.RUNNING, sm.getState());
    }

    public void testFailureCauseCapturedOnce() {
        TaskStateMachine sm = create();
        RuntimeException first = new RuntimeException("first");
        sm.fail(first);
        // second fail on already-terminating is no-op
        sm.fail(new RuntimeException("second"));
        assertSame(first, sm.getFailureCause());
    }

    public void testThreePhaseTermination() {
        // Full lifecycle: RUNNING → FAILING → FAILED
        TaskStateMachine sm = create();
        assertEquals(TaskState.RUNNING, sm.getState());
        assertFalse(sm.getState().isTerminatingOrDone());

        sm.fail(new RuntimeException("error"));
        assertEquals(TaskState.FAILING, sm.getState());
        assertTrue(sm.getState().isTerminating());
        assertTrue(sm.getState().isTerminatingOrDone());
        assertFalse(sm.getState().isDone());

        sm.terminationComplete();
        assertEquals(TaskState.FAILED, sm.getState());
        assertTrue(sm.getState().isDone());
        assertTrue(sm.getState().isTerminatingOrDone());
        assertFalse(sm.getState().isTerminating());
    }
}
