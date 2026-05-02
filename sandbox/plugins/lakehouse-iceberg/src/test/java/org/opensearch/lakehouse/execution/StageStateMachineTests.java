/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.execution;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StageStateMachineTests extends OpenSearchTestCase {

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

    private StageStateMachine create() {
        return new StageStateMachine(0, executor);
    }

    public void testInitialState() {
        StageStateMachine sm = create();
        assertEquals(StageState.PLANNED, sm.getState());
    }

    public void testPlannedToScheduling() {
        StageStateMachine sm = create();
        assertTrue(sm.transitionToScheduling());
        assertEquals(StageState.SCHEDULING, sm.getState());
    }

    public void testSchedulingToRunning() {
        StageStateMachine sm = create();
        sm.transitionToScheduling();
        assertTrue(sm.transitionToRunning());
        assertEquals(StageState.RUNNING, sm.getState());
    }

    public void testRunningToFinished() {
        StageStateMachine sm = create();
        sm.transitionToScheduling();
        sm.transitionToRunning();
        assertTrue(sm.transitionToFinished());
        assertEquals(StageState.FINISHED, sm.getState());
    }

    public void testTransitionToFailed() {
        StageStateMachine sm = create();
        sm.transitionToScheduling();
        sm.transitionToRunning();
        RuntimeException cause = new RuntimeException("boom");
        assertTrue(sm.transitionToFailed(cause));
        assertEquals(StageState.FAILED, sm.getState());
        assertSame(cause, sm.getFailureCause());
    }

    public void testTransitionToAborted() {
        StageStateMachine sm = create();
        sm.transitionToScheduling();
        assertTrue(sm.transitionToAborted());
        assertEquals(StageState.ABORTED, sm.getState());
    }

    public void testCannotTransitionFromTerminal() {
        StageStateMachine sm = create();
        sm.transitionToFinished();
        assertFalse(sm.transitionToRunning());
        assertFalse(sm.transitionToFailed(new RuntimeException()));
        assertEquals(StageState.FINISHED, sm.getState());
    }

    public void testFailureCauseCapturedOnce() {
        StageStateMachine sm = create();
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        sm.transitionToFailed(first);
        // second failure on already-terminal state is no-op
        sm.transitionToFailed(second);
        assertSame(first, sm.getFailureCause());
    }

    public void testListenerFires() throws Exception {
        StageStateMachine sm = create();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<StageState> observed = new AtomicReference<>();

        sm.addStateChangeListener(state -> {
            if (state == StageState.FINISHED) {
                observed.set(state);
                latch.countDown();
            }
        });

        sm.transitionToFinished();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(StageState.FINISHED, observed.get());
    }

    public void testSchedulingFromNonPlannedFails() {
        StageStateMachine sm = create();
        sm.transitionToScheduling();
        sm.transitionToRunning();
        assertFalse(sm.transitionToScheduling());
        assertEquals(StageState.RUNNING, sm.getState());
    }
}
