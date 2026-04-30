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

public class QueryStateMachineTests extends OpenSearchTestCase {

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

    private QueryStateMachine create() {
        return new QueryStateMachine("q-1", executor);
    }

    public void testInitialState() {
        QueryStateMachine qsm = create();
        assertEquals(QueryState.PLANNING, qsm.getState());
    }

    public void testHappyPath() {
        QueryStateMachine qsm = create();
        assertTrue(qsm.transitionToStarting());
        assertEquals(QueryState.STARTING, qsm.getState());

        assertTrue(qsm.transitionToRunning());
        assertEquals(QueryState.RUNNING, qsm.getState());

        assertTrue(qsm.transitionToFinishing());
        assertEquals(QueryState.FINISHING, qsm.getState());

        assertTrue(qsm.transitionToFinished());
        assertEquals(QueryState.FINISHED, qsm.getState());
    }

    public void testTransitionToFailed() {
        QueryStateMachine qsm = create();
        qsm.transitionToRunning();
        RuntimeException cause = new RuntimeException("query failed");
        assertTrue(qsm.transitionToFailed(cause));
        assertEquals(QueryState.FAILED, qsm.getState());
        assertSame(cause, qsm.getFailureCause());
    }

    public void testCannotTransitionFromTerminal() {
        QueryStateMachine qsm = create();
        qsm.transitionToFinished();
        assertFalse(qsm.transitionToRunning());
        assertFalse(qsm.transitionToFailed(new RuntimeException()));
        assertEquals(QueryState.FINISHED, qsm.getState());
    }

    public void testCannotGoBackwards() {
        QueryStateMachine qsm = create();
        qsm.transitionToRunning();
        assertFalse(qsm.transitionToStarting());
        assertEquals(QueryState.RUNNING, qsm.getState());
    }

    public void testStageFailureCascadesToQuery() throws Exception {
        QueryStateMachine qsm = create();
        StageStateMachine stage = new StageStateMachine(0, executor);
        qsm.addStage(stage);
        qsm.transitionToRunning();

        CountDownLatch latch = new CountDownLatch(1);
        qsm.addStateChangeListener(state -> {
            if (state == QueryState.FAILED) {
                latch.countDown();
            }
        });

        RuntimeException cause = new RuntimeException("stage 0 OOM");
        stage.transitionToFailed(cause);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(QueryState.FAILED, qsm.getState());
    }

    public void testQueryFailureAbortsAllStages() throws Exception {
        QueryStateMachine qsm = create();
        StageStateMachine stage0 = new StageStateMachine(0, executor);
        StageStateMachine stage1 = new StageStateMachine(1, executor);
        qsm.addStage(stage0);
        qsm.addStage(stage1);
        qsm.transitionToRunning();
        stage0.transitionToScheduling();
        stage0.transitionToRunning();
        stage1.transitionToScheduling();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<StageState> stage1State = new AtomicReference<>();
        stage1.addStateChangeListener(state -> {
            if (state == StageState.ABORTED) {
                stage1State.set(state);
                latch.countDown();
            }
        });

        // Fail the query directly
        qsm.transitionToFailed(new RuntimeException("timeout"));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(StageState.ABORTED, stage1State.get());
        // stage0 should also be aborted
        assertBusy(() -> assertEquals(StageState.ABORTED, stage0.getState()));
    }

    public void testMultipleStageFailuresFirstWins() throws Exception {
        QueryStateMachine qsm = create();
        StageStateMachine stage0 = new StageStateMachine(0, executor);
        StageStateMachine stage1 = new StageStateMachine(1, executor);
        qsm.addStage(stage0);
        qsm.addStage(stage1);
        qsm.transitionToRunning();

        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");

        stage0.transitionToFailed(first);
        stage1.transitionToFailed(second);

        // Wait for async cascade
        assertBusy(() -> assertEquals(QueryState.FAILED, qsm.getState()));
        assertSame(first, qsm.getFailureCause());
    }

    public void testListenerFires() throws Exception {
        QueryStateMachine qsm = create();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<QueryState> observed = new AtomicReference<>();

        qsm.addStateChangeListener(state -> {
            if (state == QueryState.RUNNING) {
                observed.set(state);
                latch.countDown();
            }
        });

        qsm.transitionToRunning();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(QueryState.RUNNING, observed.get());
    }
}
