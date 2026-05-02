/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.execution;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StateMachineTests extends OpenSearchTestCase {

    private enum TestState {
        INITIAL,
        RUNNING,
        DONE,
        FAILED
    }

    private static final Set<TestState> TERMINAL = Set.of(TestState.DONE, TestState.FAILED);
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

    public void testInitialState() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        assertEquals(TestState.INITIAL, sm.get());
    }

    public void testSet() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        TestState old = sm.set(TestState.RUNNING);
        assertEquals(TestState.INITIAL, old);
        assertEquals(TestState.RUNNING, sm.get());
    }

    public void testSetToTerminal() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        sm.set(TestState.DONE);
        assertEquals(TestState.DONE, sm.get());
    }

    public void testSetFromTerminalThrows() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        sm.set(TestState.DONE);
        expectThrows(IllegalStateException.class, () -> sm.set(TestState.RUNNING));
    }

    public void testTrySetFromTerminalIsNoOp() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        sm.trySet(TestState.DONE);
        TestState old = sm.trySet(TestState.RUNNING);
        assertEquals(TestState.DONE, old);
        assertEquals(TestState.DONE, sm.get());
    }

    public void testTrySetSameStateIsNoOp() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        TestState old = sm.trySet(TestState.INITIAL);
        assertEquals(TestState.INITIAL, old);
    }

    public void testCompareAndSet() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        assertTrue(sm.compareAndSet(TestState.INITIAL, TestState.RUNNING));
        assertEquals(TestState.RUNNING, sm.get());
    }

    public void testCompareAndSetWrongExpected() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        assertFalse(sm.compareAndSet(TestState.RUNNING, TestState.DONE));
        assertEquals(TestState.INITIAL, sm.get());
    }

    public void testSetIf() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        assertTrue(sm.setIf(TestState.RUNNING, state -> state == TestState.INITIAL));
        assertEquals(TestState.RUNNING, sm.get());
    }

    public void testSetIfPredicateFails() {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        assertFalse(sm.setIf(TestState.RUNNING, state -> state == TestState.DONE));
        assertEquals(TestState.INITIAL, sm.get());
    }

    public void testListenerFires() throws Exception {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestState> observed = new AtomicReference<>();

        sm.addStateChangeListener(newState -> {
            // First call is immediate notification of current state; ignore it
            if (newState != TestState.INITIAL) {
                observed.set(newState);
                latch.countDown();
            }
        });

        sm.set(TestState.RUNNING);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(TestState.RUNNING, observed.get());
    }

    public void testListenerNotifiedOfCurrentStateOnRegistration() throws Exception {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.RUNNING, TERMINAL);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestState> observed = new AtomicReference<>();

        sm.addStateChangeListener(newState -> {
            observed.set(newState);
            latch.countDown();
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(TestState.RUNNING, observed.get());
    }

    public void testListenersClearedOnTerminal() throws Exception {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        CountDownLatch terminalLatch = new CountDownLatch(1);
        AtomicReference<TestState> lastSeen = new AtomicReference<>();

        sm.addStateChangeListener(newState -> {
            lastSeen.set(newState);
            if (TERMINAL.contains(newState)) {
                terminalLatch.countDown();
            }
        });

        sm.set(TestState.DONE);
        assertTrue(terminalLatch.await(5, TimeUnit.SECONDS));
        assertEquals(TestState.DONE, lastSeen.get());
    }

    public void testGetStateChangeFuture() throws Exception {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.INITIAL, TERMINAL);
        CompletableFuture<TestState> future = sm.getStateChange(TestState.INITIAL);
        assertFalse(future.isDone());

        sm.set(TestState.RUNNING);
        assertEquals(TestState.RUNNING, future.get(5, TimeUnit.SECONDS));
    }

    public void testGetStateChangeAlreadyChanged() throws Exception {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.RUNNING, TERMINAL);
        CompletableFuture<TestState> future = sm.getStateChange(TestState.INITIAL);
        assertTrue(future.isDone());
        assertEquals(TestState.RUNNING, future.get());
    }

    public void testGetStateChangeTerminal() throws Exception {
        StateMachine<TestState> sm = new StateMachine<>("test", executor, TestState.DONE, TERMINAL);
        CompletableFuture<TestState> future = sm.getStateChange(TestState.DONE);
        assertTrue(future.isDone());
        assertEquals(TestState.DONE, future.get());
    }

    public void testToString() {
        StateMachine<TestState> sm = new StateMachine<>("myMachine", executor, TestState.INITIAL, TERMINAL);
        assertEquals("myMachine=INITIAL", sm.toString());
    }
}
