/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 * with JDK CompletableFuture, replaced Airlift Logger with Log4j.
 */

package org.opensearch.lakehouse.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Thread-safe state machine with async listener notification.
 * <p>
 * Generic thread-safe state container with async listener notification.
 * Terminal states are absorbing — once reached, no further transitions are allowed.
 * Listeners fire asynchronously on a dedicated executor and are cleared on terminal state.
 *
 * @param <T> the state type
 */
public class StateMachine<T> {

    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    private final String name;
    private final Executor executor;
    private final Object lock = new Object();
    private final Set<T> terminalStates;

    private volatile T state;
    private final List<StateChangeListener<T>> stateChangeListeners = new ArrayList<>();
    private final List<CompletableFuture<T>> waiters = new ArrayList<>();

    public StateMachine(String name, Executor executor, T initialState, Set<T> terminalStates) {
        this.name = requireNonNull(name, "name is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.state = requireNonNull(initialState, "initialState is null");
        this.terminalStates = Set.copyOf(requireNonNull(terminalStates, "terminalStates is null"));
    }

    public T get() {
        return state;
    }

    /**
     * Sets the state. Throws if transitioning from a terminal state.
     *
     * @return the old state
     */
    public T set(T newState) {
        T oldState = trySet(newState);
        if (!oldState.equals(newState) && isTerminalState(oldState)) {
            throw new IllegalStateException(name + " cannot transition from " + oldState + " to " + newState);
        }
        return oldState;
    }

    /**
     * Tries to set the state. Silently ignores if already terminal or same state.
     *
     * @return the state before the possible change
     */
    public T trySet(T newState) {
        requireNonNull(newState, "newState is null");

        T oldState;
        List<StateChangeListener<T>> listeners;
        List<CompletableFuture<T>> futures;

        synchronized (lock) {
            if (state.equals(newState) || isTerminalState(state)) {
                return state;
            }

            oldState = state;
            state = newState;

            listeners = List.copyOf(stateChangeListeners);
            futures = List.copyOf(waiters);
            waiters.clear();

            if (isTerminalState(state)) {
                stateChangeListeners.clear();
            }
        }

        fireStateChanged(newState, listeners, futures);
        return oldState;
    }

    /**
     * Sets the state if the current state satisfies the predicate.
     *
     * @return true if the state was changed
     */
    public boolean setIf(T newState, Predicate<T> predicate) {
        requireNonNull(newState, "newState is null");

        while (true) {
            T currentState = get();
            if (currentState.equals(newState)) {
                return false;
            }
            if (!predicate.test(currentState)) {
                return false;
            }
            if (compareAndSet(currentState, newState)) {
                return true;
            }
        }
    }

    /**
     * Sets the state if the current state equals the expected state.
     *
     * @return true if the state was changed
     */
    public boolean compareAndSet(T expectedState, T newState) {
        requireNonNull(expectedState, "expectedState is null");
        requireNonNull(newState, "newState is null");

        List<StateChangeListener<T>> listeners;
        List<CompletableFuture<T>> futures;

        synchronized (lock) {
            if (!state.equals(expectedState) || state.equals(newState)) {
                return false;
            }
            if (isTerminalState(state)) {
                throw new IllegalStateException(name + " cannot transition from terminal state " + state + " to " + newState);
            }

            state = newState;

            listeners = List.copyOf(stateChangeListeners);
            futures = List.copyOf(waiters);
            waiters.clear();

            if (isTerminalState(state)) {
                stateChangeListeners.clear();
            }
        }

        fireStateChanged(newState, listeners, futures);
        return true;
    }

    /**
     * Returns a future that completes when the state changes from the given current state.
     */
    public CompletableFuture<T> getStateChange(T currentState) {
        synchronized (lock) {
            if (!state.equals(currentState) || isTerminalState(state)) {
                return CompletableFuture.completedFuture(state);
            }
            CompletableFuture<T> future = new CompletableFuture<>();
            waiters.add(future);
            return future;
        }
    }

    /**
     * Adds a listener. Immediately notified of the current state upon registration.
     */
    public void addStateChangeListener(StateChangeListener<T> listener) {
        requireNonNull(listener, "listener is null");

        T currentState;
        synchronized (lock) {
            currentState = state;
            if (!isTerminalState(currentState)) {
                stateChangeListeners.add(listener);
            }
        }

        // always notify of current state asynchronously
        safeExecute(() -> fireListener(currentState, listener));
    }

    private void fireStateChanged(T newState, List<StateChangeListener<T>> listeners, List<CompletableFuture<T>> futures) {
        safeExecute(() -> {
            for (CompletableFuture<T> future : futures) {
                try {
                    future.complete(newState);
                } catch (Throwable e) {
                    logger.error("Error completing state change future for {}", name, e);
                }
            }
            for (StateChangeListener<T> listener : listeners) {
                fireListener(newState, listener);
            }
        });
    }

    private void fireListener(T newState, StateChangeListener<T> listener) {
        try {
            listener.stateChanged(newState);
        } catch (Throwable e) {
            logger.error("Error notifying state change listener for {}", name, e);
        }
    }

    private boolean isTerminalState(T state) {
        return terminalStates.contains(state);
    }

    private void safeExecute(Runnable command) {
        try {
            executor.execute(command);
        } catch (Exception e) {
            logger.error("Failed to execute state change notification for {}", name, e);
        }
    }

    @Override
    public String toString() {
        return name + "=" + get();
    }
}
