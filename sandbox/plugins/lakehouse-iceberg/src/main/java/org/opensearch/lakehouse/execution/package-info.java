/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Distributed query execution lifecycle — state machines and event-driven
 * stage orchestration. Ported from Trino's execution framework and adapted
 * for OpenSearch.
 *
 * @see org.opensearch.lakehouse.execution.StateMachine
 * @see org.opensearch.lakehouse.execution.QueryStateMachine
 */
package org.opensearch.lakehouse.execution;
