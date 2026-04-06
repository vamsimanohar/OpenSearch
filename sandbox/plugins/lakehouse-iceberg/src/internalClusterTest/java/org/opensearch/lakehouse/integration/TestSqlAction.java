/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import org.opensearch.action.ActionType;

/**
 * Action type for test SQL execution via transport client.
 */
public class TestSqlAction extends ActionType<SqlResponse> {

    public static final String NAME = "cluster:admin/lakehouse/test/sql";
    public static final TestSqlAction INSTANCE = new TestSqlAction();

    private TestSqlAction() {
        super(NAME, SqlResponse::new);
    }
}
