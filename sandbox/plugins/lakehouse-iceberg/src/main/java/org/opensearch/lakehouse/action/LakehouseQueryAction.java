/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.action;

import org.opensearch.action.ActionType;
import org.opensearch.ppl.action.PPLResponse;

/**
 * Action singleton for lakehouse SQL/PPL query execution.
 *
 * @opensearch.internal
 */
public class LakehouseQueryAction extends ActionType<PPLResponse> {
    /** The action name. */
    public static final String NAME = "cluster:internal/lakehouse/query";
    /** The singleton instance. */
    public static final LakehouseQueryAction INSTANCE = new LakehouseQueryAction();

    private LakehouseQueryAction() {
        super(NAME, PPLResponse::new);
    }
}
