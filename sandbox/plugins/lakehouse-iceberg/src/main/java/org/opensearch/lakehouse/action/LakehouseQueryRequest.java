/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request carrying query text and language type for lakehouse endpoints.
 *
 * @opensearch.internal
 */
public class LakehouseQueryRequest extends ActionRequest {

    private final String queryText;
    private final boolean sql;

    /**
     * Creates a request with the given query text and language.
     *
     * @param queryText the query text
     * @param sql true for SQL, false for PPL
     */
    public LakehouseQueryRequest(String queryText, boolean sql) {
        this.queryText = queryText;
        this.sql = sql;
    }

    /**
     * Creates a request from a stream.
     *
     * @param in the stream input
     * @throws IOException if reading fails
     */
    public LakehouseQueryRequest(StreamInput in) throws IOException {
        super(in);
        this.queryText = in.readString();
        this.sql = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryText);
        out.writeBoolean(sql);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (queryText == null || queryText.isEmpty()) {
            validationException = addValidationError("query text is missing or empty", validationException);
        }
        return validationException;
    }

    /**
     * Returns the query text.
     *
     * @return the query text
     */
    public String getQueryText() {
        return queryText;
    }

    /**
     * Returns whether this is a SQL query.
     *
     * @return true for SQL, false for PPL
     */
    public boolean isSql() {
        return sql;
    }
}
