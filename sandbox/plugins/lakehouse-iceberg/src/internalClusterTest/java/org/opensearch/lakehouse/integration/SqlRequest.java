/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request carrying SQL query text for integration tests.
 */
public class SqlRequest extends ActionRequest {

    private final String sql;

    public SqlRequest(String sql) {
        this.sql = sql;
    }

    public SqlRequest(StreamInput in) throws IOException {
        super(in);
        this.sql = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sql);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sql == null || sql.isEmpty()) {
            validationException = addValidationError("sql is missing or empty", validationException);
        }
        return validationException;
    }

    public String getSql() {
        return sql;
    }
}
