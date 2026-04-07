/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

/**
 * SQL dialect for Apache DataFusion.
 * DataFusion uses PostgreSQL-like SQL syntax with double-quoted identifiers.
 */
public class DataFusionSqlDialect extends PostgresqlSqlDialect {

    /** Default context for DataFusion: double-quoted identifiers, nulls sort last. */
    public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(SqlDialect.DatabaseProduct.UNKNOWN)
        .withDatabaseProductName("DataFusion")
        .withIdentifierQuoteString("\"")
        .withNullCollation(NullCollation.LAST);

    /** Singleton instance with default configuration. */
    public static final DataFusionSqlDialect DEFAULT = new DataFusionSqlDialect(DEFAULT_CONTEXT);

    /**
     * Creates a DataFusion dialect with the given context.
     *
     * @param context the dialect configuration context
     */
    public DataFusionSqlDialect(Context context) {
        super(context);
    }

    @Override
    public boolean supportsWindowFunctions() {
        return true;
    }

    @Override
    public boolean supportsAggregateFunctionFilter() {
        return false;
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
    }
}
