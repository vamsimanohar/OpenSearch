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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SQL dialect for Apache DataFusion.
 * DataFusion uses PostgreSQL-like SQL syntax with double-quoted identifiers.
 */
public class DataFusionSqlDialect extends PostgresqlSqlDialect {

    /** Default context for DataFusion: double-quoted identifiers, nulls sort last. */
    public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT.withDatabaseProduct(
        SqlDialect.DatabaseProduct.UNKNOWN
    ).withDatabaseProductName("DataFusion").withIdentifierQuoteString("\"").withNullCollation(NullCollation.LAST);

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
    public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
        unparseFetchUsingLimit(writer, offset, fetch);
    }

    /** Calcite function names that differ in DataFusion. */
    private static final Map<String, String> FUNCTION_RENAMES = Map.of("SIGN", "SIGNUM", "TRUNCATE", "TRUNC");

    /** Functions that DataFusion implements via date_part('unit', expr). */
    private static final Set<String> DATE_PART_FUNCTIONS = Set.of(
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "DAYOFWEEK",
        "DAY_OF_WEEK"
    );

    /** Functions that map to binary operators in DataFusion. */
    private static final Map<String, String> BINARY_OP_FUNCTIONS = Map.of("MOD", " % ", "DIVIDE", " / ");

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        // Calcite decomposes TIMESTAMPDIFF into CAST(/INT(Reinterpret(end - start), divisor) AS INTEGER).
        // Reinterpret has no SQL syntax — convert to epoch arithmetic for DataFusion.
        if (call.getKind() == SqlKind.REINTERPRET) {
            SqlNode operand = call.operand(0);
            if (operand instanceof SqlCall && ((SqlCall) operand).getKind() == SqlKind.MINUS) {
                SqlCall minus = (SqlCall) operand;
                writer.print("((date_part('epoch', ");
                minus.operand(0).unparse(writer, 0, 0);
                writer.print(") - date_part('epoch', ");
                minus.operand(1).unparse(writer, 0, 0);
                writer.print(")) * 1000)");
            } else {
                writer.print("(date_part('epoch', ");
                operand.unparse(writer, 0, 0);
                writer.print(") * 1000)");
            }
            return;
        }

        String name = call.getOperator().getName().toUpperCase(Locale.ROOT);

        // Calcite's /INT (integer division) → regular / for DataFusion
        if ("/INT".equals(name) && call.operandCount() == 2) {
            call.operand(0).unparse(writer, leftPrec, rightPrec);
            writer.print(" / ");
            call.operand(1).unparse(writer, leftPrec, rightPrec);
            return;
        }

        // MOD(a,b) → a % b, DIVIDE(a,b) → a / b
        String binOp = BINARY_OP_FUNCTIONS.get(name);
        if (binOp != null && call.operandCount() == 2) {
            call.operand(0).unparse(writer, leftPrec, rightPrec);
            writer.print(binOp);
            call.operand(1).unparse(writer, leftPrec, rightPrec);
            return;
        }

        // YEAR(x) → date_part('year', x)
        if (DATE_PART_FUNCTIONS.contains(name) && call.operandCount() == 1) {
            String part = ("DAYOFWEEK".equals(name) || "DAY_OF_WEEK".equals(name)) ? "dow" : name.toLowerCase(Locale.ROOT);
            writer.print("date_part('" + part + "', ");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }

        // DATE(x) → CAST(x AS DATE)
        if ("DATE".equals(name) && call.operandCount() == 1) {
            writer.print("CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS DATE)");
            return;
        }

        // Simple renames: SIGN→SIGNUM, TRUNCATE→TRUNC
        String renamed = FUNCTION_RENAMES.get(name);
        if (renamed != null) {
            writer.print(renamed);
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (int i = 0; i < call.operandCount(); i++) {
                writer.sep(",");
                call.operand(i).unparse(writer, 0, 0);
            }
            writer.endList(frame);
            return;
        }

        super.unparseCall(writer, call, leftPrec, rightPrec);
    }
}
