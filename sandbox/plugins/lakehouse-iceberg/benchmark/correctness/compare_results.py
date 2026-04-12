#!/usr/bin/env python3
"""
Compare query results from OpenSearch lakehouse plugin and datafusion-cli.

Parses both output formats, normalizes values, and reports detailed diffs.

Usage:
    python3 compare_results.py <os_response_file> <df_response_file> [--max-rows N] [--ordered]
    python3 compare_results.py --os-dir <dir> --df-dir <dir> [--max-rows N] [--summary]
"""

import argparse
import json
import math
import os
import re
import sys
from datetime import date, datetime, timedelta


# ── Epoch-day / date conversion ──────────────────────────────────────────────

EPOCH_DATE = date(1970, 1, 1)


def epoch_days_to_date_str(epoch_days):
    """Convert epoch-day integer to YYYY-MM-DD string."""
    try:
        d = EPOCH_DATE + timedelta(days=int(epoch_days))
        return d.isoformat()
    except (ValueError, OverflowError):
        return None


def date_str_to_epoch_days(s):
    """Convert YYYY-MM-DD string to epoch-day integer, or None."""
    try:
        d = datetime.strptime(s.strip(), "%Y-%m-%d").date()
        return (d - EPOCH_DATE).days
    except (ValueError, TypeError):
        return None


# ── Parsing ──────────────────────────────────────────────────────────────────

def parse_os_response(filepath):
    """Parse OpenSearch JSON response -> (column_names, rows).

    Response format:
      {"schema": [{"name":"col","alias":"..."}], "datarows": [[v1, v2, ...], ...],
       "total": N, "status": 200}

    Returns (column_names: list[str], rows: list[list]) or (None, None) on error.
    """
    try:
        with open(filepath) as f:
            content = f.read().strip()
            if not content:
                return None, None, "empty response file"
            data = json.loads(content)
    except (json.JSONDecodeError, OSError) as e:
        return None, None, f"parse error: {e}"

    if "error" in data:
        err = data["error"]
        if isinstance(err, dict):
            reason = err.get("reason", str(err))[:200]
        else:
            reason = str(err)[:200]
        return None, None, f"query error: {reason}"

    schema = data.get("schema", [])
    col_names = []
    for i, s in enumerate(schema):
        name = s.get("alias") or s.get("name") or f"col{i}"
        col_names.append(name)

    rows = data.get("datarows", data.get("rows", []))
    return col_names, rows, None


def parse_df_response(filepath):
    """Parse datafusion-cli pipe-delimited table output -> (column_names, rows).

    The output may contain multiple tables (from SET, CREATE TABLE, then the
    actual query). We always use the LAST table block.

    Returns (column_names: list[str], rows: list[list[str]]) or (None, None) on error.
    """
    try:
        with open(filepath) as f:
            text = f.read()
    except OSError as e:
        return None, None, f"read error: {e}"

    if not text.strip():
        return None, None, "empty response file"

    # Check for errors
    if re.search(r'(?i)error|panic|out of memory', text) and '|' not in text:
        first_line = text.strip().split('\n')[0][:200]
        return None, None, f"query error: {first_line}"

    lines = text.strip().split("\n")

    # Find all table blocks: +---+ header +---+ data* +---+
    blocks = []
    current_block = []
    in_block = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("+-") and stripped.endswith("-+"):
            if not in_block:
                in_block = True
                current_block = [stripped]
            else:
                current_block.append(stripped)
        elif stripped.startswith("|") and stripped.endswith("|") and in_block:
            current_block.append(stripped)
        else:
            if in_block and current_block:
                sep_count = sum(1 for l in current_block if l.startswith("+-"))
                pipe_count = sum(1 for l in current_block if l.startswith("|"))
                if sep_count >= 2 and pipe_count >= 1:
                    blocks.append(current_block)
                current_block = []
                in_block = False

    # Don't forget the last block
    if in_block and current_block:
        sep_count = sum(1 for l in current_block if l.startswith("+-"))
        pipe_count = sum(1 for l in current_block if l.startswith("|"))
        if sep_count >= 2 and pipe_count >= 1:
            blocks.append(current_block)

    if not blocks:
        return None, None, "no table found in output"

    # Use the LAST table block (skip SET/CREATE TABLE results)
    block = blocks[-1]

    pipe_lines = [l for l in block if l.startswith("|")]
    if not pipe_lines:
        return None, None, "no data rows in table"

    header = [c.strip() for c in pipe_lines[0].split("|")[1:-1]]
    rows = []
    for line in pipe_lines[1:]:
        vals = [c.strip() for c in line.split("|")[1:-1]]
        rows.append(vals)

    return header, rows, None


# ── Normalization ────────────────────────────────────────────────────────────

def normalize_value(val):
    """Normalize a single value for comparison.

    Handles:
    - NULL / None / empty -> "NULL"
    - Numeric: floats rounded to 6 significant figures
    - Booleans: true/false -> 1/0
    - Dates: epoch-day integers <-> YYYY-MM-DD strings
    - Timestamps: normalize common formats
    - Strings: strip whitespace
    """
    if val is None:
        return "NULL"

    s = str(val).strip()

    # Null variants
    if s in ("", "NULL", "null", "None", "(empty)"):
        return "NULL"

    # Boolean normalization
    if s.lower() == "true":
        return "1"
    if s.lower() == "false":
        return "0"

    # Try numeric normalization
    try:
        f = float(s)
        if math.isnan(f):
            return "NaN"
        if math.isinf(f):
            return "Inf" if f > 0 else "-Inf"
        # Integer check: if it looks like an int and has no decimal point
        if f == int(f) and "." not in s and "e" not in s.lower():
            return str(int(f))
        # Float: round to 6 significant figures
        return f"{f:.6g}"
    except (ValueError, OverflowError):
        pass

    # Date normalization: try YYYY-MM-DD
    date_match = re.match(r'^\d{4}-\d{2}-\d{2}$', s)
    if date_match:
        epoch = date_str_to_epoch_days(s)
        if epoch is not None:
            # Canonical: always use date string
            return s

    # Timestamp normalization: strip trailing zeros, normalize T separator
    ts_match = re.match(r'^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d+)?', s)
    if ts_match:
        date_part = ts_match.group(1)
        time_part = ts_match.group(2)
        frac = ts_match.group(3) or ""
        if frac:
            frac = frac.rstrip("0").rstrip(".")
        return f"{date_part}T{time_part}{frac}"

    return s


def normalize_date_aware(os_val, df_val):
    """Normalize a pair of values, handling the epoch-day vs date-string case.

    Returns (normalized_os, normalized_df).
    """
    os_norm = normalize_value(os_val)
    df_norm = normalize_value(df_val)

    if os_norm == df_norm:
        return os_norm, df_norm

    # Check if one is epoch-days and the other is a date string
    # Case 1: OS returns epoch days, DF returns date string
    try:
        os_int = int(str(os_val).strip())
        if 0 <= os_int <= 100000:  # reasonable epoch-day range
            date_str = epoch_days_to_date_str(os_int)
            if date_str and date_str == df_norm:
                return date_str, df_norm
    except (ValueError, TypeError):
        pass

    # Case 2: DF returns epoch days, OS returns date string
    try:
        df_int = int(str(df_val).strip())
        if 0 <= df_int <= 100000:
            date_str = epoch_days_to_date_str(df_int)
            if date_str and date_str == os_norm:
                return os_norm, date_str
    except (ValueError, TypeError):
        pass

    return os_norm, df_norm


# ── Comparison ───────────────────────────────────────────────────────────────

class ComparisonResult:
    """Result of comparing two query outputs."""

    def __init__(self, query_id):
        self.query_id = query_id
        self.status = "UNKNOWN"  # PASS, FAIL, SKIP, ERROR
        self.os_error = None
        self.df_error = None
        self.os_row_count = 0
        self.df_row_count = 0
        self.os_col_count = 0
        self.df_col_count = 0
        self.col_name_diffs = []
        self.value_mismatches = []
        self.row_count_match = True
        self.message = ""

    def summary_line(self):
        if self.status == "PASS":
            return f"Q{self.query_id}: PASS (rows={self.os_row_count})"
        elif self.status == "SKIP":
            return f"Q{self.query_id}: SKIP -- {self.message}"
        elif self.status == "ERROR":
            return f"Q{self.query_id}: ERROR -- {self.message}"
        else:
            parts = [f"Q{self.query_id}: FAIL"]
            if not self.row_count_match:
                parts.append(f"row_count: OS={self.os_row_count} DF={self.df_row_count}")
            if self.col_name_diffs:
                parts.append(f"col_diffs: {', '.join(self.col_name_diffs[:3])}")
            if self.value_mismatches:
                parts.append(f"value_mismatches: {len(self.value_mismatches)}")
                parts.append(f"first: {self.value_mismatches[0]}")
            return " -- ".join(parts)

    def detail_report(self):
        lines = [f"=== Q{self.query_id}: {self.status} ==="]
        if self.os_error:
            lines.append(f"  OS error: {self.os_error}")
        if self.df_error:
            lines.append(f"  DF error: {self.df_error}")
        lines.append(f"  OS rows: {self.os_row_count}, DF rows: {self.df_row_count}")
        lines.append(f"  OS cols: {self.os_col_count}, DF cols: {self.df_col_count}")
        if self.col_name_diffs:
            lines.append(f"  Column name differences:")
            for d in self.col_name_diffs:
                lines.append(f"    {d}")
        if self.value_mismatches:
            lines.append(f"  Value mismatches ({len(self.value_mismatches)} total):")
            for m in self.value_mismatches[:20]:
                lines.append(f"    {m}")
            if len(self.value_mismatches) > 20:
                lines.append(f"    ... and {len(self.value_mismatches) - 20} more")
        return "\n".join(lines)


def rows_to_sort_key(row):
    """Create a sort key from a row for order-insensitive comparison."""
    return tuple(normalize_value(v) for v in row)


def compare_query(query_id, os_file, df_file, max_rows=100, ordered=True):
    """Compare results of a single query from both engines.

    Args:
        query_id: Query identifier (e.g., 1..43)
        os_file: Path to OpenSearch JSON response
        df_file: Path to DataFusion CLI text output
        max_rows: Maximum number of rows to compare
        ordered: Whether to compare in order (True for ORDER BY queries)

    Returns:
        ComparisonResult
    """
    result = ComparisonResult(query_id)

    # Parse both responses
    os_cols, os_rows, os_err = parse_os_response(os_file)
    df_cols, df_rows, df_err = parse_df_response(df_file)

    result.os_error = os_err
    result.df_error = df_err

    # Handle missing/error cases
    if os_err and df_err:
        result.status = "SKIP"
        result.message = f"both errored: OS={os_err[:80]}, DF={df_err[:80]}"
        return result
    if os_err:
        result.status = "ERROR"
        result.message = f"OS error: {os_err}"
        return result
    if df_err:
        result.status = "ERROR"
        result.message = f"DF error: {df_err}"
        return result

    result.os_row_count = len(os_rows)
    result.df_row_count = len(df_rows)
    result.os_col_count = len(os_cols) if os_cols else 0
    result.df_col_count = len(df_cols) if df_cols else 0

    # Column count check
    if result.os_col_count != result.df_col_count:
        result.status = "FAIL"
        result.message = f"column count mismatch: OS={result.os_col_count}, DF={result.df_col_count}"
        return result

    # Column name comparison (case-insensitive, ignoring quotes)
    if os_cols and df_cols:
        for i, (oc, dc) in enumerate(zip(os_cols, df_cols)):
            oc_clean = oc.lower().strip('"').strip('`')
            dc_clean = dc.lower().strip('"').strip('`')
            if oc_clean != dc_clean:
                result.col_name_diffs.append(f"col{i}: OS='{oc}' vs DF='{dc}'")

    # Row count check
    if result.os_row_count != result.df_row_count:
        result.row_count_match = False

    # Limit rows for comparison
    compare_n = min(len(os_rows), len(df_rows), max_rows)

    if compare_n == 0:
        if result.os_row_count == 0 and result.df_row_count == 0:
            result.status = "PASS"
            return result
        result.status = "FAIL"
        result.message = f"row count: OS={result.os_row_count}, DF={result.df_row_count}"
        return result

    # Prepare row lists for comparison
    os_compare = os_rows[:compare_n]
    df_compare = df_rows[:compare_n]

    # Sort if unordered comparison requested
    if not ordered:
        os_compare = sorted(os_compare, key=rows_to_sort_key)
        df_compare = sorted(df_compare, key=rows_to_sort_key)

    # Value-by-value comparison
    for i in range(compare_n):
        os_row = os_compare[i]
        df_row = df_compare[i]

        # Normalize to lists
        if isinstance(os_row, dict):
            os_vals = list(os_row.values())
        else:
            os_vals = list(os_row)

        if isinstance(df_row, dict):
            df_vals = list(df_row.values())
        else:
            df_vals = list(df_row)

        for j in range(min(len(os_vals), len(df_vals))):
            os_norm, df_norm = normalize_date_aware(os_vals[j], df_vals[j])

            if os_norm != df_norm:
                col_name = os_cols[j] if os_cols and j < len(os_cols) else f"col{j}"
                result.value_mismatches.append(
                    f"row {i}, col '{col_name}': OS={str(os_vals[j])[:60]} vs DF={str(df_vals[j])[:60]}"
                )

    # Determine final status
    if not result.row_count_match:
        result.status = "FAIL"
    elif result.value_mismatches:
        result.status = "FAIL"
    else:
        result.status = "PASS"

    return result


# ── Batch comparison ─────────────────────────────────────────────────────────

# Queries that have ORDER BY (compare in order)
ORDERED_QUERIES = {8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 24, 25, 26, 27,
                   28, 29, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43}

# Queries known to be problematic
EXPECTED_FAILURES = {
    34: "timeout on high-cardinality GROUP BY (URL, ~100M unique values)",
    35: "timeout on high-cardinality GROUP BY (URL, ~100M unique values)",
    36: "TopK OOM on high-cardinality clientip GROUP BY",
}


def batch_compare(os_dir, df_dir, max_rows=100, num_queries=43):
    """Compare all queries in batch.

    Args:
        os_dir: Directory with OS response files (q1.json .. q43.json)
        df_dir: Directory with DF response files (q1.txt .. q43.txt)
        max_rows: Maximum rows to compare per query
        num_queries: Number of queries (default 43)

    Returns:
        list of ComparisonResult
    """
    results = []
    for q in range(1, num_queries + 1):
        os_file = os.path.join(os_dir, f"q{q}.json")
        df_file = os.path.join(df_dir, f"q{q}.txt")

        if not os.path.exists(os_file) and not os.path.exists(df_file):
            r = ComparisonResult(q)
            r.status = "SKIP"
            r.message = "no output files found"
            results.append(r)
            continue

        ordered = q in ORDERED_QUERIES
        result = compare_query(q, os_file, df_file, max_rows=max_rows, ordered=ordered)
        results.append(result)

    return results


def print_summary(results):
    """Print a summary table of all results."""
    pass_count = sum(1 for r in results if r.status == "PASS")
    fail_count = sum(1 for r in results if r.status == "FAIL")
    skip_count = sum(1 for r in results if r.status == "SKIP")
    error_count = sum(1 for r in results if r.status == "ERROR")

    print("=" * 80)
    print("CORRECTNESS VERIFICATION SUMMARY")
    print("=" * 80)
    print()

    # Per-query results
    for r in results:
        q = r.query_id
        marker = "  "
        if q in EXPECTED_FAILURES:
            marker = "* "
        status_icon = {
            "PASS": "OK  ",
            "FAIL": "FAIL",
            "SKIP": "SKIP",
            "ERROR": "ERR ",
        }.get(r.status, "????")
        print(f"  {marker}{status_icon}  {r.summary_line()}")

    print()
    print("-" * 80)
    print(f"  PASS:  {pass_count:3d}")
    print(f"  FAIL:  {fail_count:3d}")
    print(f"  SKIP:  {skip_count:3d}")
    print(f"  ERROR: {error_count:3d}")
    print(f"  TOTAL: {len(results):3d}")
    print()

    expected_fail_ids = set(EXPECTED_FAILURES.keys())
    unexpected_fails = [r for r in results if r.status == "FAIL" and r.query_id not in expected_fail_ids]
    if unexpected_fails:
        print("UNEXPECTED FAILURES:")
        for r in unexpected_fails:
            print(f"  {r.summary_line()}")
        print()

    # Print details for failures
    failures = [r for r in results if r.status in ("FAIL", "ERROR")]
    if failures:
        print("=" * 80)
        print("FAILURE DETAILS")
        print("=" * 80)
        for r in failures:
            print()
            print(r.detail_report())

    print()
    if unexpected_fails:
        print(f"RESULT: FAIL ({len(unexpected_fails)} unexpected failures)")
        return 1
    else:
        print(f"RESULT: PASS ({pass_count} passed, {skip_count} skipped, "
              f"{fail_count} expected failures)")
        return 0


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compare OpenSearch lakehouse and datafusion-cli query results"
    )
    subparsers = parser.add_subparsers(dest="command")

    # Single-query comparison
    single = subparsers.add_parser("single", help="Compare a single query")
    single.add_argument("os_file", help="OpenSearch JSON response file")
    single.add_argument("df_file", help="DataFusion CLI text output file")
    single.add_argument("--max-rows", type=int, default=100, help="Max rows to compare")
    single.add_argument("--ordered", action="store_true", help="Order-sensitive comparison")
    single.add_argument("--query-id", type=int, default=0, help="Query ID for reporting")

    # Batch comparison
    batch = subparsers.add_parser("batch", help="Compare all queries in batch")
    batch.add_argument("--os-dir", required=True, help="Directory with OS response files")
    batch.add_argument("--df-dir", required=True, help="Directory with DF response files")
    batch.add_argument("--max-rows", type=int, default=100, help="Max rows to compare")
    batch.add_argument("--num-queries", type=int, default=43, help="Number of queries")

    args = parser.parse_args()

    if args.command == "single":
        result = compare_query(
            args.query_id, args.os_file, args.df_file,
            max_rows=args.max_rows, ordered=args.ordered
        )
        print(result.detail_report())
        sys.exit(0 if result.status == "PASS" else 1)

    elif args.command == "batch":
        results = batch_compare(
            args.os_dir, args.df_dir,
            max_rows=args.max_rows, num_queries=args.num_queries
        )
        exit_code = print_summary(results)
        sys.exit(exit_code)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
