#!/usr/bin/env python3
"""Compare query results across all benchmark configs for correctness."""
import csv
import json
import os
import sys
from pathlib import Path


def load_csv(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def load_raw_response(raw_dir, qnum):
    # Try JSON first (OpenSearch), then text (datafusion-cli)
    json_path = os.path.join(raw_dir, f"q{qnum}.json")
    txt_path = os.path.join(raw_dir, f"q{qnum}.txt")

    if os.path.exists(json_path):
        try:
            with open(json_path) as f:
                content = f.read().strip()
                if not content:
                    return None
                return json.loads(content)
        except (json.JSONDecodeError, Exception):
            return None

    if os.path.exists(txt_path):
        with open(txt_path) as f:
            return {"_raw_text": f.read()}
    return None


def extract_os_rows(data):
    """Extract rows from OpenSearch JSON response."""
    if not data or "error" in data:
        return None, None
    rows = data.get("datarows", data.get("rows", []))
    schema = data.get("schema", [])
    col_names = [s.get("name", s.get("alias", f"col{i}")) for i, s in enumerate(schema)] if schema else None
    return rows, col_names


def extract_df_rows(data):
    """Extract rows from datafusion-cli text output.

    DF-CLI format (multiple statements, we want the LAST table):
      DataFusion CLI v53.0.0
      0 row(s) fetched.       <- SET result
      0 row(s) fetched.       <- CREATE TABLE result
      +----------+            <- separator
      | count(*) |            <- header
      +----------+            <- separator
      | 99997497 |            <- data row
      +----------+            <- separator
      1 row(s) fetched.
    """
    if not data or "_raw_text" not in data:
        return None, None
    text = data["_raw_text"]
    lines = text.strip().split("\n")

    # Find all table blocks: sequences of +---+ and | ... | lines
    # A table block starts with +---+, then | header |, then +---+, then | data |*, then +---+
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
                # Check if this is a valid table (has at least separator + header + separator)
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
        return None, None

    # Use the LAST table block (skip SET/CREATE TABLE results)
    block = blocks[-1]

    # Extract header and data rows from the block
    pipe_lines = [l for l in block if l.startswith("|")]
    if not pipe_lines:
        return None, None

    header = [c.strip() for c in pipe_lines[0].split("|")[1:-1]]
    rows = []
    for line in pipe_lines[1:]:
        vals = [c.strip() for c in line.split("|")[1:-1]]
        rows.append(vals)

    return rows, header


def normalize_value(val):
    """Normalize a value for comparison."""
    if val is None:
        return "NULL"
    s = str(val).strip()
    # Normalize empty strings
    if s in ("", "NULL", "null", "None"):
        return "NULL"
    # Try to normalize numbers
    try:
        f = float(s)
        if f == int(f) and "." not in s and "e" not in s.lower():
            return str(int(f))
        return f"{f:.6g}"
    except (ValueError, OverflowError):
        pass
    return s


def compare_rows(os_rows, df_rows, os_cols, df_cols, qnum):
    """Compare OpenSearch and DF-CLI results for a single query."""
    issues = []

    if os_rows is None and df_rows is None:
        return [f"Q{qnum}: Both configs returned no data"]
    if os_rows is None:
        return [f"Q{qnum}: OpenSearch returned no data, DF-CLI has {len(df_rows)} rows"]
    if df_rows is None:
        return [f"Q{qnum}: DF-CLI returned no data, OpenSearch has {len(os_rows)} rows"]

    # Row count comparison
    if len(os_rows) != len(df_rows):
        issues.append(f"Q{qnum}: Row count mismatch — OS={len(os_rows)}, DF-CLI={len(df_rows)}")

    # Column name comparison
    if os_cols and df_cols:
        os_names = [c.lower() for c in os_cols]
        df_names = [c.lower() for c in df_cols]
        if os_names != df_names:
            # Check if it's just naming vs actual different columns
            differences = []
            for i, (a, b) in enumerate(zip(os_names, df_names)):
                if a != b:
                    differences.append(f"col{i}: OS='{os_cols[i]}' vs DF='{df_cols[i]}'")
            if differences:
                issues.append(f"Q{qnum}: Column name differences — {'; '.join(differences)}")

    # Value comparison (first N rows)
    compare_count = min(len(os_rows), len(df_rows), 10)
    value_mismatches = []
    for i in range(compare_count):
        os_row = os_rows[i]
        df_row = df_rows[i]

        # Normalize both to lists of strings
        if isinstance(os_row, dict):
            os_vals = list(os_row.values())
        else:
            os_vals = list(os_row)

        if isinstance(df_row, dict):
            df_vals = list(df_row.values())
        else:
            df_vals = list(df_row)

        for j in range(min(len(os_vals), len(df_vals))):
            os_norm = normalize_value(os_vals[j])
            df_norm = normalize_value(df_vals[j])
            if os_norm != df_norm:
                col_name = os_cols[j] if os_cols and j < len(os_cols) else f"col{j}"
                # Check if it's a known representation difference
                is_date_diff = False
                try:
                    # eventdate: OS returns epoch days, DF returns date string
                    os_int = int(str(os_vals[j]))
                    if 15000 < os_int < 25000:  # Epoch days range
                        is_date_diff = True
                except (ValueError, TypeError):
                    pass

                if is_date_diff:
                    value_mismatches.append(
                        f"row{i}.{col_name}: OS={os_vals[j]}(epoch-day) vs DF={df_vals[j]}(date-str) [KNOWN]"
                    )
                else:
                    value_mismatches.append(
                        f"row{i}.{col_name}: OS={str(os_vals[j])[:50]} vs DF={str(df_vals[j])[:50]}"
                    )

    if value_mismatches:
        issues.append(f"Q{qnum}: Value differences — {'; '.join(value_mismatches[:5])}")
        if len(value_mismatches) > 5:
            issues[-1] += f" ... (+{len(value_mismatches)-5} more)"

    if not issues:
        issues.append(f"Q{qnum}: MATCH (rows={len(os_rows)})")

    return issues


def compare_os_configs(configs_data, qnum):
    """Compare results across OpenSearch configs for a single query."""
    issues = []
    row_counts = {}
    for config, data in configs_data.items():
        if data and "error" not in data:
            rows = data.get("datarows", data.get("rows", []))
            row_counts[config] = len(rows)

    if len(set(row_counts.values())) > 1:
        issues.append(f"Q{qnum}: OS config row count mismatch — {row_counts}")

    return issues


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results/comparison"

    configs = {
        "datafusion_cli": "DataFusion CLI",
        "greedy_32gb": "Greedy 32GB",
        "greedy_unlimited": "Greedy Unlimited",
        "fairspill_32gb": "FairSpill 32GB",
    }

    # Load CSV data
    csv_data = {}
    for key in configs:
        csv_path = os.path.join(results_dir, f"{key}.csv")
        rows = load_csv(csv_path)
        if rows:
            csv_data[key] = rows

    print("=" * 80)
    print("CORRECTNESS VERIFICATION REPORT")
    print("=" * 80)
    print()

    # Available configs
    available = list(csv_data.keys())
    print(f"Available configs: {', '.join(configs[k] for k in available)}")
    print()

    os_configs = [k for k in available if k != "datafusion_cli"]

    all_issues = []
    match_count = 0
    mismatch_count = 0

    # 1. Compare DF-CLI vs each OS config
    if "datafusion_cli" in csv_data:
        print("-" * 80)
        print("SECTION 1: DataFusion CLI vs OpenSearch Configs")
        print("-" * 80)
        print()

        for os_config in os_configs:
            print(f"### {configs['datafusion_cli']} vs {configs[os_config]}")
            print()

            df_raw_dir = os.path.join(results_dir, "datafusion_cli_raw")
            os_raw_dir = os.path.join(results_dir, f"{os_config}_raw")

            for q in range(43):
                qnum = q + 1

                # Check status in CSV
                df_status = csv_data["datafusion_cli"][q]["status"] if q < len(csv_data["datafusion_cli"]) else "MISSING"
                os_status = csv_data[os_config][q]["status"] if q < len(csv_data[os_config]) else "MISSING"

                if df_status != "OK" or os_status != "OK":
                    status_msg = f"Q{qnum}: Skipped — DF={df_status}, OS={os_status}"
                    print(f"  {status_msg}")
                    all_issues.append(status_msg)
                    continue

                # Load raw data
                df_data = load_raw_response(df_raw_dir, qnum)
                os_data = load_raw_response(os_raw_dir, qnum)

                df_rows, df_cols = extract_df_rows(df_data)
                os_rows, os_cols = extract_os_rows(os_data)

                issues = compare_rows(os_rows, df_rows, os_cols, df_cols, qnum)
                for issue in issues:
                    if "MATCH" in issue:
                        match_count += 1
                        print(f"  OK  {issue}")
                    else:
                        mismatch_count += 1
                        print(f"  !!  {issue}")
                        all_issues.append(issue)
            print()

    # 2. Compare across OS configs
    if len(os_configs) > 1:
        print("-" * 80)
        print("SECTION 2: Cross-Config Consistency (OpenSearch configs)")
        print("-" * 80)
        print()

        for q in range(43):
            qnum = q + 1
            configs_data = {}
            for oc in os_configs:
                raw_dir = os.path.join(results_dir, f"{oc}_raw")
                data = load_raw_response(raw_dir, qnum)
                if data:
                    configs_data[oc] = data

            if len(configs_data) > 1:
                issues = compare_os_configs(configs_data, qnum)
                for issue in issues:
                    print(f"  !!  {issue}")
                    all_issues.append(issue)

        if not all_issues:
            print("  All queries consistent across OS configs")
        print()

    # 3. Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"  Matches: {match_count}")
    print(f"  Mismatches: {mismatch_count}")
    print(f"  Total issues: {len(all_issues)}")
    print()

    if all_issues:
        print("All issues:")
        for issue in all_issues:
            print(f"  - {issue}")


if __name__ == "__main__":
    main()
