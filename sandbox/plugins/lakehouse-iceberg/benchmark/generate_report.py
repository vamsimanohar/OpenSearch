#!/usr/bin/env python3
"""Generate markdown comparison report from benchmark CSV results."""
import csv
import json
import os
import sys
from pathlib import Path

def load_csv(path):
    """Load CSV file into list of dicts."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))

def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def load_raw_response(raw_dir, qnum):
    """Load raw JSON response for a query."""
    path = os.path.join(raw_dir, f"q{qnum}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return None

def extract_first_row(raw_dir, qnum):
    """Extract first row from raw response for correctness comparison."""
    data = load_raw_response(raw_dir, qnum)
    if not data:
        return None
    rows = data.get("datarows", data.get("rows", []))
    if rows:
        return rows[0]
    return None

def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results/comparison"

    configs = {
        "datafusion_cli": "DataFusion CLI",
        "greedy_32gb": "Greedy 32GB",
        "greedy_unlimited": "Greedy Unlimited",
        "fairspill_32gb": "FairSpill 32GB",
    }

    data = {}
    for key in configs:
        csv_path = os.path.join(results_dir, f"{key}.csv")
        rows = load_csv(csv_path)
        if rows:
            data[key] = rows

    if not data:
        print("No benchmark data found in", results_dir)
        return

    report_path = os.path.join(results_dir, "BENCHMARK_REPORT.md")
    with open(report_path, "w") as f:
        f.write("# ClickBench Memory Pool Benchmark Report\n\n")
        f.write(f"**Date:** {os.popen('date').read().strip()}\n")
        f.write("**Dataset:** S3-30F (100M rows, 30 Parquet files, Iceberg via Glue)\n")
        f.write(f"**Machine:** {os.popen('uname -n').read().strip()} "
                f"({os.popen('nproc').read().strip()} CPUs, "
                f"{os.popen('free -g').read().splitlines()[1].split()[1]}GB RAM)\n\n")

        # ── Summary ──────────────────────────────────────────────────────
        f.write("## Summary\n\n")
        f.write("| Config | Passed | Failed | Timeout | Total Time (s) | Avg Query (s) |\n")
        f.write("|--------|--------|--------|---------|----------------|---------------|\n")
        for key, label in configs.items():
            if key not in data:
                continue
            rows = data[key]
            ok = [r for r in rows if r["status"] == "OK"]
            fail = [r for r in rows if r["status"] == "FAIL"]
            to = [r for r in rows if r.get("status") == "TIMEOUT"]
            total = sum(safe_float(r["time_s"]) for r in ok)
            avg = total / len(ok) if ok else 0
            f.write(f"| {label} | {len(ok)}/{len(rows)} | {len(fail)} | {len(to)} | "
                    f"{total:.1f} | {avg:.2f} |\n")
        f.write("\n")

        # ── Per-query time comparison ────────────────────────────────────
        f.write("## Per-Query Time Comparison (seconds)\n\n")
        active_configs = [k for k in configs if k in data]
        header = "| Q# |"
        sep = "|---:|"
        for key in active_configs:
            header += f" {configs[key]} |"
            sep += "--------:|"
        # Add speedup column (DF-CLI vs best OS config)
        os_configs = [k for k in active_configs if k != "datafusion_cli"]
        if "datafusion_cli" in data and os_configs:
            header += " Ratio (best OS / DF-CLI) |"
            sep += "--------:|"
        f.write(header + "\n")
        f.write(sep + "\n")

        for q in range(43):
            qnum = q + 1
            row = f"| Q{qnum} |"
            times = {}
            for key in active_configs:
                if key in data and q < len(data[key]):
                    r = data[key][q]
                    if r["status"] == "OK":
                        t = safe_float(r["time_s"])
                        times[key] = t
                        row += f" {t:.3f} |"
                    else:
                        row += f" {r['status']} |"
                else:
                    row += " - |"

            # Speedup ratio
            if "datafusion_cli" in times and os_configs:
                df_time = times["datafusion_cli"]
                os_times = [times[k] for k in os_configs if k in times]
                best_os = min(os_times) if os_times else float("inf")
                if df_time > 0 and best_os < float("inf"):
                    ratio = best_os / df_time
                    marker = " :white_check_mark:" if ratio < 2.0 else " :warning:" if ratio < 5.0 else " :x:"
                    row += f" {ratio:.1f}x{marker} |"
                else:
                    row += " - |"
            elif "datafusion_cli" in data and os_configs:
                row += " - |"

            f.write(row + "\n")
        f.write("\n")

        # ── Memory usage (RSS) for OS configs ────────────────────────────
        f.write("## Memory Usage (Process RSS, MB)\n\n")
        if os_configs:
            header = "| Q# |"
            sep = "|---:|"
            for key in os_configs:
                header += f" {configs[key]} (before→after) |"
                sep += "--------:|"
            f.write(header + "\n")
            f.write(sep + "\n")
            for q in range(43):
                qnum = q + 1
                row = f"| Q{qnum} |"
                for key in os_configs:
                    if key in data and q < len(data[key]):
                        r = data[key][q]
                        before = r.get("rss_before_mb", "-")
                        after = r.get("rss_after_mb", "-")
                        row += f" {before}→{after} |"
                    else:
                        row += " - |"
                f.write(row + "\n")
            f.write("\n")

        # ── datafusion-cli peak RSS ──────────────────────────────────────
        if "datafusion_cli" in data:
            f.write("## DataFusion CLI Peak RSS (MB)\n\n")
            f.write("| Q# | Time (s) | Peak RSS (MB) |\n")
            f.write("|---:|--------:|--------:|\n")
            for q in range(min(43, len(data["datafusion_cli"]))):
                r = data["datafusion_cli"][q]
                rss = r.get("peak_rss_mb", "-")
                t = r["time_s"]
                status = r["status"]
                if status == "OK":
                    f.write(f"| Q{q+1} | {safe_float(t):.3f} | {rss} |\n")
                else:
                    f.write(f"| Q{q+1} | {status} | {rss} |\n")
            f.write("\n")

        # ── Correctness comparison ───────────────────────────────────────
        f.write("## Correctness Analysis\n\n")

        # Compare row counts across OS configs
        if len(os_configs) > 1:
            f.write("### Row Count Consistency Across OpenSearch Configs\n\n")
            mismatches = []
            for q in range(43):
                counts = {}
                for key in os_configs:
                    if key in data and q < len(data[key]):
                        r = data[key][q]
                        if r["status"] == "OK":
                            counts[key] = r.get("row_count", "?")
                if len(set(counts.values())) > 1:
                    mismatches.append((q + 1, counts))
            if mismatches:
                f.write(f"**{len(mismatches)} queries with different row counts:**\n\n")
                for qnum, counts in mismatches:
                    f.write(f"- Q{qnum}: {counts}\n")
            else:
                f.write("All queries produce consistent row counts across OpenSearch configs.\n")
            f.write("\n")

        # Compare first-row samples between configs (if raw data available)
        f.write("### Query Status by Config\n\n")
        f.write("| Q# |")
        for key in active_configs:
            f.write(f" {configs[key]} |")
        f.write("\n")
        f.write("|---:|")
        for _ in active_configs:
            f.write("--------|")
        f.write("\n")
        for q in range(43):
            row = f"| Q{q+1} |"
            for key in active_configs:
                if key in data and q < len(data[key]):
                    status = data[key][q]["status"]
                    if status == "OK":
                        row += " OK |"
                    elif status == "FAIL":
                        row += " FAIL |"
                    else:
                        row += f" {status} |"
                else:
                    row += " - |"
            f.write(row + "\n")
        f.write("\n")

        # ── Performance tiers ────────────────────────────────────────────
        f.write("## Performance Tiers (Best OS Config)\n\n")
        if os_configs:
            fast = []   # <2s
            medium = []  # 2-10s
            slow = []   # 10-60s
            very_slow = []  # >60s
            failed = []

            for q in range(43):
                best_time = float("inf")
                best_config = None
                any_ok = False
                for key in os_configs:
                    if key in data and q < len(data[key]):
                        r = data[key][q]
                        if r["status"] == "OK":
                            any_ok = True
                            t = safe_float(r["time_s"])
                            if t < best_time:
                                best_time = t
                                best_config = key
                if not any_ok:
                    failed.append(q + 1)
                elif best_time < 2:
                    fast.append((q + 1, best_time, best_config))
                elif best_time < 10:
                    medium.append((q + 1, best_time, best_config))
                elif best_time < 60:
                    slow.append((q + 1, best_time, best_config))
                else:
                    very_slow.append((q + 1, best_time, best_config))

            f.write(f"- **Fast (<2s):** {len(fast)} queries — {', '.join(f'Q{q}({t:.1f}s)' for q,t,_ in fast)}\n")
            f.write(f"- **Medium (2-10s):** {len(medium)} queries — {', '.join(f'Q{q}({t:.1f}s)' for q,t,_ in medium)}\n")
            f.write(f"- **Slow (10-60s):** {len(slow)} queries — {', '.join(f'Q{q}({t:.1f}s)' for q,t,_ in slow)}\n")
            f.write(f"- **Very Slow (>60s):** {len(very_slow)} queries — {', '.join(f'Q{q}({t:.1f}s)' for q,t,_ in very_slow)}\n")
            f.write(f"- **Failed/Timeout:** {len(failed)} queries — {', '.join(f'Q{q}' for q in failed)}\n")
            f.write("\n")

        # ── Config comparison analysis ───────────────────────────────────
        f.write("## Config Winner Per Query\n\n")
        if len(os_configs) > 1:
            f.write("Which OpenSearch config was fastest for each query:\n\n")
            f.write("| Q# | Winner | Time (s) | Runner-up | Time (s) | Delta |\n")
            f.write("|---:|--------|--------:|-----------|--------:|------:|\n")
            for q in range(43):
                qnum = q + 1
                times_by_config = {}
                for key in os_configs:
                    if key in data and q < len(data[key]):
                        r = data[key][q]
                        if r["status"] == "OK":
                            times_by_config[key] = safe_float(r["time_s"])
                if len(times_by_config) >= 2:
                    sorted_configs = sorted(times_by_config.items(), key=lambda x: x[1])
                    winner = sorted_configs[0]
                    runner = sorted_configs[1]
                    delta = runner[1] - winner[1]
                    f.write(f"| Q{qnum} | {configs[winner[0]]} | {winner[1]:.3f} | "
                            f"{configs[runner[0]]} | {runner[1]:.3f} | +{delta:.3f} |\n")
                elif len(times_by_config) == 1:
                    winner = list(times_by_config.items())[0]
                    f.write(f"| Q{qnum} | {configs[winner[0]]} | {winner[1]:.3f} | - | - | - |\n")
                else:
                    f.write(f"| Q{qnum} | N/A | - | - | - | - |\n")
            f.write("\n")

        # ── Recommendation ───────────────────────────────────────────────
        f.write("## Recommendation\n\n")

        # Auto-generate recommendation based on data
        if os_configs:
            # Count wins per config
            wins = {k: 0 for k in os_configs}
            total_times = {k: 0.0 for k in os_configs}
            for q in range(43):
                best_time = float("inf")
                best_key = None
                for key in os_configs:
                    if key in data and q < len(data[key]):
                        r = data[key][q]
                        if r["status"] == "OK":
                            t = safe_float(r["time_s"])
                            total_times[key] += t
                            if t < best_time:
                                best_time = t
                                best_key = key
                if best_key:
                    wins[best_key] += 1

            f.write("### Auto-Analysis\n\n")
            f.write("| Config | Wins | Total Time (s) |\n")
            f.write("|--------|-----:|--------:|\n")
            for key in os_configs:
                f.write(f"| {configs[key]} | {wins[key]} | {total_times[key]:.1f} |\n")
            f.write("\n")

            best_config = min(os_configs, key=lambda k: total_times.get(k, float("inf")))
            f.write(f"**Best overall config: {configs[best_config]}** "
                    f"(lowest total query time: {total_times[best_config]:.1f}s)\n\n")

            f.write("### Production Recommendations\n\n")
            f.write("- **Single-user / analytics workload:** Use GreedyMemoryPool with a limit "
                    "(32GB or ~50% of available non-heap memory). Fastest per-query performance.\n")
            f.write("- **Multi-user / concurrent workload:** Use GreedyMemoryPool with limit. "
                    "FairSpillPool divides memory too aggressively across partitions, causing all "
                    "operators to spill even when total memory is available. The Greedy pool lets "
                    "the first query use available memory; subsequent queries queue or spill only when "
                    "actually needed.\n")
            f.write("- **Avoid:** FairSpillPool for Iceberg/Parquet workloads — the per-partition "
                    "division (pool_size / num_spill_consumers) gives each hash aggregate only "
                    "~660MB on a 36-core machine with 24GB pool, causing massive unnecessary spilling.\n")
            f.write("- **Memory sizing:** Set DataFusion pool to ~50% of non-JVM-heap memory. "
                    "E.g., on a 32GB node with 8GB JVM heap, set DataFusion pool to 12GB.\n")

    print(f"Report written to {report_path}")

if __name__ == "__main__":
    main()
