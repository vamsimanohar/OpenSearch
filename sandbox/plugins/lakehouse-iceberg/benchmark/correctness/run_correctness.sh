#!/bin/bash
# =============================================================================
# Correctness Verification Suite: OpenSearch Lakehouse vs DataFusion CLI
#
# Runs the same 43 ClickBench queries on both engines and compares results.
#
# Usage:
#   ./run_correctness.sh [options]
#
# Options:
#   --endpoint URL      OpenSearch endpoint (default: http://localhost:9200)
#   --table TABLE       OpenSearch table name (default: hits_s3)
#   --data-path PATH    Path to parquet data for datafusion-cli
#                       (S3 path, local file, or local directory)
#                       Default: s3://iceberg-benchmark-test-263689514295/iceberg-warehouse/hits/data/
#   --queries-dir DIR   Custom queries directory (default: ./queries)
#   --output-dir DIR    Output directory for results (default: ./results)
#   --timeout SECS      Query timeout in seconds (default: 120)
#   --max-rows N        Max rows to compare (default: 100)
#   --skip-os           Skip OpenSearch execution (use existing results)
#   --skip-df           Skip DataFusion execution (use existing results)
#   --queries Q1,Q2..   Run only specific queries (e.g., 1,2,5,37)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Defaults ─────────────────────────────────────────────────────────────────
ENDPOINT="http://localhost:9200"
TABLE="hits_s3"
DATA_PATH="s3://iceberg-benchmark-test-263689514295/iceberg-warehouse/hits/data/"
QUERIES_DIR="${SCRIPT_DIR}/queries"
OUTPUT_DIR="${SCRIPT_DIR}/results"
TIMEOUT=120
MAX_ROWS=100
SKIP_OS=false
SKIP_DF=false
QUERY_FILTER=""
DATAFUSION_CLI=""

# ── Parse arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --endpoint)   ENDPOINT="$2"; shift 2 ;;
        --table)      TABLE="$2"; shift 2 ;;
        --data-path)  DATA_PATH="$2"; shift 2 ;;
        --queries-dir) QUERIES_DIR="$2"; shift 2 ;;
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        --timeout)    TIMEOUT="$2"; shift 2 ;;
        --max-rows)   MAX_ROWS="$2"; shift 2 ;;
        --skip-os)    SKIP_OS=true; shift ;;
        --skip-df)    SKIP_DF=true; shift ;;
        --queries)    QUERY_FILTER="$2"; shift 2 ;;
        --help|-h)
            head -20 "$0" | grep '^#' | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# ── Validate ─────────────────────────────────────────────────────────────────
if [ "$SKIP_DF" = false ]; then
    # Find datafusion-cli
    if [ -z "$DATAFUSION_CLI" ]; then
        if command -v datafusion-cli &>/dev/null; then
            DATAFUSION_CLI="$(command -v datafusion-cli)"
        elif [ -x "$HOME/.cargo/bin/datafusion-cli" ]; then
            DATAFUSION_CLI="$HOME/.cargo/bin/datafusion-cli"
        else
            echo "ERROR: datafusion-cli not found in PATH or ~/.cargo/bin/"
            echo "  Install with: cargo install datafusion-cli"
            exit 1
        fi
    fi
fi

# ── Setup output directories ─────────────────────────────────────────────────
OS_RAW_DIR="${OUTPUT_DIR}/os_raw"
DF_RAW_DIR="${OUTPUT_DIR}/df_raw"
mkdir -p "$OS_RAW_DIR" "$DF_RAW_DIR"

# ── Resource tracking helpers ────────────────────────────────────────────────
OS_METRICS_CSV="${OUTPUT_DIR}/os_metrics.csv"
echo "query,time_s,status,rss_before_mb,rss_after_mb,cpu_pct,row_count" > "$OS_METRICS_CSV"

# Find the OpenSearch JVM process PID
find_os_pid() {
    pgrep -f "org.opensearch.bootstrap.OpenSearch" | head -1
}

# Get RSS in MB for a given PID
get_rss_mb() {
    local pid="$1"
    if [ -n "$pid" ] && [ -d "/proc/$pid" ]; then
        awk '/^VmRSS:/ {printf "%.0f", $2/1024}' "/proc/$pid/status" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Get CPU% for a given PID (instantaneous from /proc/stat)
get_cpu_pct() {
    local pid="$1"
    if [ -n "$pid" ] && [ -d "/proc/$pid" ]; then
        ps -p "$pid" -o %cpu= 2>/dev/null | tr -d ' ' || echo "0"
    else
        echo "0"
    fi
}

OS_PID=$(find_os_pid)
if [ -n "$OS_PID" ]; then
    echo "  OpenSearch PID: ${OS_PID} (RSS: $(get_rss_mb $OS_PID) MB)"
else
    echo "  WARNING: Could not find OpenSearch PID for resource tracking"
fi

# ── Determine which queries to run ───────────────────────────────────────────
get_query_list() {
    if [ -n "$QUERY_FILTER" ]; then
        echo "$QUERY_FILTER" | tr ',' '\n'
    else
        seq 1 43
    fi
}

# ── Safe JSON payload generation ─────────────────────────────────────────────
make_json_payload() {
    python3 -c "import json,sys; print(json.dumps({'query': sys.stdin.read().strip()}))" <<< "$1"
}

# ── Header ───────────────────────────────────────────────────────────────────
echo "============================================================"
echo "  Correctness Verification: OpenSearch Lakehouse vs DataFusion CLI"
echo "============================================================"
echo "  Date:        $(date)"
echo "  Endpoint:    ${ENDPOINT}"
echo "  Table:       ${TABLE}"
echo "  Data path:   ${DATA_PATH:-N/A}"
echo "  Queries dir: ${QUERIES_DIR}"
echo "  Output dir:  ${OUTPUT_DIR}"
echo "  Timeout:     ${TIMEOUT}s"
echo "  Max rows:    ${MAX_ROWS}"
echo "  Skip OS:     ${SKIP_OS}"
echo "  Skip DF:     ${SKIP_DF}"
echo "  Queries:     ${QUERY_FILTER:-all (1-43)}"
echo "============================================================"
echo ""

# ── Known problematic queries ────────────────────────────────────────────────
# Q34, Q35: timeout on high-cardinality GROUP BY (URL ~100M unique values)
# Q36: TopK OOM on high-cardinality clientip GROUP BY
EXPECTED_FAILURES="34 35 36"

is_expected_failure() {
    local q="$1"
    for ef in $EXPECTED_FAILURES; do
        if [ "$q" = "$ef" ]; then
            return 0
        fi
    done
    return 1
}

# ── Phase 1: Run OpenSearch queries ──────────────────────────────────────────
if [ "$SKIP_OS" = false ]; then
    echo "--- Phase 1: Running OpenSearch queries ---"
    echo ""

    # Quick health check
    if ! curl -s -m 5 "${ENDPOINT}" > /dev/null 2>&1; then
        echo "ERROR: Cannot reach OpenSearch at ${ENDPOINT}"
        echo "  Make sure OpenSearch is running with the lakehouse plugin."
        exit 1
    fi

    os_pass=0
    os_fail=0
    os_skip=0

    for q in $(get_query_list); do
        qnum=$(printf "%02d" "$q")
        query_file="${QUERIES_DIR}/q${qnum}_os.sql"

        if [ ! -f "$query_file" ]; then
            echo "  Q${q}: SKIP (no query file: ${query_file})"
            os_skip=$((os_skip + 1))
            continue
        fi

        # Read query, replace table name
        sql=$(cat "$query_file")
        sql=$(echo "$sql" | sed "s/\bFROM hits\b/FROM ${TABLE}/gI; s/\bJOIN hits\b/JOIN ${TABLE}/gI")
        # Remove trailing semicolons (OpenSearch doesn't want them)
        sql="${sql%;}"

        payload=$(make_json_payload "$sql")

        # Capture resource metrics before query
        rss_before=$(get_rss_mb "$OS_PID")

        # Execute with timeout
        start_ns=$(date +%s%N)
        result=$(curl -s -m "$TIMEOUT" -X POST "${ENDPOINT}/_lakehouse/sql" \
            -H "Content-Type: application/json" \
            -d "$payload" 2>&1) || true
        curl_exit=$?
        end_ns=$(date +%s%N)
        elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
        elapsed_s=$(python3 -c "print(round(${elapsed_ms} / 1000.0, 2))")

        # Capture resource metrics after query
        rss_after=$(get_rss_mb "$OS_PID")
        cpu_pct=$(get_cpu_pct "$OS_PID")

        # Save raw response
        echo "$result" > "${OS_RAW_DIR}/q${q}.json"

        if [ "$curl_exit" -eq 28 ] || [ -z "$result" ]; then
            if is_expected_failure "$q"; then
                echo "  Q${q}: TIMEOUT (${elapsed_s}s) [expected]  RSS: ${rss_before}→${rss_after} MB"
            else
                echo "  Q${q}: TIMEOUT (${elapsed_s}s)  RSS: ${rss_before}→${rss_after} MB"
            fi
            echo "${q},${elapsed_s},TIMEOUT,${rss_before},${rss_after},${cpu_pct},0" >> "$OS_METRICS_CSV"
            os_fail=$((os_fail + 1))
        elif echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if 'error' in d else 1)" 2>/dev/null; then
            error=$(echo "$result" | python3 -c "
import sys, json
d = json.load(sys.stdin)
e = d.get('error', {})
if isinstance(e, dict):
    print(e.get('reason', str(e))[:80])
else:
    print(str(e)[:80])
" 2>/dev/null) || error="unknown"
            echo "  Q${q}: ERROR (${elapsed_s}s) -- ${error}  RSS: ${rss_before}→${rss_after} MB"
            echo "${q},${elapsed_s},ERROR,${rss_before},${rss_after},${cpu_pct},0" >> "$OS_METRICS_CSV"
            os_fail=$((os_fail + 1))
        else
            row_count=$(echo "$result" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(len(d.get('datarows', d.get('rows', []))))
" 2>/dev/null) || row_count="?"
            echo "  Q${q}: OK (${elapsed_s}s, rows=${row_count})  RSS: ${rss_before}→${rss_after} MB, CPU: ${cpu_pct}%"
            echo "${q},${elapsed_s},OK,${rss_before},${rss_after},${cpu_pct},${row_count}" >> "$OS_METRICS_CSV"
            os_pass=$((os_pass + 1))
        fi
    done

    echo ""
    echo "  OpenSearch: ${os_pass} OK, ${os_fail} failed/timeout, ${os_skip} skipped"
    # Resource summary
    if [ -n "$OS_PID" ]; then
        peak_rss=$(awk -F',' 'NR>1 {if($5+0 > max) max=$5+0} END {print max+0}' "$OS_METRICS_CSV")
        echo "  Peak RSS: ${peak_rss} MB"
        echo "  Metrics saved to: ${OS_METRICS_CSV}"
    fi
    echo ""
fi

# ── Phase 2: Run DataFusion queries ──────────────────────────────────────────
if [ "$SKIP_DF" = false ]; then
    echo "--- Phase 2: Running DataFusion CLI queries ---"
    echo ""

    # Build the preamble SQL (table setup)
    DF_PREAMBLE="SET datafusion.execution.listing_table_ignore_subdirectory = false;"
    DF_PREAMBLE="${DF_PREAMBLE} CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '${DATA_PATH}' OPTIONS ('binary_as_string' 'true');"

    df_pass=0
    df_fail=0
    df_skip=0

    for q in $(get_query_list); do
        qnum=$(printf "%02d" "$q")
        query_file="${QUERIES_DIR}/q${qnum}_df.sql"

        if [ ! -f "$query_file" ]; then
            echo "  Q${q}: SKIP (no query file: ${query_file})"
            df_skip=$((df_skip + 1))
            continue
        fi

        sql=$(cat "$query_file")

        # Write full SQL to temp file (preamble + query)
        tmpfile=$(mktemp /tmp/df_correctness_XXXXX.sql)
        echo "${DF_PREAMBLE} ${sql}" > "$tmpfile"

        start_ns=$(date +%s%N)
        df_stdout=$("$DATAFUSION_CLI" < "$tmpfile" 2>"${DF_RAW_DIR}/q${q}_stderr.txt") || true
        end_ns=$(date +%s%N)
        elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
        elapsed_s=$(python3 -c "print(round(${elapsed_ms} / 1000.0, 2))")

        # Save raw output
        echo "$df_stdout" > "${DF_RAW_DIR}/q${q}.txt"
        rm -f "$tmpfile"

        stderr_content=$(cat "${DF_RAW_DIR}/q${q}_stderr.txt" 2>/dev/null) || stderr_content=""

        if echo "$df_stdout" "$stderr_content" | grep -qi "error\|panic\|out of memory" && ! echo "$df_stdout" | grep -q "^|"; then
            errMsg=$(echo "$stderr_content" | head -1 | cut -c1-80)
            if is_expected_failure "$q"; then
                echo "  Q${q}: ERROR (${elapsed_s}s) [expected] -- ${errMsg}"
            else
                echo "  Q${q}: ERROR (${elapsed_s}s) -- ${errMsg}"
            fi
            df_fail=$((df_fail + 1))
        elif [ -z "$df_stdout" ] || ! echo "$df_stdout" | grep -q "^|"; then
            echo "  Q${q}: ERROR (${elapsed_s}s) -- no output"
            df_fail=$((df_fail + 1))
        else
            # Count data rows (pipe lines minus header)
            row_count=$(echo "$df_stdout" | grep -c "^|") || true
            row_count=${row_count:-0}
            # Subtract header lines (at least 1 header row per table block)
            # For the last block: header row = 1
            row_count=$((row_count > 1 ? row_count - 1 : 0))
            echo "  Q${q}: OK (${elapsed_s}s, rows~=${row_count})"
            df_pass=$((df_pass + 1))
        fi
    done

    echo ""
    echo "  DataFusion: ${df_pass} OK, ${df_fail} failed/error, ${df_skip} skipped"
    echo ""
fi

# ── Phase 3: Compare results ─────────────────────────────────────────────────
echo "--- Phase 3: Comparing results ---"
echo ""

python3 "${SCRIPT_DIR}/compare_results.py" batch \
    --os-dir "${OS_RAW_DIR}" \
    --df-dir "${DF_RAW_DIR}" \
    --max-rows "${MAX_ROWS}" \
    --num-queries 43

exit_code=$?

echo ""
echo "============================================================"
echo "  Results saved to: ${OUTPUT_DIR}/"
echo "  OS raw responses: ${OS_RAW_DIR}/"
echo "  DF raw responses: ${DF_RAW_DIR}/"
echo "============================================================"

exit $exit_code
