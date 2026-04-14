#!/bin/bash
# ClickBench benchmark runner for DataFusion CLI.
# Runs all 43 ClickBench queries and records timing, status, and row counts.
# Uses the same dataset as run-clickbench.sh (OpenSearch) for comparison.
#
# Both S3 and local parquet data are supported.
# Default: S3 (same data OpenSearch reads via Iceberg).
#
# Prerequisites:
#   - datafusion-cli installed (PATH or ~/.cargo/bin/)
#   - S3 access (IMDS on EC2, or AWS_PROFILE=default on dev machine)
#     OR local parquet file/directory
#
# Usage:
#   ./run-clickbench-df.sh [OPTIONS]
#
# Options:
#   --data-path PATH   Parquet data (S3 path or local). Default: S3 ClickBench.
#   --timeout SECS     Per-query timeout in seconds (default: 180)
#   --output DIR       Directory for results (default: results/df_<timestamp>)
#   --queries Q1,Q2    Comma-separated list of query numbers to run (default: all)
#   --help             Show this help message

set -euo pipefail

# -- Defaults -----------------------------------------------------------------
DATA_PATH="s3://iceberg-benchmark-test-263689514295/iceberg-warehouse/hits/data/"
TIMEOUT=180
OUTPUT_DIR=""
QUERY_LIST=""
DATAFUSION_CLI=""

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# -- Parse arguments ----------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-path)  DATA_PATH="$2"; shift 2 ;;
        --timeout)    TIMEOUT="$2"; shift 2 ;;
        --output)     OUTPUT_DIR="$2"; shift 2 ;;
        --queries)    QUERY_LIST="$2"; shift 2 ;;
        --help|-h)
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "ERROR: Unknown option: $1"
            echo "Run with --help for usage."
            exit 1
            ;;
    esac
done

# -- Find datafusion-cli -----------------------------------------------------
if command -v datafusion-cli &>/dev/null; then
    DATAFUSION_CLI="$(command -v datafusion-cli)"
elif [ -x "$HOME/.cargo/bin/datafusion-cli" ]; then
    DATAFUSION_CLI="$HOME/.cargo/bin/datafusion-cli"
else
    echo "ERROR: datafusion-cli not found in PATH or ~/.cargo/bin/"
    echo "  Install with: cargo install datafusion-cli"
    exit 1
fi

# -- Output directory ---------------------------------------------------------
if [ -z "$OUTPUT_DIR" ]; then
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    OUTPUT_DIR="${SCRIPT_DIR}/results/df_${TIMESTAMP}"
fi
mkdir -p "$OUTPUT_DIR"

RESULTS_CSV="${OUTPUT_DIR}/results.csv"

# -- Queries ------------------------------------------------------------------
# All 43 ClickBench queries in DataFusion SQL dialect.
# Lowercase column names (matching the S3/Iceberg parquet schema).
# Uses length() instead of CHAR_LENGTH(), date strings instead of DATE literals,
# extract() for timestamp ops, DATE_TRUNC for floor-to-minute.
load_queries() {
    QUERIES=()
    # Q1
    QUERIES+=("SELECT COUNT(*) FROM hits")
    # Q2
    QUERIES+=("SELECT COUNT(*) FROM hits WHERE advengineid <> 0")
    # Q3
    QUERIES+=("SELECT SUM(advengineid), COUNT(*), AVG(resolutionwidth) FROM hits")
    # Q4
    QUERIES+=("SELECT AVG(userid) FROM hits")
    # Q5
    QUERIES+=("SELECT COUNT(DISTINCT userid) FROM hits")
    # Q6
    QUERIES+=("SELECT COUNT(DISTINCT searchphrase) FROM hits")
    # Q7
    QUERIES+=("SELECT MIN(eventdate), MAX(eventdate) FROM hits")
    # Q8
    QUERIES+=("SELECT advengineid, COUNT(*) AS c FROM hits WHERE advengineid <> 0 GROUP BY advengineid ORDER BY c DESC")
    # Q9
    QUERIES+=("SELECT regionid, COUNT(DISTINCT userid) AS u FROM hits GROUP BY regionid ORDER BY u DESC LIMIT 10")
    # Q10
    QUERIES+=("SELECT regionid, SUM(advengineid), COUNT(*) AS c, AVG(resolutionwidth), COUNT(DISTINCT userid) FROM hits GROUP BY regionid ORDER BY c DESC LIMIT 10")
    # Q11
    QUERIES+=("SELECT mobilephonemodel, COUNT(DISTINCT userid) AS u FROM hits WHERE mobilephonemodel <> '' GROUP BY mobilephonemodel ORDER BY u DESC LIMIT 10")
    # Q12
    QUERIES+=("SELECT mobilephone, mobilephonemodel, COUNT(DISTINCT userid) AS u FROM hits WHERE mobilephonemodel <> '' GROUP BY mobilephone, mobilephonemodel ORDER BY u DESC LIMIT 10")
    # Q13
    QUERIES+=("SELECT searchphrase, COUNT(*) AS c FROM hits WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    # Q14
    QUERIES+=("SELECT searchphrase, COUNT(DISTINCT userid) AS u FROM hits WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY u DESC LIMIT 10")
    # Q15
    QUERIES+=("SELECT searchengineid, searchphrase, COUNT(*) AS c FROM hits WHERE searchphrase <> '' GROUP BY searchengineid, searchphrase ORDER BY c DESC LIMIT 10")
    # Q16
    QUERIES+=("SELECT userid, COUNT(*) AS c FROM hits GROUP BY userid ORDER BY c DESC LIMIT 10")
    # Q17
    QUERIES+=("SELECT userid, searchphrase, COUNT(*) AS c FROM hits GROUP BY userid, searchphrase ORDER BY c DESC LIMIT 10")
    # Q18
    QUERIES+=("SELECT userid, searchphrase, COUNT(*) AS c FROM hits GROUP BY userid, searchphrase LIMIT 10")
    # Q19
    QUERIES+=("SELECT userid, extract(minute FROM eventtime) AS m, searchphrase, COUNT(*) AS c FROM hits GROUP BY userid, extract(minute FROM eventtime), searchphrase ORDER BY c DESC LIMIT 10")
    # Q20
    QUERIES+=("SELECT userid FROM hits WHERE userid = 435090932899640449")
    # Q21
    QUERIES+=("SELECT COUNT(*) FROM hits WHERE url LIKE '%google%'")
    # Q22
    QUERIES+=("SELECT searchphrase, MIN(url), COUNT(*) AS c FROM hits WHERE url LIKE '%google%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    # Q23
    QUERIES+=("SELECT searchphrase, MIN(url), MIN(title), COUNT(*) AS c, COUNT(DISTINCT userid) FROM hits WHERE title LIKE '%Google%' AND url NOT LIKE '%.google.%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    # Q24
    QUERIES+=("SELECT * FROM hits WHERE url LIKE '%google%' ORDER BY eventtime LIMIT 10")
    # Q25
    QUERIES+=("SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY eventtime LIMIT 10")
    # Q26
    QUERIES+=("SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY searchphrase LIMIT 10")
    # Q27
    QUERIES+=("SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY eventtime, searchphrase LIMIT 10")
    # Q28
    QUERIES+=("SELECT counterid, AVG(length(url)) AS l, COUNT(*) AS c FROM hits WHERE url <> '' GROUP BY counterid HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")
    # Q29
    QUERIES+=("SELECT REGEXP_REPLACE(referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(length(referer)) AS l, COUNT(*) AS c, MIN(referer) FROM hits WHERE referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")
    # Q30
    QUERIES+=("SELECT SUM(resolutionwidth), SUM(resolutionwidth + 1), SUM(resolutionwidth + 2), SUM(resolutionwidth + 3), SUM(resolutionwidth + 4), SUM(resolutionwidth + 5), SUM(resolutionwidth + 6), SUM(resolutionwidth + 7), SUM(resolutionwidth + 8), SUM(resolutionwidth + 9), SUM(resolutionwidth + 10), SUM(resolutionwidth + 11), SUM(resolutionwidth + 12), SUM(resolutionwidth + 13), SUM(resolutionwidth + 14), SUM(resolutionwidth + 15), SUM(resolutionwidth + 16), SUM(resolutionwidth + 17), SUM(resolutionwidth + 18), SUM(resolutionwidth + 19), SUM(resolutionwidth + 20), SUM(resolutionwidth + 21), SUM(resolutionwidth + 22), SUM(resolutionwidth + 23), SUM(resolutionwidth + 24), SUM(resolutionwidth + 25), SUM(resolutionwidth + 26), SUM(resolutionwidth + 27), SUM(resolutionwidth + 28), SUM(resolutionwidth + 29), SUM(resolutionwidth + 30), SUM(resolutionwidth + 31), SUM(resolutionwidth + 32), SUM(resolutionwidth + 33), SUM(resolutionwidth + 34), SUM(resolutionwidth + 35), SUM(resolutionwidth + 36), SUM(resolutionwidth + 37), SUM(resolutionwidth + 38), SUM(resolutionwidth + 39), SUM(resolutionwidth + 40), SUM(resolutionwidth + 41), SUM(resolutionwidth + 42), SUM(resolutionwidth + 43), SUM(resolutionwidth + 44), SUM(resolutionwidth + 45), SUM(resolutionwidth + 46), SUM(resolutionwidth + 47), SUM(resolutionwidth + 48), SUM(resolutionwidth + 49), SUM(resolutionwidth + 50), SUM(resolutionwidth + 51), SUM(resolutionwidth + 52), SUM(resolutionwidth + 53), SUM(resolutionwidth + 54), SUM(resolutionwidth + 55), SUM(resolutionwidth + 56), SUM(resolutionwidth + 57), SUM(resolutionwidth + 58), SUM(resolutionwidth + 59), SUM(resolutionwidth + 60), SUM(resolutionwidth + 61), SUM(resolutionwidth + 62), SUM(resolutionwidth + 63), SUM(resolutionwidth + 64), SUM(resolutionwidth + 65), SUM(resolutionwidth + 66), SUM(resolutionwidth + 67), SUM(resolutionwidth + 68), SUM(resolutionwidth + 69), SUM(resolutionwidth + 70), SUM(resolutionwidth + 71), SUM(resolutionwidth + 72), SUM(resolutionwidth + 73), SUM(resolutionwidth + 74), SUM(resolutionwidth + 75), SUM(resolutionwidth + 76), SUM(resolutionwidth + 77), SUM(resolutionwidth + 78), SUM(resolutionwidth + 79), SUM(resolutionwidth + 80), SUM(resolutionwidth + 81), SUM(resolutionwidth + 82), SUM(resolutionwidth + 83), SUM(resolutionwidth + 84), SUM(resolutionwidth + 85), SUM(resolutionwidth + 86), SUM(resolutionwidth + 87), SUM(resolutionwidth + 88), SUM(resolutionwidth + 89) FROM hits")
    # Q31
    QUERIES+=("SELECT searchengineid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits WHERE searchphrase <> '' GROUP BY searchengineid, clientip ORDER BY c DESC LIMIT 10")
    # Q32
    QUERIES+=("SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits WHERE searchphrase <> '' GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10")
    # Q33
    QUERIES+=("SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10")
    # Q34  (expected: OOM on 32GB nodes -- GROUP BY url has ~100M unique values)
    QUERIES+=("SELECT url, COUNT(*) AS c FROM hits GROUP BY url ORDER BY c DESC LIMIT 10")
    # Q35  (expected: OOM on 32GB nodes -- same as Q34)
    QUERIES+=("SELECT 1, url, COUNT(*) AS c FROM hits GROUP BY 1, url ORDER BY c DESC LIMIT 10")
    # Q36
    QUERIES+=("SELECT clientip, clientip - 1, clientip - 2, clientip - 3, COUNT(*) AS c FROM hits GROUP BY clientip, clientip - 1, clientip - 2, clientip - 3 ORDER BY c DESC LIMIT 10")
    # Q37
    QUERIES+=("SELECT url, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND eventdate >= '2013-07-01' AND eventdate <= '2013-07-31' AND dontcounthits = 0 AND isrefresh = 0 AND url <> '' GROUP BY url ORDER BY PageViews DESC LIMIT 10")
    # Q38
    QUERIES+=("SELECT title, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND eventdate >= '2013-07-01' AND eventdate <= '2013-07-31' AND dontcounthits = 0 AND isrefresh = 0 AND title <> '' GROUP BY title ORDER BY PageViews DESC LIMIT 10")
    # Q39
    QUERIES+=("SELECT url, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND eventdate >= '2013-07-01' AND eventdate <= '2013-07-31' AND isrefresh = 0 AND islink <> 0 AND isdownload = 0 GROUP BY url ORDER BY PageViews DESC LIMIT 10")
    # Q40
    QUERIES+=("SELECT traficsourceid, searchengineid, advengineid, CASE WHEN (searchengineid = 0 AND advengineid = 0) THEN referer ELSE '' END AS Src, url AS Dst, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND eventdate >= '2013-07-01' AND eventdate <= '2013-07-31' AND isrefresh = 0 GROUP BY traficsourceid, searchengineid, advengineid, CASE WHEN (searchengineid = 0 AND advengineid = 0) THEN referer ELSE '' END, url ORDER BY PageViews DESC LIMIT 10")
    # Q41
    QUERIES+=("SELECT urlhash, eventdate, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND eventdate >= '2013-07-01' AND eventdate <= '2013-07-31' AND isrefresh = 0 AND traficsourceid IN (-1, 6) AND refererhash = 3594120000172545465 GROUP BY urlhash, eventdate ORDER BY PageViews DESC LIMIT 10")
    # Q42
    QUERIES+=("SELECT windowclientwidth, windowclientheight, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND eventdate >= '2013-07-01' AND eventdate <= '2013-07-31' AND isrefresh = 0 AND dontcounthits = 0 AND urlhash = 2868770270353813622 GROUP BY windowclientwidth, windowclientheight ORDER BY PageViews DESC LIMIT 10")
    # Q43
    QUERIES+=("SELECT DATE_TRUNC('minute', eventtime) AS M, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND eventdate >= '2013-07-15' AND eventdate <= '2013-07-16' AND isrefresh = 0 AND dontcounthits = 0 GROUP BY DATE_TRUNC('minute', eventtime) ORDER BY M LIMIT 10")
}

# -- Determine which queries to run -------------------------------------------
if [ -n "$QUERY_LIST" ]; then
    IFS=',' read -ra QUERY_NUMS <<< "$QUERY_LIST"
else
    QUERY_NUMS=($(seq 1 43))
fi

# -- Preamble SQL (table setup) -----------------------------------------------
DF_PREAMBLE="SET datafusion.execution.listing_table_ignore_subdirectory = false;"
DF_PREAMBLE="${DF_PREAMBLE} CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '${DATA_PATH}' OPTIONS ('binary_as_string' 'true');"

# -- Banner -------------------------------------------------------------------
echo "============================================"
echo " ClickBench -- DataFusion CLI"
echo "============================================"
echo "  Date:      $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Data path: ${DATA_PATH}"
echo "  DF CLI:    ${DATAFUSION_CLI}"
echo "  Timeout:   ${TIMEOUT}s per query"
echo "  Queries:   ${QUERY_NUMS[*]}"
echo "  Output:    ${OUTPUT_DIR}"
MACHINE_INFO="$(uname -n)"
if command -v nproc &>/dev/null; then
    MACHINE_INFO+=" ($(nproc) vCPUs"
    if command -v free &>/dev/null; then
        MACHINE_INFO+=", $(free -g | awk '/Mem/{print $2}')GB RAM"
    fi
    MACHINE_INFO+=")"
fi
echo "  Machine:   ${MACHINE_INFO}"
echo "============================================"
echo ""

# -- Quick connectivity check -------------------------------------------------
echo "Checking data access..."
tmpcheck=$(mktemp /tmp/df_check_XXXXX.sql)
echo "${DF_PREAMBLE} SELECT COUNT(*) FROM hits LIMIT 1;" > "$tmpcheck"
check_result=$(timeout 30 "$DATAFUSION_CLI" < "$tmpcheck" 2>&1) || true
rm -f "$tmpcheck"
if ! echo "$check_result" | grep -q "^|"; then
    echo "ERROR: Cannot read data at ${DATA_PATH}"
    echo "$check_result" | tail -5
    exit 1
fi
echo "  OK"
echo ""

# -- Load queries -------------------------------------------------------------
load_queries

# -- CSV header ---------------------------------------------------------------
echo "query,time_s,status,rows" > "$RESULTS_CSV"

# -- Run queries --------------------------------------------------------------
PASSED=0
FAILED=0
TIMED_OUT=0
TOTAL_TIME_OK="0"

echo "Running ${#QUERY_NUMS[@]} queries..."
echo ""
printf "  %-4s  %10s  %8s  %8s\n" "Q#" "Time" "Status" "Rows"
printf "  %-4s  %10s  %8s  %8s\n" "----" "----------" "--------" "--------"

for qnum in "${QUERY_NUMS[@]}"; do
    idx=$((qnum - 1))
    if [ "$idx" -lt 0 ] || [ "$idx" -ge "${#QUERIES[@]}" ]; then
        printf "  Q%-3d  %10s  %8s  %8s\n" "$qnum" "-" "SKIP" "-"
        echo "${qnum},-,SKIP,0" >> "$RESULTS_CSV"
        continue
    fi

    sql="${QUERIES[$idx]}"

    # Write preamble + query to temp file
    tmpfile=$(mktemp /tmp/df_bench_XXXXX.sql)
    echo "${DF_PREAMBLE} ${sql};" > "$tmpfile"

    start_ns=$(date +%s%N)
    df_stdout=$(timeout "$TIMEOUT" "$DATAFUSION_CLI" < "$tmpfile" 2>"${OUTPUT_DIR}/q$(printf '%02d' "$qnum")_stderr.txt") || true
    exit_code=$?
    end_ns=$(date +%s%N)

    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    elapsed_s=$(python3 -c "print(round(${elapsed_ms}/1000.0, 3))")

    # Save raw output
    echo "$df_stdout" > "${OUTPUT_DIR}/q$(printf '%02d' "$qnum").txt"
    rm -f "$tmpfile"

    # Determine status
    row_count=0
    status=""

    if [ "$exit_code" -eq 124 ]; then
        status="TIMEOUT"
    elif echo "$df_stdout" | grep -qi "error\|panic\|out of memory" && ! echo "$df_stdout" | grep -q "^|"; then
        status="ERROR"
    elif [ -z "$df_stdout" ] || ! echo "$df_stdout" | grep -q "^|"; then
        status="ERROR"
    else
        status="OK"
        # Count data rows: pipe-delimited lines minus separator lines (+--)
        row_count=$(echo "$df_stdout" | grep "^|" | grep -cv "^+-" 2>/dev/null) || true
        # Subtract header row
        row_count=$((row_count > 1 ? row_count - 1 : row_count))
    fi

    # Update counters
    case "$status" in
        OK)
            PASSED=$((PASSED + 1))
            TOTAL_TIME_OK=$(python3 -c "print(round(${TOTAL_TIME_OK} + ${elapsed_s}, 3))")
            printf "  Q%-3d  %9ss  %8s  %8s\n" "$qnum" "$elapsed_s" "OK" "$row_count"
            ;;
        ERROR)
            FAILED=$((FAILED + 1))
            errMsg=$(cat "${OUTPUT_DIR}/q$(printf '%02d' "$qnum")_stderr.txt" 2>/dev/null | head -1 | cut -c1-60)
            printf "  Q%-3d  %9ss  %8s  %s\n" "$qnum" "$elapsed_s" "ERROR" "$errMsg"
            ;;
        TIMEOUT)
            TIMED_OUT=$((TIMED_OUT + 1))
            printf "  Q%-3d  %9ss  %8s\n" "$qnum" "$elapsed_s" "TIMEOUT"
            ;;
    esac

    echo "${qnum},${elapsed_s},${status},${row_count}" >> "$RESULTS_CSV"
done

# -- Summary ------------------------------------------------------------------
echo ""
echo "============================================"
echo " Summary"
echo "============================================"
TOTAL=$((PASSED + FAILED + TIMED_OUT))
echo "  Total queries:  ${TOTAL}"
echo "  Passed (OK):    ${PASSED}"
echo "  Errors:         ${FAILED}"
echo "  Timeouts:       ${TIMED_OUT}"
echo "  Total time (OK): ${TOTAL_TIME_OK}s"
echo ""
echo "  Results CSV:    ${RESULTS_CSV}"
echo "  Raw responses:  ${OUTPUT_DIR}/q*.txt"
echo "============================================"
