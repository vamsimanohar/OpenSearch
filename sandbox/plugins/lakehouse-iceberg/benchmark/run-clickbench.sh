#!/bin/bash
# ClickBench benchmark runner for OpenSearch Lakehouse plugin.
# Runs all 43 ClickBench queries via POST _lakehouse/sql and records timing,
# status, and row counts.
#
# Prerequisites:
#   - OpenSearch running with lakehouse-iceberg plugin
#   - An Iceberg table registered (e.g. hits_s3)
#   - curl, python3 available on PATH
#
# Usage:
#   ./run-clickbench.sh [OPTIONS]
#
# Options:
#   --endpoint URL     OpenSearch endpoint (default: http://localhost:9200)
#   --table TABLE      Registered Iceberg table name (default: hits_s3)
#   --timeout SECS     Per-query timeout in seconds (default: 90)
#   --output DIR       Directory for results (default: results/run_<timestamp>)
#   --queries Q1,Q2    Comma-separated list of query numbers to run (default: all)
#   --help             Show this help message

set -euo pipefail

# -- Defaults -----------------------------------------------------------------
ENDPOINT="http://localhost:9200"
TABLE="hits_s3"
TIMEOUT=90
OUTPUT_DIR=""
QUERY_LIST=""

# -- Parse arguments ----------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --endpoint)   ENDPOINT="$2"; shift 2 ;;
        --table)      TABLE="$2"; shift 2 ;;
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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# -- Output directory ---------------------------------------------------------
if [ -z "$OUTPUT_DIR" ]; then
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    OUTPUT_DIR="${SCRIPT_DIR}/results/run_${TIMESTAMP}"
fi
mkdir -p "$OUTPUT_DIR"

RESULTS_JSON="${OUTPUT_DIR}/results.json"
RESULTS_CSV="${OUTPUT_DIR}/results.csv"

# -- Queries ------------------------------------------------------------------
# All 43 ClickBench queries adapted for OpenSearch Lakehouse SQL.
# Lowercase column names, DATE literals, CHAR_LENGTH(), no trailing semicolons.
# The placeholder __TABLE__ is replaced with the actual table name at runtime.
load_queries() {
    QUERIES=()
    # Q1
    QUERIES+=("SELECT COUNT(*) FROM __TABLE__")
    # Q2
    QUERIES+=("SELECT COUNT(*) FROM __TABLE__ WHERE advengineid <> 0")
    # Q3
    QUERIES+=("SELECT SUM(advengineid), COUNT(*), AVG(resolutionwidth) FROM __TABLE__")
    # Q4
    QUERIES+=("SELECT AVG(userid) FROM __TABLE__")
    # Q5
    QUERIES+=("SELECT COUNT(DISTINCT userid) FROM __TABLE__")
    # Q6
    QUERIES+=("SELECT COUNT(DISTINCT searchphrase) FROM __TABLE__")
    # Q7
    QUERIES+=("SELECT MIN(eventdate), MAX(eventdate) FROM __TABLE__")
    # Q8
    QUERIES+=("SELECT advengineid, COUNT(*) AS c FROM __TABLE__ WHERE advengineid <> 0 GROUP BY advengineid ORDER BY c DESC")
    # Q9
    QUERIES+=("SELECT regionid, COUNT(DISTINCT userid) AS u FROM __TABLE__ GROUP BY regionid ORDER BY u DESC LIMIT 10")
    # Q10
    QUERIES+=("SELECT regionid, SUM(advengineid), COUNT(*) AS c, AVG(resolutionwidth), COUNT(DISTINCT userid) FROM __TABLE__ GROUP BY regionid ORDER BY c DESC LIMIT 10")
    # Q11
    QUERIES+=("SELECT mobilephonemodel, COUNT(DISTINCT userid) AS u FROM __TABLE__ WHERE mobilephonemodel <> '' GROUP BY mobilephonemodel ORDER BY u DESC LIMIT 10")
    # Q12
    QUERIES+=("SELECT mobilephone, mobilephonemodel, COUNT(DISTINCT userid) AS u FROM __TABLE__ WHERE mobilephonemodel <> '' GROUP BY mobilephone, mobilephonemodel ORDER BY u DESC LIMIT 10")
    # Q13
    QUERIES+=("SELECT searchphrase, COUNT(*) AS c FROM __TABLE__ WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    # Q14
    QUERIES+=("SELECT searchphrase, COUNT(DISTINCT userid) AS u FROM __TABLE__ WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY u DESC LIMIT 10")
    # Q15
    QUERIES+=("SELECT searchengineid, searchphrase, COUNT(*) AS c FROM __TABLE__ WHERE searchphrase <> '' GROUP BY searchengineid, searchphrase ORDER BY c DESC LIMIT 10")
    # Q16
    QUERIES+=("SELECT userid, COUNT(*) AS c FROM __TABLE__ GROUP BY userid ORDER BY c DESC LIMIT 10")
    # Q17
    QUERIES+=("SELECT userid, searchphrase, COUNT(*) AS c FROM __TABLE__ GROUP BY userid, searchphrase ORDER BY c DESC LIMIT 10")
    # Q18
    QUERIES+=("SELECT userid, searchphrase, COUNT(*) AS c FROM __TABLE__ GROUP BY userid, searchphrase LIMIT 10")
    # Q19
    QUERIES+=("SELECT userid, EXTRACT(MINUTE FROM eventtime) AS m, searchphrase, COUNT(*) AS c FROM __TABLE__ GROUP BY userid, EXTRACT(MINUTE FROM eventtime), searchphrase ORDER BY c DESC LIMIT 10")
    # Q20
    QUERIES+=("SELECT userid FROM __TABLE__ WHERE userid = 435090932899640449")
    # Q21
    QUERIES+=("SELECT COUNT(*) FROM __TABLE__ WHERE url LIKE '%google%'")
    # Q22
    QUERIES+=("SELECT searchphrase, MIN(url), COUNT(*) AS c FROM __TABLE__ WHERE url LIKE '%google%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    # Q23
    QUERIES+=("SELECT searchphrase, MIN(url), MIN(title), COUNT(*) AS c, COUNT(DISTINCT userid) FROM __TABLE__ WHERE title LIKE '%Google%' AND url NOT LIKE '%.google.%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    # Q24
    QUERIES+=("SELECT * FROM __TABLE__ WHERE url LIKE '%google%' ORDER BY eventtime LIMIT 10")
    # Q25
    QUERIES+=("SELECT searchphrase FROM __TABLE__ WHERE searchphrase <> '' ORDER BY eventtime LIMIT 10")
    # Q26
    QUERIES+=("SELECT searchphrase FROM __TABLE__ WHERE searchphrase <> '' ORDER BY searchphrase LIMIT 10")
    # Q27
    QUERIES+=("SELECT searchphrase FROM __TABLE__ WHERE searchphrase <> '' ORDER BY eventtime, searchphrase LIMIT 10")
    # Q28
    QUERIES+=("SELECT counterid, AVG(CHAR_LENGTH(url)) AS l, COUNT(*) AS c FROM __TABLE__ WHERE url <> '' GROUP BY counterid HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")
    # Q29
    QUERIES+=("SELECT SUBSTRING(referer FROM POSITION('://' IN referer) + 3 FOR CASE WHEN POSITION('/' IN SUBSTRING(referer FROM POSITION('://' IN referer) + 3)) > 0 THEN POSITION('/' IN SUBSTRING(referer FROM POSITION('://' IN referer) + 3)) - 1 ELSE CHAR_LENGTH(referer) END) AS k, AVG(CHAR_LENGTH(referer)) AS l, COUNT(*) AS c, MIN(referer) FROM __TABLE__ WHERE referer <> '' GROUP BY SUBSTRING(referer FROM POSITION('://' IN referer) + 3 FOR CASE WHEN POSITION('/' IN SUBSTRING(referer FROM POSITION('://' IN referer) + 3)) > 0 THEN POSITION('/' IN SUBSTRING(referer FROM POSITION('://' IN referer) + 3)) - 1 ELSE CHAR_LENGTH(referer) END) HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")
    # Q30
    QUERIES+=("SELECT SUM(resolutionwidth), SUM(resolutionwidth + 1), SUM(resolutionwidth + 2), SUM(resolutionwidth + 3), SUM(resolutionwidth + 4), SUM(resolutionwidth + 5), SUM(resolutionwidth + 6), SUM(resolutionwidth + 7), SUM(resolutionwidth + 8), SUM(resolutionwidth + 9), SUM(resolutionwidth + 10), SUM(resolutionwidth + 11), SUM(resolutionwidth + 12), SUM(resolutionwidth + 13), SUM(resolutionwidth + 14), SUM(resolutionwidth + 15), SUM(resolutionwidth + 16), SUM(resolutionwidth + 17), SUM(resolutionwidth + 18), SUM(resolutionwidth + 19), SUM(resolutionwidth + 20), SUM(resolutionwidth + 21), SUM(resolutionwidth + 22), SUM(resolutionwidth + 23), SUM(resolutionwidth + 24), SUM(resolutionwidth + 25), SUM(resolutionwidth + 26), SUM(resolutionwidth + 27), SUM(resolutionwidth + 28), SUM(resolutionwidth + 29), SUM(resolutionwidth + 30), SUM(resolutionwidth + 31), SUM(resolutionwidth + 32), SUM(resolutionwidth + 33), SUM(resolutionwidth + 34), SUM(resolutionwidth + 35), SUM(resolutionwidth + 36), SUM(resolutionwidth + 37), SUM(resolutionwidth + 38), SUM(resolutionwidth + 39), SUM(resolutionwidth + 40), SUM(resolutionwidth + 41), SUM(resolutionwidth + 42), SUM(resolutionwidth + 43), SUM(resolutionwidth + 44), SUM(resolutionwidth + 45), SUM(resolutionwidth + 46), SUM(resolutionwidth + 47), SUM(resolutionwidth + 48), SUM(resolutionwidth + 49), SUM(resolutionwidth + 50), SUM(resolutionwidth + 51), SUM(resolutionwidth + 52), SUM(resolutionwidth + 53), SUM(resolutionwidth + 54), SUM(resolutionwidth + 55), SUM(resolutionwidth + 56), SUM(resolutionwidth + 57), SUM(resolutionwidth + 58), SUM(resolutionwidth + 59), SUM(resolutionwidth + 60), SUM(resolutionwidth + 61), SUM(resolutionwidth + 62), SUM(resolutionwidth + 63), SUM(resolutionwidth + 64), SUM(resolutionwidth + 65), SUM(resolutionwidth + 66), SUM(resolutionwidth + 67), SUM(resolutionwidth + 68), SUM(resolutionwidth + 69), SUM(resolutionwidth + 70), SUM(resolutionwidth + 71), SUM(resolutionwidth + 72), SUM(resolutionwidth + 73), SUM(resolutionwidth + 74), SUM(resolutionwidth + 75), SUM(resolutionwidth + 76), SUM(resolutionwidth + 77), SUM(resolutionwidth + 78), SUM(resolutionwidth + 79), SUM(resolutionwidth + 80), SUM(resolutionwidth + 81), SUM(resolutionwidth + 82), SUM(resolutionwidth + 83), SUM(resolutionwidth + 84), SUM(resolutionwidth + 85), SUM(resolutionwidth + 86), SUM(resolutionwidth + 87), SUM(resolutionwidth + 88), SUM(resolutionwidth + 89) FROM __TABLE__")
    # Q31
    QUERIES+=("SELECT searchengineid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM __TABLE__ WHERE searchphrase <> '' GROUP BY searchengineid, clientip ORDER BY c DESC LIMIT 10")
    # Q32
    QUERIES+=("SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM __TABLE__ WHERE searchphrase <> '' GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10")
    # Q33
    QUERIES+=("SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM __TABLE__ GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10")
    # Q34  (expected: TIMEOUT on 32GB nodes -- GROUP BY url has ~100M unique values)
    QUERIES+=("SELECT url, COUNT(*) AS c FROM __TABLE__ GROUP BY url ORDER BY c DESC LIMIT 10")
    # Q35  (expected: TIMEOUT on 32GB nodes -- same as Q34)
    QUERIES+=("SELECT 1 AS \"one\", url, COUNT(*) AS c FROM __TABLE__ GROUP BY url ORDER BY c DESC LIMIT 10")
    # Q36
    QUERIES+=("SELECT clientip, clientip - 1, clientip - 2, clientip - 3, COUNT(*) AS c FROM __TABLE__ GROUP BY clientip, clientip - 1, clientip - 2, clientip - 3 ORDER BY c DESC LIMIT 10")
    # Q37
    QUERIES+=("SELECT url, COUNT(*) AS PageViews FROM __TABLE__ WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND dontcounthits = 0 AND isrefresh = 0 AND url <> '' GROUP BY url ORDER BY PageViews DESC LIMIT 10")
    # Q38
    QUERIES+=("SELECT title, COUNT(*) AS PageViews FROM __TABLE__ WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND dontcounthits = 0 AND isrefresh = 0 AND title <> '' GROUP BY title ORDER BY PageViews DESC LIMIT 10")
    # Q39
    QUERIES+=("SELECT url, COUNT(*) AS PageViews FROM __TABLE__ WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 AND islink <> 0 AND isdownload = 0 GROUP BY url ORDER BY PageViews DESC LIMIT 10")
    # Q40
    QUERIES+=("SELECT traficsourceid, searchengineid, advengineid, CASE WHEN (searchengineid = 0 AND advengineid = 0) THEN referer ELSE '' END AS Src, url AS Dst, COUNT(*) AS PageViews FROM __TABLE__ WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 GROUP BY traficsourceid, searchengineid, advengineid, CASE WHEN (searchengineid = 0 AND advengineid = 0) THEN referer ELSE '' END, url ORDER BY PageViews DESC LIMIT 10")
    # Q41
    QUERIES+=("SELECT urlhash, eventdate, COUNT(*) AS PageViews FROM __TABLE__ WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 AND traficsourceid IN (-1, 6) AND refererhash = 3594120000172545465 GROUP BY urlhash, eventdate ORDER BY PageViews DESC LIMIT 10")
    # Q42
    QUERIES+=("SELECT windowclientwidth, windowclientheight, COUNT(*) AS PageViews FROM __TABLE__ WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 AND dontcounthits = 0 AND urlhash = 2868770270353813622 GROUP BY windowclientwidth, windowclientheight ORDER BY PageViews DESC LIMIT 10")
    # Q43
    QUERIES+=("SELECT FLOOR(eventtime TO MINUTE) AS M, COUNT(*) AS PageViews FROM __TABLE__ WHERE counterid = 62 AND eventdate >= DATE '2013-07-15' AND eventdate <= DATE '2013-07-16' AND isrefresh = 0 AND dontcounthits = 0 GROUP BY FLOOR(eventtime TO MINUTE) ORDER BY M LIMIT 10")

    # Replace __TABLE__ placeholder with actual table name
    for i in "${!QUERIES[@]}"; do
        QUERIES[$i]="${QUERIES[$i]//__TABLE__/$TABLE}"
    done
}

# -- Determine which queries to run -------------------------------------------
if [ -n "$QUERY_LIST" ]; then
    IFS=',' read -ra QUERY_NUMS <<< "$QUERY_LIST"
else
    QUERY_NUMS=($(seq 1 43))
fi

# -- Helpers ------------------------------------------------------------------
json_payload() {
    python3 -c "import json,sys; print(json.dumps({'query': sys.stdin.read().strip()}))" <<< "$1"
}

# -- Banner -------------------------------------------------------------------
echo "============================================"
echo " ClickBench -- OpenSearch Lakehouse"
echo "============================================"
echo "  Date:      $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Endpoint:  ${ENDPOINT}"
echo "  Table:     ${TABLE}"
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

# -- Check connectivity -------------------------------------------------------
echo "Checking OpenSearch connectivity..."
if ! curl -sf -m 5 "${ENDPOINT}" > /dev/null 2>&1; then
    echo "ERROR: Cannot reach OpenSearch at ${ENDPOINT}"
    exit 1
fi
echo "  OK"
echo ""

# -- Load queries -------------------------------------------------------------
load_queries

# -- Warmup -------------------------------------------------------------------
echo "Running warmup query (SELECT COUNT(*))..."
WARMUP_PAYLOAD=$(json_payload "SELECT COUNT(*) FROM ${TABLE}")
warmup_result=$(curl -s -m 60 -X POST "${ENDPOINT}/_lakehouse/sql" \
    -H "Content-Type: application/json" \
    -d "$WARMUP_PAYLOAD" 2>&1) || true
if echo "$warmup_result" | grep -q '"error"'; then
    echo "WARNING: Warmup query returned an error. Check table registration."
    echo "$warmup_result" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin); e=d.get('error',{})
    print('  ',e.get('reason',str(e))[:200] if isinstance(e,dict) else str(e)[:200])
except: pass
" 2>/dev/null || true
fi
echo "  Done"
echo ""

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
        continue
    fi

    sql="${QUERIES[$idx]}"
    payload=$(json_payload "$sql")

    start_ns=$(date +%s%N)
    result=$(curl -s -m "$TIMEOUT" -X POST "${ENDPOINT}/_lakehouse/sql" \
        -H "Content-Type: application/json" \
        -d "$payload" 2>&1) || true
    curl_exit=$?
    end_ns=$(date +%s%N)

    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    elapsed_s=$(python3 -c "print(round(${elapsed_ms}/1000.0, 3))")

    # Determine status, row count, error message
    row_count=0
    error_msg=""
    status=""

    if [ "$curl_exit" -eq 28 ] || [ -z "$result" ]; then
        status="TIMEOUT"
    elif echo "$result" | grep -q '"error"'; then
        status="ERROR"
        error_msg=$(echo "$result" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin); e=d.get('error',{})
    print(e.get('reason',str(e))[:120] if isinstance(e,dict) else str(e)[:120])
except: print('parse error')
" 2>/dev/null) || error_msg="unknown"
    else
        status="OK"
        row_count=$(echo "$result" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    print(len(d.get('datarows', d.get('rows', []))))
except: print(0)
" 2>/dev/null) || row_count=0
    fi

    # Save raw response
    echo "$result" > "${OUTPUT_DIR}/q$(printf '%02d' "$qnum").json"

    # Update counters
    case "$status" in
        OK)
            PASSED=$((PASSED + 1))
            TOTAL_TIME_OK=$(python3 -c "print(round(${TOTAL_TIME_OK} + ${elapsed_s}, 3))")
            printf "  Q%-3d  %9ss  %8s  %8s\n" "$qnum" "$elapsed_s" "OK" "$row_count"
            ;;
        ERROR)
            FAILED=$((FAILED + 1))
            printf "  Q%-3d  %9ss  %8s  %s\n" "$qnum" "$elapsed_s" "ERROR" "$error_msg"
            ;;
        TIMEOUT)
            TIMED_OUT=$((TIMED_OUT + 1))
            printf "  Q%-3d  %9ss  %8s\n" "$qnum" "$elapsed_s" "TIMEOUT"
            ;;
    esac

    echo "${qnum},${elapsed_s},${status},${row_count}" >> "$RESULTS_CSV"
done

# -- Write JSON results -------------------------------------------------------
python3 - "$RESULTS_JSON" "$ENDPOINT" "$TABLE" "$TIMEOUT" "$PASSED" "$FAILED" "$TIMED_OUT" "$TOTAL_TIME_OK" "${OUTPUT_DIR}" <<'PYEOF'
import json, sys, os
from datetime import datetime

results_path = sys.argv[1]
endpoint = sys.argv[2]
table = sys.argv[3]
timeout = int(sys.argv[4])
passed = int(sys.argv[5])
failed = int(sys.argv[6])
timed_out = int(sys.argv[7])
total_time_ok = float(sys.argv[8])
output_dir = sys.argv[9]

# Read CSV to build query results
queries = []
csv_path = os.path.join(output_dir, "results.csv")
with open(csv_path) as f:
    header = f.readline()
    for line in f:
        parts = line.strip().split(",", 3)
        if len(parts) >= 4:
            qnum, time_s, status, rows = parts
            # Load error from raw JSON if status is ERROR
            error = ""
            raw_path = os.path.join(output_dir, f"q{int(qnum):02d}.json")
            if status == "ERROR" and os.path.exists(raw_path):
                try:
                    with open(raw_path) as rf:
                        d = json.load(rf)
                        e = d.get("error", {})
                        error = e.get("reason", str(e))[:200] if isinstance(e, dict) else str(e)[:200]
                except:
                    pass
            queries.append({
                "query": int(qnum),
                "time_s": float(time_s),
                "status": status,
                "rows": int(rows),
                "error": error
            })

report = {
    "timestamp": datetime.now().isoformat(),
    "config": {
        "endpoint": endpoint,
        "table": table,
        "timeout_s": timeout
    },
    "summary": {
        "total": passed + failed + timed_out,
        "passed": passed,
        "failed": failed,
        "timed_out": timed_out,
        "total_time_ok_s": total_time_ok,
    },
    "queries": queries
}

with open(results_path, "w") as f:
    json.dump(report, f, indent=2)
PYEOF

# -- Summary table ------------------------------------------------------------
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
echo "  Results JSON:   ${RESULTS_JSON}"
echo "  Raw responses:  ${OUTPUT_DIR}/q*.json"
echo "============================================"
