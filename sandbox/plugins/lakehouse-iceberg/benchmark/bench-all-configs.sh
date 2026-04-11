#!/bin/bash
# Comprehensive benchmark: runs all 43 ClickBench queries across multiple memory pool configs.
# Tracks time, process RSS, and DataFusion memory pool usage per query.
#
# Usage: ./bench-all-configs.sh [phase]
#   No args  — run all phases
#   1        — datafusion-cli baseline only
#   2        — Greedy 32GB only
#   3        — Greedy unlimited only
#   4        — FairSpill 32GB only
#   report   — generate comparison report from existing results

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results/comparison"
REPO_ROOT="/local/home/reddyvam/opensource/OpenSearch/.claude/worktrees/analytics-dwh-engine"
mkdir -p "$RESULTS_DIR"

TABLE="${TABLE:-hits_s3}"
ENDPOINT="${ENDPOINT:-http://localhost:9200}"
TIMEOUT="${TIMEOUT:-120}"

# ── Queries ──────────────────────────────────────────────────────────────────
# 43 ClickBench queries — OpenSearch version (lowercase cols, DATE literals, CHAR_LENGTH)
read_os_queries() {
    OS_QUERIES=()
    OS_QUERIES+=("SELECT COUNT(*) FROM ${TABLE}")
    OS_QUERIES+=("SELECT COUNT(*) FROM ${TABLE} WHERE advengineid <> 0")
    OS_QUERIES+=("SELECT SUM(advengineid), COUNT(*), AVG(resolutionwidth) FROM ${TABLE}")
    OS_QUERIES+=("SELECT AVG(userid) FROM ${TABLE}")
    OS_QUERIES+=("SELECT COUNT(DISTINCT userid) FROM ${TABLE}")
    OS_QUERIES+=("SELECT COUNT(DISTINCT searchphrase) FROM ${TABLE}")
    OS_QUERIES+=("SELECT MIN(eventdate), MAX(eventdate) FROM ${TABLE}")
    OS_QUERIES+=("SELECT advengineid, COUNT(*) AS c FROM ${TABLE} WHERE advengineid <> 0 GROUP BY advengineid ORDER BY c DESC")
    OS_QUERIES+=("SELECT regionid, COUNT(DISTINCT userid) AS u FROM ${TABLE} GROUP BY regionid ORDER BY u DESC LIMIT 10")
    OS_QUERIES+=("SELECT regionid, SUM(advengineid), COUNT(*) AS c, AVG(resolutionwidth), COUNT(DISTINCT userid) FROM ${TABLE} GROUP BY regionid ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT mobilephonemodel, COUNT(DISTINCT userid) AS u FROM ${TABLE} WHERE mobilephonemodel <> '' GROUP BY mobilephonemodel ORDER BY u DESC LIMIT 10")
    OS_QUERIES+=("SELECT mobilephone, mobilephonemodel, COUNT(DISTINCT userid) AS u FROM ${TABLE} WHERE mobilephonemodel <> '' GROUP BY mobilephone, mobilephonemodel ORDER BY u DESC LIMIT 10")
    OS_QUERIES+=("SELECT searchphrase, COUNT(*) AS c FROM ${TABLE} WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT searchphrase, COUNT(DISTINCT userid) AS u FROM ${TABLE} WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY u DESC LIMIT 10")
    OS_QUERIES+=("SELECT searchengineid, searchphrase, COUNT(*) AS c FROM ${TABLE} WHERE searchphrase <> '' GROUP BY searchengineid, searchphrase ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT userid, COUNT(*) AS c FROM ${TABLE} GROUP BY userid ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT userid, searchphrase, COUNT(*) AS c FROM ${TABLE} GROUP BY userid, searchphrase ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT userid, searchphrase, COUNT(*) AS c FROM ${TABLE} GROUP BY userid, searchphrase LIMIT 10")
    OS_QUERIES+=("SELECT userid, EXTRACT(MINUTE FROM eventtime) AS m, searchphrase, COUNT(*) AS c FROM ${TABLE} GROUP BY userid, EXTRACT(MINUTE FROM eventtime), searchphrase ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT userid FROM ${TABLE} WHERE userid = 435090932899640449")
    OS_QUERIES+=("SELECT COUNT(*) FROM ${TABLE} WHERE url LIKE '%google%'")
    OS_QUERIES+=("SELECT searchphrase, MIN(url), COUNT(*) AS c FROM ${TABLE} WHERE url LIKE '%google%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT searchphrase, MIN(url), MIN(title), COUNT(*) AS c, COUNT(DISTINCT userid) FROM ${TABLE} WHERE title LIKE '%Google%' AND url NOT LIKE '%.google.%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT * FROM ${TABLE} WHERE url LIKE '%google%' ORDER BY eventtime LIMIT 10")
    OS_QUERIES+=("SELECT searchphrase FROM ${TABLE} WHERE searchphrase <> '' ORDER BY eventtime LIMIT 10")
    OS_QUERIES+=("SELECT searchphrase FROM ${TABLE} WHERE searchphrase <> '' ORDER BY searchphrase LIMIT 10")
    OS_QUERIES+=("SELECT searchphrase FROM ${TABLE} WHERE searchphrase <> '' ORDER BY eventtime, searchphrase LIMIT 10")
    OS_QUERIES+=("SELECT counterid, AVG(CHAR_LENGTH(url)) AS l, COUNT(*) AS c FROM ${TABLE} WHERE url <> '' GROUP BY counterid HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")
    # Q29: referer domain extraction (complex SUBSTRING)
    OS_QUERIES+=("SELECT SUBSTRING(referer FROM POSITION('://' IN referer) + 3 FOR CASE WHEN POSITION('/' IN SUBSTRING(referer FROM POSITION('://' IN referer) + 3)) > 0 THEN POSITION('/' IN SUBSTRING(referer FROM POSITION('://' IN referer) + 3)) - 1 ELSE CHAR_LENGTH(referer) END) AS k, AVG(CHAR_LENGTH(referer)) AS l, COUNT(*) AS c, MIN(referer) FROM ${TABLE} WHERE referer <> '' GROUP BY SUBSTRING(referer FROM POSITION('://' IN referer) + 3 FOR CASE WHEN POSITION('/' IN SUBSTRING(referer FROM POSITION('://' IN referer) + 3)) > 0 THEN POSITION('/' IN SUBSTRING(referer FROM POSITION('://' IN referer) + 3)) - 1 ELSE CHAR_LENGTH(referer) END) HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")
    OS_QUERIES+=("SELECT SUM(resolutionwidth), SUM(resolutionwidth + 1), SUM(resolutionwidth + 2), SUM(resolutionwidth + 3), SUM(resolutionwidth + 4), SUM(resolutionwidth + 5), SUM(resolutionwidth + 6), SUM(resolutionwidth + 7), SUM(resolutionwidth + 8), SUM(resolutionwidth + 9), SUM(resolutionwidth + 10), SUM(resolutionwidth + 11), SUM(resolutionwidth + 12), SUM(resolutionwidth + 13), SUM(resolutionwidth + 14), SUM(resolutionwidth + 15), SUM(resolutionwidth + 16), SUM(resolutionwidth + 17), SUM(resolutionwidth + 18), SUM(resolutionwidth + 19), SUM(resolutionwidth + 20), SUM(resolutionwidth + 21), SUM(resolutionwidth + 22), SUM(resolutionwidth + 23), SUM(resolutionwidth + 24), SUM(resolutionwidth + 25), SUM(resolutionwidth + 26), SUM(resolutionwidth + 27), SUM(resolutionwidth + 28), SUM(resolutionwidth + 29), SUM(resolutionwidth + 30), SUM(resolutionwidth + 31), SUM(resolutionwidth + 32), SUM(resolutionwidth + 33), SUM(resolutionwidth + 34), SUM(resolutionwidth + 35), SUM(resolutionwidth + 36), SUM(resolutionwidth + 37), SUM(resolutionwidth + 38), SUM(resolutionwidth + 39), SUM(resolutionwidth + 40), SUM(resolutionwidth + 41), SUM(resolutionwidth + 42), SUM(resolutionwidth + 43), SUM(resolutionwidth + 44), SUM(resolutionwidth + 45), SUM(resolutionwidth + 46), SUM(resolutionwidth + 47), SUM(resolutionwidth + 48), SUM(resolutionwidth + 49), SUM(resolutionwidth + 50), SUM(resolutionwidth + 51), SUM(resolutionwidth + 52), SUM(resolutionwidth + 53), SUM(resolutionwidth + 54), SUM(resolutionwidth + 55), SUM(resolutionwidth + 56), SUM(resolutionwidth + 57), SUM(resolutionwidth + 58), SUM(resolutionwidth + 59), SUM(resolutionwidth + 60), SUM(resolutionwidth + 61), SUM(resolutionwidth + 62), SUM(resolutionwidth + 63), SUM(resolutionwidth + 64), SUM(resolutionwidth + 65), SUM(resolutionwidth + 66), SUM(resolutionwidth + 67), SUM(resolutionwidth + 68), SUM(resolutionwidth + 69), SUM(resolutionwidth + 70), SUM(resolutionwidth + 71), SUM(resolutionwidth + 72), SUM(resolutionwidth + 73), SUM(resolutionwidth + 74), SUM(resolutionwidth + 75), SUM(resolutionwidth + 76), SUM(resolutionwidth + 77), SUM(resolutionwidth + 78), SUM(resolutionwidth + 79), SUM(resolutionwidth + 80), SUM(resolutionwidth + 81), SUM(resolutionwidth + 82), SUM(resolutionwidth + 83), SUM(resolutionwidth + 84), SUM(resolutionwidth + 85), SUM(resolutionwidth + 86), SUM(resolutionwidth + 87), SUM(resolutionwidth + 88), SUM(resolutionwidth + 89) FROM ${TABLE}")
    OS_QUERIES+=("SELECT searchengineid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM ${TABLE} WHERE searchphrase <> '' GROUP BY searchengineid, clientip ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM ${TABLE} WHERE searchphrase <> '' GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM ${TABLE} GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT url, COUNT(*) AS c FROM ${TABLE} GROUP BY url ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT 1 AS \"one\", url, COUNT(*) AS c FROM ${TABLE} GROUP BY url ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT clientip, clientip - 1, clientip - 2, clientip - 3, COUNT(*) AS c FROM ${TABLE} GROUP BY clientip, clientip - 1, clientip - 2, clientip - 3 ORDER BY c DESC LIMIT 10")
    OS_QUERIES+=("SELECT url, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND dontcounthits = 0 AND isrefresh = 0 AND url <> '' GROUP BY url ORDER BY PageViews DESC LIMIT 10")
    OS_QUERIES+=("SELECT title, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND dontcounthits = 0 AND isrefresh = 0 AND title <> '' GROUP BY title ORDER BY PageViews DESC LIMIT 10")
    OS_QUERIES+=("SELECT url, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 AND islink <> 0 AND isdownload = 0 GROUP BY url ORDER BY PageViews DESC LIMIT 10")
    OS_QUERIES+=("SELECT traficsourceid, searchengineid, advengineid, CASE WHEN (searchengineid = 0 AND advengineid = 0) THEN referer ELSE '' END AS Src, url AS Dst, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 GROUP BY traficsourceid, searchengineid, advengineid, CASE WHEN (searchengineid = 0 AND advengineid = 0) THEN referer ELSE '' END, url ORDER BY PageViews DESC LIMIT 10")
    OS_QUERIES+=("SELECT urlhash, eventdate, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 AND traficsourceid IN (-1, 6) AND refererhash = 3594120000172545465 GROUP BY urlhash, eventdate ORDER BY PageViews DESC LIMIT 10")
    OS_QUERIES+=("SELECT windowclientwidth, windowclientheight, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 AND dontcounthits = 0 AND urlhash = 2868770270353813622 GROUP BY windowclientwidth, windowclientheight ORDER BY PageViews DESC LIMIT 10")
    OS_QUERIES+=("SELECT FLOOR(eventtime TO MINUTE) AS M, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= DATE '2013-07-15' AND eventdate <= DATE '2013-07-16' AND isrefresh = 0 AND dontcounthits = 0 GROUP BY FLOOR(eventtime TO MINUTE) ORDER BY M LIMIT 10")
}

# 43 ClickBench queries — datafusion-cli version (int eventdate, length(), extract(), DATE_TRUNC)
read_df_queries() {
    DF_QUERIES=()
    DF_QUERIES+=("SELECT COUNT(*) FROM ${TABLE}")
    DF_QUERIES+=("SELECT COUNT(*) FROM ${TABLE} WHERE advengineid <> 0")
    DF_QUERIES+=("SELECT SUM(advengineid), COUNT(*), AVG(resolutionwidth) FROM ${TABLE}")
    DF_QUERIES+=("SELECT AVG(userid) FROM ${TABLE}")
    DF_QUERIES+=("SELECT COUNT(DISTINCT userid) FROM ${TABLE}")
    DF_QUERIES+=("SELECT COUNT(DISTINCT searchphrase) FROM ${TABLE}")
    DF_QUERIES+=("SELECT MIN(eventdate), MAX(eventdate) FROM ${TABLE}")
    DF_QUERIES+=("SELECT advengineid, COUNT(*) AS c FROM ${TABLE} WHERE advengineid <> 0 GROUP BY advengineid ORDER BY c DESC")
    DF_QUERIES+=("SELECT regionid, COUNT(DISTINCT userid) AS u FROM ${TABLE} GROUP BY regionid ORDER BY u DESC LIMIT 10")
    DF_QUERIES+=("SELECT regionid, SUM(advengineid), COUNT(*) AS c, AVG(resolutionwidth), COUNT(DISTINCT userid) FROM ${TABLE} GROUP BY regionid ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT mobilephonemodel, COUNT(DISTINCT userid) AS u FROM ${TABLE} WHERE mobilephonemodel <> '' GROUP BY mobilephonemodel ORDER BY u DESC LIMIT 10")
    DF_QUERIES+=("SELECT mobilephone, mobilephonemodel, COUNT(DISTINCT userid) AS u FROM ${TABLE} WHERE mobilephonemodel <> '' GROUP BY mobilephone, mobilephonemodel ORDER BY u DESC LIMIT 10")
    DF_QUERIES+=("SELECT searchphrase, COUNT(*) AS c FROM ${TABLE} WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT searchphrase, COUNT(DISTINCT userid) AS u FROM ${TABLE} WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY u DESC LIMIT 10")
    DF_QUERIES+=("SELECT searchengineid, searchphrase, COUNT(*) AS c FROM ${TABLE} WHERE searchphrase <> '' GROUP BY searchengineid, searchphrase ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT userid, COUNT(*) AS c FROM ${TABLE} GROUP BY userid ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT userid, searchphrase, COUNT(*) AS c FROM ${TABLE} GROUP BY userid, searchphrase ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT userid, searchphrase, COUNT(*) AS c FROM ${TABLE} GROUP BY userid, searchphrase LIMIT 10")
    DF_QUERIES+=("SELECT userid, extract(minute FROM to_timestamp_seconds(eventtime)) AS m, searchphrase, COUNT(*) AS c FROM ${TABLE} GROUP BY userid, m, searchphrase ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT userid FROM ${TABLE} WHERE userid = 435090932899640449")
    DF_QUERIES+=("SELECT COUNT(*) FROM ${TABLE} WHERE url LIKE '%google%'")
    DF_QUERIES+=("SELECT searchphrase, MIN(url), COUNT(*) AS c FROM ${TABLE} WHERE url LIKE '%google%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT searchphrase, MIN(url), MIN(title), COUNT(*) AS c, COUNT(DISTINCT userid) FROM ${TABLE} WHERE title LIKE '%Google%' AND url NOT LIKE '%.google.%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT * FROM ${TABLE} WHERE url LIKE '%google%' ORDER BY eventtime LIMIT 10")
    DF_QUERIES+=("SELECT searchphrase FROM ${TABLE} WHERE searchphrase <> '' ORDER BY eventtime LIMIT 10")
    DF_QUERIES+=("SELECT searchphrase FROM ${TABLE} WHERE searchphrase <> '' ORDER BY searchphrase LIMIT 10")
    DF_QUERIES+=("SELECT searchphrase FROM ${TABLE} WHERE searchphrase <> '' ORDER BY eventtime, searchphrase LIMIT 10")
    DF_QUERIES+=("SELECT counterid, AVG(length(url)) AS l, COUNT(*) AS c FROM ${TABLE} WHERE url <> '' GROUP BY counterid HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")
    DF_QUERIES+=("SELECT REGEXP_REPLACE(referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(length(referer)) AS l, COUNT(*) AS c, MIN(referer) FROM ${TABLE} WHERE referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")
    DF_QUERIES+=("SELECT SUM(resolutionwidth), SUM(resolutionwidth + 1), SUM(resolutionwidth + 2), SUM(resolutionwidth + 3), SUM(resolutionwidth + 4), SUM(resolutionwidth + 5), SUM(resolutionwidth + 6), SUM(resolutionwidth + 7), SUM(resolutionwidth + 8), SUM(resolutionwidth + 9), SUM(resolutionwidth + 10), SUM(resolutionwidth + 11), SUM(resolutionwidth + 12), SUM(resolutionwidth + 13), SUM(resolutionwidth + 14), SUM(resolutionwidth + 15), SUM(resolutionwidth + 16), SUM(resolutionwidth + 17), SUM(resolutionwidth + 18), SUM(resolutionwidth + 19), SUM(resolutionwidth + 20), SUM(resolutionwidth + 21), SUM(resolutionwidth + 22), SUM(resolutionwidth + 23), SUM(resolutionwidth + 24), SUM(resolutionwidth + 25), SUM(resolutionwidth + 26), SUM(resolutionwidth + 27), SUM(resolutionwidth + 28), SUM(resolutionwidth + 29), SUM(resolutionwidth + 30), SUM(resolutionwidth + 31), SUM(resolutionwidth + 32), SUM(resolutionwidth + 33), SUM(resolutionwidth + 34), SUM(resolutionwidth + 35), SUM(resolutionwidth + 36), SUM(resolutionwidth + 37), SUM(resolutionwidth + 38), SUM(resolutionwidth + 39), SUM(resolutionwidth + 40), SUM(resolutionwidth + 41), SUM(resolutionwidth + 42), SUM(resolutionwidth + 43), SUM(resolutionwidth + 44), SUM(resolutionwidth + 45), SUM(resolutionwidth + 46), SUM(resolutionwidth + 47), SUM(resolutionwidth + 48), SUM(resolutionwidth + 49), SUM(resolutionwidth + 50), SUM(resolutionwidth + 51), SUM(resolutionwidth + 52), SUM(resolutionwidth + 53), SUM(resolutionwidth + 54), SUM(resolutionwidth + 55), SUM(resolutionwidth + 56), SUM(resolutionwidth + 57), SUM(resolutionwidth + 58), SUM(resolutionwidth + 59), SUM(resolutionwidth + 60), SUM(resolutionwidth + 61), SUM(resolutionwidth + 62), SUM(resolutionwidth + 63), SUM(resolutionwidth + 64), SUM(resolutionwidth + 65), SUM(resolutionwidth + 66), SUM(resolutionwidth + 67), SUM(resolutionwidth + 68), SUM(resolutionwidth + 69), SUM(resolutionwidth + 70), SUM(resolutionwidth + 71), SUM(resolutionwidth + 72), SUM(resolutionwidth + 73), SUM(resolutionwidth + 74), SUM(resolutionwidth + 75), SUM(resolutionwidth + 76), SUM(resolutionwidth + 77), SUM(resolutionwidth + 78), SUM(resolutionwidth + 79), SUM(resolutionwidth + 80), SUM(resolutionwidth + 81), SUM(resolutionwidth + 82), SUM(resolutionwidth + 83), SUM(resolutionwidth + 84), SUM(resolutionwidth + 85), SUM(resolutionwidth + 86), SUM(resolutionwidth + 87), SUM(resolutionwidth + 88), SUM(resolutionwidth + 89) FROM ${TABLE}")
    DF_QUERIES+=("SELECT searchengineid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM ${TABLE} WHERE searchphrase <> '' GROUP BY searchengineid, clientip ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM ${TABLE} WHERE searchphrase <> '' GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM ${TABLE} GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT url, COUNT(*) AS c FROM ${TABLE} GROUP BY url ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT 1, url, COUNT(*) AS c FROM ${TABLE} GROUP BY 1, url ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT clientip, clientip - 1, clientip - 2, clientip - 3, COUNT(*) AS c FROM ${TABLE} GROUP BY clientip, clientip - 1, clientip - 2, clientip - 3 ORDER BY c DESC LIMIT 10")
    DF_QUERIES+=("SELECT url, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= 15887 AND eventdate <= 15917 AND dontcounthits = 0 AND isrefresh = 0 AND url <> '' GROUP BY url ORDER BY PageViews DESC LIMIT 10")
    DF_QUERIES+=("SELECT title, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= 15887 AND eventdate <= 15917 AND dontcounthits = 0 AND isrefresh = 0 AND title <> '' GROUP BY title ORDER BY PageViews DESC LIMIT 10")
    DF_QUERIES+=("SELECT url, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= 15887 AND eventdate <= 15917 AND isrefresh = 0 AND islink <> 0 AND isdownload = 0 GROUP BY url ORDER BY PageViews DESC LIMIT 10")
    DF_QUERIES+=("SELECT traficsourceid, searchengineid, advengineid, CASE WHEN (searchengineid = 0 AND advengineid = 0) THEN referer ELSE '' END AS Src, url AS Dst, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= 15887 AND eventdate <= 15917 AND isrefresh = 0 GROUP BY traficsourceid, searchengineid, advengineid, Src, Dst ORDER BY PageViews DESC LIMIT 10")
    DF_QUERIES+=("SELECT urlhash, eventdate, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= 15887 AND eventdate <= 15917 AND isrefresh = 0 AND traficsourceid IN (-1, 6) AND refererhash = 3594120000172545465 GROUP BY urlhash, eventdate ORDER BY PageViews DESC LIMIT 10")
    DF_QUERIES+=("SELECT windowclientwidth, windowclientheight, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= 15887 AND eventdate <= 15917 AND isrefresh = 0 AND dontcounthits = 0 AND urlhash = 2868770270353813622 GROUP BY windowclientwidth, windowclientheight ORDER BY PageViews DESC LIMIT 10")
    DF_QUERIES+=("SELECT DATE_TRUNC('minute', to_timestamp_seconds(eventtime)) AS M, COUNT(*) AS PageViews FROM ${TABLE} WHERE counterid = 62 AND eventdate >= 15900 AND eventdate <= 15901 AND isrefresh = 0 AND dontcounthits = 0 GROUP BY DATE_TRUNC('minute', to_timestamp_seconds(eventtime)) ORDER BY M LIMIT 10")
}

# ── Helpers ──────────────────────────────────────────────────────────────────
get_process_rss_mb() {
    local pid
    pid=$(pgrep -f 'org.opensearch.bootstrap' | head -1) || true
    if [ -n "$pid" ]; then
        awk '/VmRSS/{print int($2/1024)}' "/proc/$pid/status" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Safe JSON payload generation via python3 — handles all quoting edge cases
make_json_payload() {
    python3 -c "import json,sys; print(json.dumps({'query': sys.stdin.read().strip()}))" <<< "$1"
}

# Re-register table to refresh AWS credentials (STS tokens expire)
refresh_table() {
    echo "    (refreshing table credentials...)"
    curl -s -X DELETE "${ENDPOINT}/${TABLE}" > /dev/null 2>&1 || true
    sleep 1
    curl -s -X PUT "${ENDPOINT}/${TABLE}" \
        -H 'Content-Type: application/json' \
        -d '{
          "settings": {
            "index.lakehouse.enabled": true,
            "index.lakehouse.type": "glue",
            "index.lakehouse.region": "us-west-2",
            "index.lakehouse.warehouse": "s3://iceberg-benchmark-test-263689514295/iceberg-warehouse",
            "index.lakehouse.namespace": "iceberg_benchmark_db",
            "index.lakehouse.table": "hits"
          }
        }' > /dev/null 2>&1
    sleep 2
}

# Save result row for a single query — also saves raw response for correctness
save_result_row() {
    local output_file="$1" qnum="$2" status="$3" elapsed_s="$4"
    local rss_before="${5:-0}" rss_after="${6:-0}" row_count="${7:-0}"
    echo "${qnum},${elapsed_s},${status},${rss_before},${rss_after},${row_count}" >> "$output_file"
}

# ── OpenSearch query runner ──────────────────────────────────────────────────
run_one_config() {
    local config_name="$1"
    local output_file="${RESULTS_DIR}/${config_name}.csv"
    local raw_dir="${RESULTS_DIR}/${config_name}_raw"
    mkdir -p "$raw_dir"

    echo "=== Running config: ${config_name} ==="
    echo "query,time_s,status,rss_before_mb,rss_after_mb,row_count" > "$output_file"

    read_os_queries
    local num_queries=${#OS_QUERIES[@]}

    # Warmup
    local payload
    payload=$(make_json_payload "SELECT COUNT(*) FROM ${TABLE}")
    curl -s -m 60 -X POST "${ENDPOINT}/_lakehouse/sql" \
        -H "Content-Type: application/json" \
        -d "$payload" > /dev/null 2>&1 || true
    sleep 2

    for ((q=0; q<num_queries; q++)); do
        local qnum=$((q + 1))
        local sql="${OS_QUERIES[$q]}"

        local rss_before
        rss_before=$(get_process_rss_mb)
        payload=$(make_json_payload "$sql")

        local start_ns end_ns elapsed_ms elapsed_s
        start_ns=$(date +%s%N)

        local result
        result=$(curl -s -m "$TIMEOUT" -X POST "${ENDPOINT}/_lakehouse/sql" \
            -H "Content-Type: application/json" \
            -d "$payload" 2>&1) || true
        local curl_exit=$?

        end_ns=$(date +%s%N)
        elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
        elapsed_s=$(python3 -c "print(round(${elapsed_ms} / 1000.0, 3))")

        local rss_after
        rss_after=$(get_process_rss_mb)

        # Save raw response for correctness analysis
        echo "$result" > "${raw_dir}/q${qnum}.json"

        # Count rows in response
        local row_count=0
        if [ -n "$result" ] && ! echo "$result" | grep -q '"error"'; then
            row_count=$(echo "$result" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    rows = d.get('datarows', d.get('rows', []))
    print(len(rows))
except:
    print(0)
" 2>/dev/null) || row_count=0
        fi

        if [ "$curl_exit" -eq 28 ] || [ -z "$result" ]; then
            echo "  Q${qnum}: TIMEOUT (${elapsed_s}s) RSS: ${rss_before}→${rss_after} MB"
            save_result_row "$output_file" "$qnum" "TIMEOUT" "$elapsed_s" "$rss_before" "$rss_after" 0
            # Refresh table after timeout (STS token may have expired)
            refresh_table
        elif echo "$result" | grep -q '"error"'; then
            local error
            error=$(echo "$result" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    e = d.get('error', {})
    if isinstance(e, dict):
        print(e.get('reason', str(e))[:100])
    else:
        print(str(e)[:100])
except:
    print('parse error')
" 2>/dev/null) || error="unknown"
            echo "  Q${qnum}: FAIL (${elapsed_s}s) — ${error}"
            save_result_row "$output_file" "$qnum" "FAIL" "$elapsed_s" "$rss_before" "$rss_after" 0
            # Refresh if token expired
            if echo "$result" | grep -q "token.*expired\|expired.*token"; then
                refresh_table
            fi
        else
            echo "  Q${qnum}: ${elapsed_s}s  rows=${row_count}  RSS: ${rss_before}→${rss_after} MB"
            save_result_row "$output_file" "$qnum" "OK" "$elapsed_s" "$rss_before" "$rss_after" "$row_count"
        fi
    done

    # Summary
    python3 -c "
import csv
with open('${output_file}') as f:
    rows = list(csv.DictReader(f))
ok = [r for r in rows if r['status'] == 'OK']
fail = [r for r in rows if r['status'] == 'FAIL']
to = [r for r in rows if r['status'] == 'TIMEOUT']
total_time = sum(float(r['time_s']) for r in ok)
print(f'  Summary [${config_name}]: {len(ok)}/{len(rows)} OK, {len(fail)} FAIL, {len(to)} TIMEOUT, total={total_time:.1f}s')
"
}

# ── datafusion-cli baseline ─────────────────────────────────────────────────
run_datafusion_baseline() {
    local output_file="${RESULTS_DIR}/datafusion_cli.csv"
    local raw_dir="${RESULTS_DIR}/datafusion_cli_raw"
    mkdir -p "$raw_dir"

    echo "=== Running datafusion-cli baseline ==="

    # Set AWS credentials
    export AWS_ACCESS_KEY_ID
    export AWS_SECRET_ACCESS_KEY
    export AWS_SESSION_TOKEN
    export AWS_DEFAULT_REGION=us-west-2
    AWS_ACCESS_KEY_ID=$(AWS_PROFILE=default aws configure get aws_access_key_id)
    AWS_SECRET_ACCESS_KEY=$(AWS_PROFILE=default aws configure get aws_secret_access_key)
    AWS_SESSION_TOKEN=$(AWS_PROFILE=default aws configure get aws_session_token 2>/dev/null || echo "")

    local S3_LOCATION="s3://iceberg-benchmark-test-263689514295/iceberg-warehouse/hits/data/"
    local PREAMBLE="SET datafusion.execution.listing_table_ignore_subdirectory = false;"
    PREAMBLE+=" CREATE EXTERNAL TABLE ${TABLE} STORED AS PARQUET LOCATION '${S3_LOCATION}' OPTIONS ('binary_as_string' 'true');"

    read_df_queries
    local num_queries=${#DF_QUERIES[@]}

    echo "query,time_s,status,peak_rss_mb,row_count" > "$output_file"

    for ((q=0; q<num_queries; q++)); do
        local qnum=$((q + 1))
        local sql="${DF_QUERIES[$q]}"

        # Write SQL to temp file to avoid shell quoting issues
        local tmpfile
        tmpfile=$(mktemp /tmp/df_query_XXXXX.sql)
        echo "${PREAMBLE} ${sql};" > "$tmpfile"

        local start_ns end_ns elapsed_ms elapsed_s
        start_ns=$(date +%s%N)

        # Run datafusion-cli, capture stdout+stderr separately
        local df_stdout df_stderr df_exit
        df_stdout=$(datafusion-cli < "$tmpfile" 2>"${raw_dir}/q${qnum}_stderr.txt") || true
        df_exit=$?

        end_ns=$(date +%s%N)
        elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
        elapsed_s=$(python3 -c "print(round(${elapsed_ms} / 1000.0, 3))")

        # Save raw output
        echo "$df_stdout" > "${raw_dir}/q${qnum}.txt"

        # Count result rows (lines starting with |, minus header/separator rows)
        local row_count
        row_count=$(echo "$df_stdout" | grep -c "^|") || true
        row_count=${row_count:-0}
        row_count=$((row_count > 4 ? row_count - 4 : 0))

        # Check RSS of datafusion-cli process (already finished, so use 0)
        local peak_rss=0

        rm -f "$tmpfile"

        local stderr_content
        stderr_content=$(cat "${raw_dir}/q${qnum}_stderr.txt" 2>/dev/null) || stderr_content=""

        if echo "$df_stdout" "$stderr_content" | grep -qi "error\|panic\|out of memory"; then
            # Check if it's just a benign "0 rows" message vs actual error
            if echo "$df_stdout" | grep -q "^|"; then
                echo "  Q${qnum}: ${elapsed_s}s  rows=${row_count}"
                echo "${qnum},${elapsed_s},OK,${peak_rss},${row_count}" >> "$output_file"
            else
                local errMsg
                errMsg=$(echo "$stderr_content" | head -1 | cut -c1-100)
                echo "  Q${qnum}: FAIL (${elapsed_s}s) — ${errMsg}"
                echo "${qnum},${elapsed_s},FAIL,${peak_rss},0" >> "$output_file"
            fi
        elif [ -z "$df_stdout" ] || ! echo "$df_stdout" | grep -q "^|"; then
            echo "  Q${qnum}: FAIL (${elapsed_s}s) — no output"
            echo "${qnum},${elapsed_s},FAIL,${peak_rss},0" >> "$output_file"
        else
            echo "  Q${qnum}: ${elapsed_s}s  rows=${row_count}"
            echo "${qnum},${elapsed_s},OK,${peak_rss},${row_count}" >> "$output_file"
        fi
    done

    # Summary
    python3 -c "
import csv
with open('${output_file}') as f:
    rows = list(csv.DictReader(f))
ok = [r for r in rows if r['status'] == 'OK']
fail = [r for r in rows if r['status'] == 'FAIL']
total_time = sum(float(r['time_s']) for r in ok)
print(f'  Summary [DATAFUSION-CLI]: {len(ok)}/{len(rows)} OK, {len(fail)} FAIL, total={total_time:.1f}s')
"
}

# ── OpenSearch lifecycle ─────────────────────────────────────────────────────
start_opensearch() {
    local pool_type="$1"
    local pool_limit_bytes="$2"
    local config_name="$3"
    local log_file="${RESULTS_DIR}/${config_name}_opensearch.log"

    echo "Starting OpenSearch [${config_name}]: pool_type=${pool_type}, limit=${pool_limit_bytes}..."

    # Kill any existing OpenSearch
    pkill -f 'org.opensearch.bootstrap' 2>/dev/null || true
    sleep 3
    # Kill gradle daemons holding port
    pkill -f 'GradleDaemon' 2>/dev/null || true
    sleep 5

    # Verify port is free
    if curl -s -m 2 http://localhost:9200 > /dev/null 2>&1; then
        echo "WARNING: Port 9200 still in use, waiting 15s..."
        sleep 15
    fi

    cd "$REPO_ROOT"
    OPENSEARCH_JAVA_OPTS="-Ddatafusion_memory_pool_type=${pool_type} -Ddatafusion_memory_pool_limit_bytes=${pool_limit_bytes} -Xms4g -Xmx4g" \
        ./gradlew run -PinstalledPlugins='["analytics-engine","analytics-backend-datafusion","lakehouse-iceberg","dsl-query-executor"]' \
        > "$log_file" 2>&1 &
    local gradle_pid=$!
    echo "Gradle PID: ${gradle_pid}"

    # Wait for startup (up to 180s)
    echo "Waiting for OpenSearch..."
    for i in $(seq 1 180); do
        if curl -s -m 2 http://localhost:9200 > /dev/null 2>&1; then
            echo "OpenSearch started after ${i}s"
            sleep 3

            # Register S3 Iceberg table
            curl -s -X PUT "http://localhost:9200/${TABLE}" \
                -H 'Content-Type: application/json' \
                -d '{
                  "settings": {
                    "index.lakehouse.enabled": true,
                    "index.lakehouse.type": "glue",
                    "index.lakehouse.region": "us-west-2",
                    "index.lakehouse.warehouse": "s3://iceberg-benchmark-test-263689514295/iceberg-warehouse",
                    "index.lakehouse.namespace": "iceberg_benchmark_db",
                    "index.lakehouse.table": "hits"
                  }
                }' 2>/dev/null
            echo ""
            sleep 3

            # Warmup — first query is always slow (loads Iceberg metadata + S3 connection)
            echo "Running warmup query..."
            local warmup_payload
            warmup_payload=$(make_json_payload "SELECT COUNT(*) FROM ${TABLE}")
            curl -s -m 120 -X POST "http://localhost:9200/_lakehouse/sql" \
                -H "Content-Type: application/json" \
                -d "$warmup_payload" > /dev/null 2>&1 || true
            echo "Warmup complete"
            return 0
        fi
        if ! kill -0 "$gradle_pid" 2>/dev/null; then
            echo "ERROR: Gradle process died. Check $log_file"
            return 1
        fi
        sleep 1
    done
    echo "ERROR: OpenSearch failed to start within 180s. Check $log_file"
    return 1
}

stop_opensearch() {
    echo "Stopping OpenSearch..."
    pkill -f 'org.opensearch.bootstrap' 2>/dev/null || true
    sleep 5
    pkill -f 'GradleDaemon' 2>/dev/null || true
    sleep 5
    echo "OpenSearch stopped"
}

# ── Report generation ────────────────────────────────────────────────────────
generate_report() {
    echo ">>> Generating comparison report..."
    python3 << 'PYEOF'
import csv, json, os, glob

results_dir = os.path.expandvars("${RESULTS_DIR}") if "${RESULTS_DIR}" != "" else "results/comparison"
PYEOF

    # Use a separate Python script to avoid variable expansion issues
    python3 "${SCRIPT_DIR}/generate_report.py" "$RESULTS_DIR"
}

# ── Main ─────────────────────────────────────────────────────────────────────
PHASE="${1:-all}"

echo "============================================"
echo "ClickBench Comprehensive Benchmark"
echo "Date: $(date)"
echo "Machine: $(uname -n) ($(nproc) CPUs, $(free -g | awk '/Mem/{print $2}')GB RAM)"
echo "Phase: ${PHASE}"
echo "Results: ${RESULTS_DIR}"
echo "============================================"
echo ""

case "$PHASE" in
    1|df|datafusion)
        run_datafusion_baseline
        ;;
    2|greedy32)
        start_opensearch "greedy" "34359738368" "greedy_32gb"
        run_one_config "greedy_32gb"
        stop_opensearch
        ;;
    3|unlimited)
        start_opensearch "greedy" "0" "greedy_unlimited"
        run_one_config "greedy_unlimited"
        stop_opensearch
        ;;
    4|fairspill)
        start_opensearch "fair_spill" "34359738368" "fairspill_32gb"
        run_one_config "fairspill_32gb"
        stop_opensearch
        ;;
    report)
        python3 "${SCRIPT_DIR}/generate_report.py" "$RESULTS_DIR"
        ;;
    all)
        # Phase 1: datafusion-cli baseline
        echo ">>> Phase 1/4: datafusion-cli baseline"
        run_datafusion_baseline
        echo ""

        # Phase 2: Greedy 32GB
        echo ">>> Phase 2/4: GreedyMemoryPool 32GB"
        start_opensearch "greedy" "34359738368" "greedy_32gb"
        run_one_config "greedy_32gb"
        stop_opensearch
        echo ""

        # Phase 3: Greedy unlimited
        echo ">>> Phase 3/4: GreedyMemoryPool unlimited"
        start_opensearch "greedy" "0" "greedy_unlimited"
        run_one_config "greedy_unlimited"
        stop_opensearch
        echo ""

        # Phase 4: FairSpill 32GB
        echo ">>> Phase 4/4: FairSpillPool 32GB"
        start_opensearch "fair_spill" "34359738368" "fairspill_32gb"
        run_one_config "fairspill_32gb"
        stop_opensearch
        echo ""

        # Report
        echo ">>> Generating comparison report..."
        python3 "${SCRIPT_DIR}/generate_report.py" "$RESULTS_DIR"
        ;;
    *)
        echo "Usage: $0 [all|1|2|3|4|report|df|greedy32|unlimited|fairspill]"
        exit 1
        ;;
esac

echo ""
echo "============================================"
echo "Benchmark complete at $(date)"
echo "Results in: ${RESULTS_DIR}/"
echo "============================================"
