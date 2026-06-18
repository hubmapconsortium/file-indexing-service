#!/bin/bash
########################################################################################################
# Launches two _reindex operations (public and consortium) and monitors them until completion.
# Reports elapsed time for each and prints a README-ready timing summary.
#
# Usage: ./reindex_and_monitor.sh
#
# Requires: curl, python3
# Expects ES_URL to be set in environment or edit the default below.
########################################################################################################

ES_URL="${ES_URL:-https://search-hubmap-dev-test-hfnqv4ylo5ywvc42vwnyptbup4.us-east-1.es.amazonaws.com}"
POLL_INTERVAL=60  # seconds between status checks

CONSORTIUM_DEST="hm_consortium_files"
PUBLIC_DEST="hm_public_files"

CONSORTIUM_EXPECTED=10903886
PUBLIC_EXPECTED=9933524

echo "=== Reindex Monitor ==="
echo "ES URL: ${ES_URL}"
echo "Poll interval: ${POLL_INTERVAL}s"
echo

# ---------------------------------------------------------------------------
# Launch consortium reindex
# ---------------------------------------------------------------------------
echo "Launching consortium reindex at $(date)..."
CONSORTIUM_START=$(date +%s)
CONSORTIUM_START_HUMAN=$(date)

curl -s -X POST "${ES_URL}/${CONSORTIUM_DEST}/_delete_by_query?conflicts=proceed" \
    -H "Content-Type: application/json" \
    -d '{"query":{"match_all":{}}}' > /dev/null 2>&1

curl -s -o /dev/null -X POST "${ES_URL}/_reindex" \
    -H "Content-Type: application/json" \
    -d '{
        "source": {
            "index": [
                "hm_consortium_files_0-3",
                "hm_consortium_files_4-7",
                "hm_consortium_files_8-b",
                "hm_consortium_files_c-f"
            ]
        },
        "dest": {
            "index": "'"${CONSORTIUM_DEST}"'"
        }
    }' &
CONSORTIUM_CURL_PID=$!
echo "Consortium reindex launched (curl pid ${CONSORTIUM_CURL_PID})"
echo

# ---------------------------------------------------------------------------
# Launch public reindex
# ---------------------------------------------------------------------------
echo "Launching public reindex at $(date)..."
PUBLIC_START=$(date +%s)
PUBLIC_START_HUMAN=$(date)

curl -s -o /dev/null -X POST "${ES_URL}/_reindex" \
    -H "Content-Type: application/json" \
    -d '{
        "source": {
            "index": [
                "hm_public_files_0-3",
                "hm_public_files_4-7",
                "hm_public_files_8-b",
                "hm_public_files_c-f"
            ]
        },
        "dest": {
            "index": "'"${PUBLIC_DEST}"'"
        }
    }' &
PUBLIC_CURL_PID=$!
echo "Public reindex launched (curl pid ${PUBLIC_CURL_PID})"
echo

# ---------------------------------------------------------------------------
# Monitor loop
# ---------------------------------------------------------------------------
CONSORTIUM_DONE=0
PUBLIC_DONE=0
CONSORTIUM_END=0
PUBLIC_END=0

echo "Monitoring... (Ctrl-C to stop, reindexes will continue on server)"
echo "----------------------------------------------------------------------"

while [ $CONSORTIUM_DONE -eq 0 ] || [ $PUBLIC_DONE -eq 0 ]; do
    sleep $POLL_INTERVAL
    NOW=$(date +%s)
    NOW_HUMAN=$(date)

    # Get task info
    TASKS=$(curl -s "${ES_URL}/_tasks?actions=*reindex&detailed=true")

    # Get current counts
    CONSORTIUM_COUNT=$(curl -s "${ES_URL}/${CONSORTIUM_DEST}/_count" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null || echo 0)
    PUBLIC_COUNT=$(curl -s "${ES_URL}/${PUBLIC_DEST}/_count" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null || echo 0)

    # Count active reindex tasks
    ACTIVE_TASKS=$(echo "$TASKS" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    tasks = []
    for node in d.get('nodes', {}).values():
        for tid, t in node.get('tasks', {}).items():
            if 'reindex' in t.get('action', ''):
                desc = t.get('description', '')
                status = t.get('status', {})
                elapsed = round(t.get('running_time_in_nanos', 0) / 1e9)
                tasks.append(f'  Task {tid}: {status.get(\"created\",0)+status.get(\"updated\",0):,} docs, {elapsed}s elapsed | {desc[:80]}')
    if tasks:
        print('\n'.join(tasks))
    else:
        print('  No active reindex tasks')
except Exception as e:
    print(f'  Error parsing tasks: {e}')
" 2>/dev/null)

    echo "${NOW_HUMAN}"
    echo "Active tasks:"
    echo "$ACTIVE_TASKS"
    echo "  ${CONSORTIUM_DEST}: ${CONSORTIUM_COUNT:-(unknown)} docs"
    echo "  ${PUBLIC_DEST}: ${PUBLIC_COUNT:-(unknown)} docs"
    echo

    # Check consortium completion
    if [ $CONSORTIUM_DONE -eq 0 ]; then
        if [ "${CONSORTIUM_COUNT}" -ge "${CONSORTIUM_EXPECTED}" ] 2>/dev/null; then
            CONSORTIUM_DONE=1
            CONSORTIUM_END=$NOW
            CONSORTIUM_END_HUMAN=$(date)
            CONSORTIUM_ELAPSED=$(( (CONSORTIUM_END - CONSORTIUM_START) / 60 ))
            echo "*** CONSORTIUM REINDEX COMPLETE at ${CONSORTIUM_END_HUMAN}"
            echo "    ${CONSORTIUM_COUNT} docs in ${CONSORTIUM_ELAPSED} minutes"
            echo
        fi
    fi

    # Check public completion
    if [ $PUBLIC_DONE -eq 0 ]; then
        if [ "${PUBLIC_COUNT}" -ge "${PUBLIC_EXPECTED}" ] 2>/dev/null; then
            PUBLIC_DONE=1
            PUBLIC_END=$NOW
            PUBLIC_END_HUMAN=$(date)
            PUBLIC_ELAPSED=$(( (PUBLIC_END - PUBLIC_START) / 60 ))
            echo "*** PUBLIC REINDEX COMPLETE at ${PUBLIC_END_HUMAN}"
            echo "    ${PUBLIC_COUNT} docs in ${PUBLIC_ELAPSED} minutes"
            echo
        fi
    fi

    # Also stop if no active tasks remain and counts are nonzero
    if echo "$ACTIVE_TASKS" | grep -q "No active reindex tasks"; then
        if [ "${CONSORTIUM_COUNT}" -gt 0 ] && [ "${PUBLIC_COUNT}" -gt 0 ]; then
            if [ $CONSORTIUM_DONE -eq 0 ]; then
                CONSORTIUM_DONE=1
                CONSORTIUM_END=$NOW
                CONSORTIUM_ELAPSED=$(( (CONSORTIUM_END - CONSORTIUM_START) / 60 ))
                echo "*** CONSORTIUM REINDEX COMPLETE (no active tasks) at $(date)"
                echo "    ${CONSORTIUM_COUNT} docs in ${CONSORTIUM_ELAPSED} minutes"
            fi
            if [ $PUBLIC_DONE -eq 0 ]; then
                PUBLIC_DONE=1
                PUBLIC_END=$NOW
                PUBLIC_ELAPSED=$(( (PUBLIC_END - PUBLIC_START) / 60 ))
                echo "*** PUBLIC REINDEX COMPLETE (no active tasks) at $(date)"
                echo "    ${PUBLIC_COUNT} docs in ${PUBLIC_ELAPSED} minutes"
            fi
        fi
    fi
done

echo "======================================================================"
echo "=== REINDEX SUMMARY ==="
echo "======================================================================"
echo
echo "Consortium reindex:"
echo "  Started:  ${CONSORTIUM_START_HUMAN}"
echo "  Finished: ${CONSORTIUM_END_HUMAN}"
echo "  Elapsed:  ${CONSORTIUM_ELAPSED} minutes"
echo "  Documents: ${CONSORTIUM_COUNT}"
echo
echo "Public reindex:"
echo "  Started:  ${PUBLIC_START_HUMAN}"
echo "  Finished: ${PUBLIC_END_HUMAN}"
echo "  Elapsed:  ${PUBLIC_ELAPSED} minutes"
echo "  Documents: ${PUBLIC_COUNT}"
echo
TOTAL_ELAPSED=$(( (CONSORTIUM_ELAPSED > PUBLIC_ELAPSED ? CONSORTIUM_ELAPSED : PUBLIC_ELAPSED) ))
echo "Total wall-clock time (longer of the two): ${TOTAL_ELAPSED} minutes"
echo
echo "README line:"
echo "  Executing the QDSL _reindex to merge 4 partition indices into each"
echo "  production-ready index took approximately ${TOTAL_ELAPSED} minutes,"
echo "  running both reindexes concurrently."
PYEOF
