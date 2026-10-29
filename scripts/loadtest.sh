#!/usr/bin/env bash
# Load test: COUNT jobs become due uniformly inside WINDOW seconds, across 3 nodes. Reports fire lag from the DB record.
# Defaults reproduce the case-study target: 100,000 jobs in a 10-minute window.
source "$(dirname "$0")/lib.sh"
COUNT="${COUNT:-100000}"
WINDOW="${WINDOW:-600}"
TENANTS="${TENANTS:-20}"
WORK_MS="${WORK_MS:-5}"
START_DELAY="${START_DELAY:-20}"

echo "seeding $COUNT jobs over ${WINDOW}s (tenants=$TENANTS, work=${WORK_MS}ms, first due in ${START_DELAY}s)"
SEED=$(api POST "/api/bench/seed?count=$COUNT&windowSeconds=$WINDOW&tenants=$TENANTS&workMillis=$WORK_MS&startDelaySeconds=$START_DELAY")
echo "$SEED"
BATCH=$(echo "$SEED" | sed -n 's/.*"batch":"\([^"]*\)".*/\1/p')
[[ -n "$BATCH" ]] || fail "seeding failed"

DEADLINE=$(( $(date +%s) + START_DELAY + WINDOW + 180 ))
while :; do
  sleep 15
  PROGRESS=$(sql "SELECT status || '=' || count(*) FROM job_run WHERE payload->>'bench'='$BATCH' GROUP BY status ORDER BY 1" | tr '\n' ' ')
  echo "[$(date +%H:%M:%S)] $PROGRESS"
  [[ "$(sql "SELECT count(*) FROM job_run WHERE payload->>'bench'='$BATCH' AND status IN ('PENDING','RUNNING')")" == 0 ]] && break
  (( $(date +%s) > DEADLINE )) && fail "load test did not finish in time"
done

REPORT=$(api GET "/api/bench/report?batch=$BATCH")
echo "$REPORT"
mkdir -p bench-results && echo "$REPORT" > "bench-results/load-$BATCH.json"
P99=$(echo "$REPORT" | sed -n 's/.*"lag_p99_ms":\([0-9.]*\).*/\1/p')
awk -v p="$P99" 'BEGIN { exit !(p < 2000) }' && pass "p99 fire lag ${P99} ms < 2000 ms" || fail "p99 fire lag ${P99} ms >= 2000 ms"
