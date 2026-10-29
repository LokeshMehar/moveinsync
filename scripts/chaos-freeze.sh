#!/usr/bin/env bash
# SIGSTOP test: a node is frozen (docker pause = cgroup freezer, the same as a very long GC pause) for longer than
# its lease. Another node finishes the work. When the frozen node resumes, its stale commits must be rejected.
source "$(dirname "$0")/lib.sh"
VICTIM="${VICTIM:-sched-node3}"
VICTIM_NODE="${VICTIM#sched-}"
VICTIM_PORT="${VICTIM_PORT:-8083}"
FREEZE_SECONDS="${FREEZE_SECONDS:-45}"
TENANT="chaos-freeze-$RANDOM"
# never leave the victim frozen if an assertion or timeout aborts the script
trap 'docker unpause "$VICTIM" > /dev/null 2>&1 || true' EXIT
JOBS=60

api POST /api/tenants "{\"id\":\"$TENANT\",\"maxConcurrency\":200}" > /dev/null
make_bulk_body /tmp/freeze-bulk.json $JOBS PICKUP_ALERT '"workMillis":25000'
api POST "/api/jobs/bulk?tenant=$TENANT" /tmp/freeze-bulk.json; echo

wait_for "victim has running work" 60 "SELECT count(*) > 0 FROM job_run WHERE tenant_id='$TENANT' AND status='RUNNING' AND lease_owner='$VICTIM_NODE'"
HELD=$(sql "SELECT count(*) FROM job_run WHERE tenant_id='$TENANT' AND status='RUNNING' AND lease_owner='$VICTIM_NODE'")
FENCED_BEFORE=$(metric "$VICTIM_PORT" 'scheduler_fencing_rejections_total')
echo ">>> docker pause $VICTIM for ${FREEZE_SECONDS}s (it holds $HELD leases of 15 s)"
docker pause "$VICTIM" > /dev/null
sleep "$FREEZE_SECONDS"

TAKEN=$(sql "SELECT count(*) FROM job_run WHERE tenant_id='$TENANT' AND attempts > 1")
echo "while frozen: $TAKEN runs were re-leased by other nodes"
echo ">>> docker unpause $VICTIM"
docker unpause "$VICTIM" > /dev/null

wait_for "all $JOBS jobs succeeded" 180 "SELECT count(*) = $JOBS FROM job_run WHERE tenant_id='$TENANT' AND status='SUCCEEDED'" || fail "jobs did not complete"
sleep 5
EFFECTS=$(sql "SELECT count(*) FROM effect WHERE tenant_id='$TENANT'")
DISTINCT=$(sql "SELECT count(DISTINCT idempotency_key) FROM effect WHERE tenant_id='$TENANT'")
FENCED_AFTER=$(metric "$VICTIM_PORT" 'scheduler_fencing_rejections_total')
echo "effects=$EFFECTS distinct_keys=$DISTINCT stale_commits_rejected_on_$VICTIM_NODE=$((FENCED_AFTER - FENCED_BEFORE))"

[[ "$EFFECTS" == "$JOBS" && "$DISTINCT" == "$JOBS" ]] || fail "expected exactly $JOBS effects"
(( TAKEN > 0 )) || fail "expected other nodes to take over the frozen node's runs"
(( FENCED_AFTER - FENCED_BEFORE > 0 )) || fail "expected the frozen node's stale commits to be rejected by the fencing token"
pass "frozen node's stale writes rejected by fencing token; $JOBS/$JOBS effects exactly once"
