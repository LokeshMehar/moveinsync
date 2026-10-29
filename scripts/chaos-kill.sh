#!/usr/bin/env bash
# kill -9 test: 1,200 pickup alerts are being sent; one node is killed mid-flight.
# Expect: the remaining nodes take over within the lease time and every alert is sent exactly once.
source "$(dirname "$0")/lib.sh"
VICTIM="${VICTIM:-sched-node2}"
TENANT="chaos-kill-$RANDOM"
# a failed run must not leave the cluster a node short for the next one
trap 'docker start "$VICTIM" > /dev/null 2>&1 || true' EXIT

api POST /api/tenants "{\"id\":\"$TENANT\",\"maxConcurrency\":40}" > /dev/null
make_bulk_body /tmp/kill-bulk.json 1200 PICKUP_ALERT '"workMillis":1500'
api POST "/api/jobs/bulk?tenant=$TENANT" /tmp/kill-bulk.json; echo

echo "waiting until the victim is mid-flight..."
wait_for "victim has running work" 60 "SELECT count(*) >= 5 FROM job_run WHERE tenant_id='$TENANT' AND status='RUNNING' AND lease_owner = '${VICTIM#sched-}'"
DONE_BEFORE=$(sql "SELECT count(*) FROM job_run WHERE tenant_id='$TENANT' AND status='SUCCEEDED'")
IN_FLIGHT=$(sql "SELECT count(*) FROM job_run WHERE tenant_id='$TENANT' AND status='RUNNING' AND lease_owner = '${VICTIM#sched-}'")
echo ">>> docker kill $VICTIM  (done so far: $DONE_BEFORE, in flight on victim: $IN_FLIGHT)"
KILLED_AT=$(date +%s)
docker kill "$VICTIM" > /dev/null

wait_for "all 1200 alerts succeeded" 180 "SELECT count(*) = 1200 FROM job_run WHERE tenant_id='$TENANT' AND status='SUCCEEDED'" \
  || fail "not all alerts completed after the kill"
RECOVERED_IN=$(( $(date +%s) - KILLED_AT ))

EFFECTS=$(sql "SELECT count(*) FROM effect WHERE tenant_id='$TENANT'")
DISTINCT=$(sql "SELECT count(DISTINCT idempotency_key) FROM effect WHERE tenant_id='$TENANT'")
TAKEN=$(sql "SELECT count(*) FROM job_run WHERE tenant_id='$TENANT' AND attempts > 1")
echo "effects=$EFFECTS distinct_keys=$DISTINCT runs_taken_over=$TAKEN finished_${RECOVERED_IN}s_after_kill (lease=${LEASE:-15s})"

[[ "$EFFECTS" == 1200 && "$DISTINCT" == 1200 ]] || fail "expected exactly 1200 effects"
(( TAKEN > 0 )) || fail "expected the survivors to take over in-flight runs of the dead node"
pass "kill -9 of $VICTIM: 1200/1200 alerts sent exactly once; $TAKEN in-flight runs were taken over"

docker start "$VICTIM" > /dev/null && echo "restarted $VICTIM"
