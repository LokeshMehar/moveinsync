#!/usr/bin/env bash
# Guided tour of the API with curl (also usable as copy-paste samples for Postman).  Needs the compose stack up.
source "$(dirname "$0")/lib.sh"
ADMIN_TOKEN=$(token acme-admin admin-pass)
EMP_TOKEN=$(token acme-emp emp-pass)
GLOBEX_TOKEN=$(token globex-admin admin-pass)
h() { echo; echo "### $*"; }

h "1. Admin of tenant 'acme' creates a recurring roster job: 22:00 every day in the tenant's own time zone"
CREATED=$(curl -s -X POST "$BASE/api/jobs" -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"nightly-roster","type":"ROSTER_GENERATION","cron":"0 0 22 * * *","timeZone":"Asia/Kolkata","misfirePolicy":"COALESCE"}')
echo "$CREATED"
JOB=$(echo "$CREATED" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

h "2. Next 3 fire times of that job"
curl -s "$BASE/api/jobs/$JOB/next-fires?count=3" -H "Authorization: Bearer $ADMIN_TOKEN"; echo

h "3. A DST-aware job in New York: what happens at 02:30 on the day clocks jump? (see docs/DESIGN.md §7)"
curl -s -X POST "$BASE/api/jobs" -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"dst-demo","type":"BILLING_CLOSE","cron":"0 30 2 * * *","timeZone":"America/New_York"}' | sed -n 's/.*"nextFireAt":"\([^"]*\)".*/next fire: \1/p'

h "4. Trigger the roster job manually, then look at its runs (one run, one effect)"
curl -s -X POST "$BASE/api/jobs/$JOB/trigger" -H "Authorization: Bearer $ADMIN_TOKEN"; echo
sleep 2
curl -s "$BASE/api/jobs/$JOB/runs" -H "Authorization: Bearer $ADMIN_TOKEN"; echo

h "5. Tenant isolation: globex's admin asks for acme's job -> 404 (not 403: existence is not revealed)"
curl -s -o /dev/null -w "HTTP %{http_code}\n" "$BASE/api/jobs/$JOB" -H "Authorization: Bearer $GLOBEX_TOKEN"

h "6. RBAC: an employee can read but not create -> 403"
curl -s -o /dev/null -w "GET  HTTP %{http_code}\n" "$BASE/api/jobs/$JOB" -H "Authorization: Bearer $EMP_TOKEN"
curl -s -o /dev/null -w "POST HTTP %{http_code}\n" -X POST "$BASE/api/jobs" -H "Authorization: Bearer $EMP_TOKEN" -H 'Content-Type: application/json' -d '{"name":"x","type":"BILLING_CLOSE"}'

h "7. A failing job: retries with backoff, then the dead-letter list, then replay"
FLAKY=$(curl -s -X POST "$BASE/api/jobs" -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"always-fails","type":"FLAKY","payload":{"failTimes":100},"maxAttempts":3}' | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
sleep 12
curl -s "$BASE/api/dead-letters" -H "Authorization: Bearer $ADMIN_TOKEN"; echo
RUN=$(curl -s "$BASE/api/dead-letters" -H "Authorization: Bearer $ADMIN_TOKEN" | sed -n 's/.*"id":\([0-9]*\).*/\1/p' | head -1)
echo "replay run $RUN:"; curl -s -o /dev/null -w "HTTP %{http_code}\n" -X POST "$BASE/api/dead-letters/$RUN/replay" -H "Authorization: Bearer $ADMIN_TOKEN"

h "8. Pause / resume every job of the tenant"
curl -s -X POST "$BASE/api/tenants/acme/pause"  -H "Authorization: Bearer $ADMIN_TOKEN"; echo
curl -s -X POST "$BASE/api/tenants/acme/resume" -H "Authorization: Bearer $ADMIN_TOKEN"; echo

h "9. What the on-call sees (operator): cluster summary and key metrics"
curl -s "$BASE/api/ops/summary" -H "Authorization: Bearer $OPS_TOKEN"; echo
curl -s "$BASE/actuator/prometheus" -H "Authorization: Bearer $OPS_TOKEN" | grep -E '^scheduler_(runs_running|retrying_size|deadletter_size|queue_oldest_due_ms|fencing_rejections_total|leases_takeovers_total|partitions_owned)' | head -12
