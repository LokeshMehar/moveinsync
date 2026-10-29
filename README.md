# Trip job scheduler

Case Study 3 from the MoveInSync 2026 backend assessment: a scheduler for timed trip jobs (nightly rosters, pickup
alerts 30 minutes before pickup, no-show cancellation, daily billing close) that runs on three or more identical nodes.
Java 21, Spring Boot 3.3, PostgreSQL 16. Nothing else to install besides Docker.

The goal: no job is lost, no effect happens twice, a node that freezes past its lease can't write anything, and one
tenant's burst doesn't hold up another tenant. `docs/DESIGN.md` explains how, with diagrams and a failure table.

## Run it

```bash
docker compose up --build     # postgres + 3 nodes on :8081 :8082 :8083
```

Demo users (seeded from `application.yml`, local use only):

| user | password | role | tenant |
|---|---|---|---|
| operator | operator-pass | OPERATOR | all (pass `?tenant=`) |
| acme-admin | admin-pass | ADMIN | acme, Asia/Kolkata |
| acme-emp | emp-pass | EMPLOYEE, read only | acme |
| globex-admin | admin-pass | ADMIN | globex, America/New_York |

```bash
TOKEN=$(curl -s localhost:8081/auth/token -H 'Content-Type: application/json' \
        -d '{"username":"acme-admin","password":"admin-pass"}' | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')

curl -s localhost:8081/api/jobs -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"name":"nightly-roster","type":"ROSTER_GENERATION","cron":"0 0 22 * * *","timeZone":"Asia/Kolkata"}'
```

`bash scripts/demo.sh` runs through every endpoint with curl.

## API

| endpoint | who | what |
|---|---|---|
| `POST /auth/token` | anyone | log in, get a JWT |
| `POST /api/jobs`, `POST /api/jobs/bulk` | admin, operator | one-off (`runAt`) or recurring (`cron` + `timeZone`); bulk takes up to 5,000 one-offs |
| `GET /api/jobs`, `/{id}`, `/{id}/runs` | everyone, own tenant | read |
| `GET /api/jobs/{id}/next-fires?count=5` | everyone | upcoming fire times in the tenant's zone |
| `POST /api/jobs/{id}/trigger`, `DELETE /api/jobs/{id}` | admin, operator | run now, deactivate |
| `POST /api/tenants/{id}/pause` and `/resume` | admin (own tenant), operator | stop/start claiming for the tenant |
| `GET /api/dead-letters`, `POST /api/dead-letters/{runId}/replay` | admin, operator | look at and replay dead runs |
| `POST /api/tenants`, `PUT /api/tenants/{id}/concurrency` | operator | tenants and their concurrency cap |
| `GET /api/ops/summary`, `/actuator/prometheus` | operator | cluster state, metrics |
| `POST /api/bench/seed`, `GET /api/bench/report` | operator | load test helpers (`BENCH_ENABLED=true`, on in compose) |

Cron uses Spring's six fields (`sec min hour dom month dow`); a five-field expression gets `0` seconds added.
Job types: `PICKUP_ALERT`, `ROSTER_GENERATION`, `NO_SHOW_CANCEL`, `BILLING_CLOSE`, and `FLAKY` (fails on purpose, for retry demos).

## Tests

```bash
mvn test     # needs Docker: Testcontainers starts a real PostgreSQL. About 2 minutes.
```

50 tests, all passing:

* 18 unit tests with no database: cron and DST, backoff bounds and jitter, partition balance and how few partitions
  move when a node leaves or joins, the three misfire policies.
* 18 cluster tests, with several nodes in one JVM against real PostgreSQL: exactly-once with 6,000 jobs on 3 nodes,
  16 threads inserting the same occurrence, fencing (a direct store-level test and a whole frozen node), killing a node
  with 1,200 alerts in flight, retries, dead letter and replay, max run time, the tenant cap (sampled from the database
  while running), a 3,000-job tenant next to a 20-job tenant, pause/resume, misfires, graceful hand-over, replaying a
  dead run of a SKIP job, seeding that must not overwrite operator changes.
* 12 API tests: login, role checks, tenant isolation on each endpoint, error bodies that don't leak (including 404/405/415),
  operator-only metrics, delete cancelling queued runs, trigger returning its own run, create-only tenants.
* 2 config tests: the prod profile refuses the public default JWT secret.

## Chaos runs on the real containers

```bash
bash scripts/chaos-kill.sh      # docker kill a node while 1,200 alerts run
bash scripts/chaos-freeze.sh    # docker pause a node for 45 s (lease is 15 s), then unpause
```

Last run, from a fresh database:

```
kill:   40 in-flight runs taken over, 1200 effects, 1200 distinct keys, finished 34 s after the kill
freeze: 19 runs re-leased while frozen, 19 stale commits rejected on the frozen node, 60 effects for 60 jobs
```

## Benchmarks

Machine: Xeon w5-2455X (12 cores), 31 GB, Windows 11, Docker Desktop. Postgres and all three nodes run on that one
box, so a real multi-host setup should do better. Fire lag is first start minus scheduled time, read back from the
`job_run` table by `scripts/loadtest.sh`.

| run | result |
|---|---|
| 100,000 jobs spread over 10 minutes, 20 tenants | all succeeded, 0 duplicate effects, lag p50 151 ms, p95 422 ms, p99 627 ms, max 2.5 s |
| 30,000 jobs over 60 s (re-run on a fresh database) | all succeeded, 0 duplicates, p99 350 ms |
| 100,000 jobs all due inside 1 second | drained in 12.6 s (about 8,000 runs/s), 0 duplicates; lag p99 10.7 s because of the backlog |

The ceiling is the single PostgreSQL primary (two writes and one insert per run), then the per-tenant probe in the
dispatch loop. More in DESIGN.md section 10.

## Assumptions and limits

* Payloads hold ids and flags, not personal data. They are never logged.
* The handlers write their "effect" to a table instead of calling an SMS gateway. A real handler has to pass
  `ClaimedRun.idempotencyKey()` to the provider.
* DST: a time that doesn't exist fires once at the first valid instant; a time that happens twice fires once, at the
  first occurrence. So an hourly job loses one real hour on the fall-back day.
* The demo passwords and the default `JWT_SECRET` are for local use, and compose publishes ports on localhost only.
  With `SPRING_PROFILES_ACTIVE=prod` no demo users are seeded and the app refuses to start on the default secret.
* PostgreSQL is a single point of failure here; use an HA setup in production. Not built: multi-region, API rate
  limiting, per-job pause, changing the partition count (fixed at 64), a UI, cleanup of old finished runs.
* The task also asks for a short demo video. That is not in the repo yet.

## Where things are

```
core/      cron + DST, backoff, rendezvous partitioning, misfire resolver (no I/O, unit-tested alone)
store/     JDBC repositories; RunStore holds every fenced state change
engine/    ClusterMembership, Materializer, SchedulerNode (dispatch loop), RunExecutor, metrics, caches
handlers/  JobHandler -> SimulatedWorkHandler -> the five job types
service/   JobService, OpsService, BenchService
api/       controllers and the error handler       security/  JWT, Caller (tenant scoping), seed data
```
