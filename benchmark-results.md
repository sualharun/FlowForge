# Benchmark and verification results

Every number on this page was produced by a run recorded in `load-tests/results/`. Nothing is
estimated, extrapolated, or carried over from a different configuration. Where a metric was not
measured it is reported as *not measured*, not as zero. See
[benchmark methodology](docs/benchmark-methodology.md) for the definitions behind each field.

## Environment

All runs below executed on one machine against the Docker Compose topology, sequentially, from
commit `6fb4304` with no source changes in the worktree. Each run records its own git revision and
`git status` output; the only modification any of them observed was the previous run's own result
file, which each script writes into the tracked tree.

| Property | Value |
|---|---|
| Host | Apple M3 Pro (`Mac15,6`), macOS 26.6.2, arm64 |
| Docker | Docker Desktop 28.0.1; 11 CPUs and 8.22 GB (7.65 GiB) allocated to the VM |
| Runtime | Temurin OpenJDK 21.0.2; Python 3.13.2 (standard library only) |
| Images | Built locally from this tree with `docker compose up --build -d` |
| Services | 1 API, 1 scheduler, **2 worker replicas**, 1 PostgreSQL 16, 1 Redis 7, 1 Kafka 3.9.1 (KRaft) |
| `FLOWFORGE_GLOBAL_CONCURRENCY` | 32 |
| `FLOWFORGE_WORKER_CONCURRENCY` | 4 per worker process (8 handler slots across both replicas) |
| `FLOWFORGE_TASK_TYPE_CONCURRENCY` | empty (no per-type caps) |
| `WORKER_TIMEOUT_MS` | 15000 |
| Kafka topics | 4 explicit topics, 12 partitions each, replication factor 1 |
| Scheduler scan interval | 500 ms (default) |

The database was **not** reset between runs; earlier traffic and examples were already present.
Correctness assertions are scoped to the workflow IDs each run submitted, so prior rows cannot
affect a run's verdict. The API metrics snapshots inside each JSON file (`metricsBefore` /
`metricsAfter`) describe the whole system and must not be read as per-run counters.

A warm-up run (`warmup.json`, 10 workflows) was executed first and is retained separately, because
JVM start-up, JIT, and database page-cache effects make a cold run a different experiment. It is
excluded from the comparisons below.

## Why these numbers are trusted, and an earlier set was not

An earlier set of runs derived throughput from `time.monotonic()` alone. On macOS that clock does
not advance while the host is asleep, so idle sleep during the longer runs understated execution
duration — by a factor of 2.2 on the 1000-workflow scenario — and overstated throughput. The
symptom was visible in the retained data: a workflow's persisted lifetime exceeded the reported
duration of the entire run that produced it.

The harness now refuses to publish throughput it cannot defend. Each run:

- brackets execution between paired wall-clock and monotonic samples, and brackets every wall read
  between two monotonic reads so a pause *during sampling* is itself measurable;
- accumulates `max(forward wall increment, forward monotonic increment, 0)` for deadline
  accounting, so a host suspend counts against the deadline even when monotonic stalls;
- sums the per-observation disagreement between the two clocks, which catches clocks that diverge
  and re-converge so the endpoints alone would look consistent;
- cross-checks the combined span of persisted database timestamps against the client interval and
  rejects the run if the database span is longer. Because a span is a difference, a constant
  client/server clock offset does not invalidate a run;
- reports `correctnessVerified` and `timingVerified` separately, sets throughput to `null` unless
  both hold, and exits non-zero.

The acceptance budget is a declared measurement-quality policy — 250 ms absolute plus 0.1 % of the
measured duration — not a claim about OS clock accuracy. The runs below were executed with the host
held awake. Observed clock disagreement was **40.5 ms on the 180-second run against a 431 ms
budget**, and at most 1.3 ms elsewhere. Fourteen deterministic unit tests cover this logic and run
in CI on every push.

## Throughput and latency

```bash
python3 load-tests/benchmark.py --scenario normal --workflows 1000 --output load-tests/results/normal.json
python3 load-tests/benchmark.py --scenario high-concurrency --workflows 100 --width 20 --output load-tests/results/concurrency.json
python3 load-tests/benchmark.py --scenario large-dag --workflows 2 --width 20 --layers 20 --output load-tests/results/large-dag.json
python3 load-tests/benchmark.py --scenario retry-storm --workflows 100 --width 10 --output load-tests/results/retry-storm.json
```

| Scenario | Completed | Tasks | Attempts | Execution | Workflows/s | Tasks/s | Clock disagreement |
|---|---:|---:|---:|---:|---:|---:|---:|
| `normal` (1000 × 5-task chain) | 1000 / 1000 | 5000 | 5000 | 180.60 s | 5.537 | 27.69 | 40.5 ms |
| `high-concurrency` (100 × 20 parallel) | 100 / 100 | 2000 | 2000 | 54.20 s | 1.845 | 36.90 | 1.0 ms |
| `large-dag` (2 × 20 layers × 20 wide) | 2 / 2 | 800 | 800 | 34.36 s | 0.058 | 23.28 | 0.6 ms |
| `retry-storm` (100 × 10 payments, each failing twice) | 100 / 100 | 1000 | 3000 | 72.00 s | 1.389 | 13.89 | 1.3 ms |

**Workflow latency and task-attempt duration are different intervals and are reported separately.**
Workflow latency is persisted `finishedAt − createdAt` for the whole DAG, so it includes all time
queued behind the global admission limit. Task-attempt duration is a single attempt's
`finishedAt − startedAt`, from the durable claim through scheduler application of the result. Both
are nearest-rank percentiles over that run's own records.

| Scenario | Workflow p50 | Workflow p95 | Attempt p50 | Attempt p95 |
|---|---:|---:|---:|---:|
| `normal` | 112 005 ms | 170 997 ms | 90.8 ms | 522.3 ms |
| `high-concurrency` | 26 516 ms | 50 924 ms | 100.2 ms | 544.0 ms |
| `large-dag` | 30 281 ms | 33 912 ms | 103.3 ms | 586.0 ms |
| `retry-storm` | 36 530 ms | 68 321 ms | 93.0 ms | 556.3 ms |

The gap between the two columns is queueing, not slow execution. In `normal`, 5000 tasks were
admitted through a global limit of 32 with 8 handler slots, so individual attempts stayed near
90 ms at p50 while whole workflows waited minutes for admission. Raising
`FLOWFORGE_GLOBAL_CONCURRENCY`, worker concurrency, or replica count moves these numbers; the
measurements describe only the configuration in the table above, and single runs do not establish
how they would scale.

## Correctness observed during the same runs

Every benchmark re-reads each completed workflow's tasks and attempt history and asserts dependency
ordering, exponential-backoff spacing, and payment-ledger uniqueness.

| Scenario | Failure rate | Retries | Retry rate | Dependency edges checked | Backoff intervals checked | Duplicate side effects | Submission errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| `normal` | 0.0 | 0 | 0.0000 | 4000 | 0 | 0 | 0 |
| `high-concurrency` | 0.0 | 0 | 0.0000 | 0 | 0 | 0 | 0 |
| `large-dag` | 0.0 | 0 | 0.0000 | 15 200 | 0 | 0 | 0 |
| `retry-storm` | 0.0 | 2000 | 0.6667 | 0 | 2000 | 0 | 0 |

`high-concurrency` and `retry-storm` workflows contain no dependency edges by construction, so their
edge counts are legitimately zero. `retry-storm`'s retry rate of 0.6667 is exactly the designed
shape: 1000 tasks each failed attempts 1 and 2 before succeeding on attempt 3, giving 2000 retried
attempts out of 3000, and all 2000 retry intervals satisfied the configured 200 ms × 2 backoff
floor. No scenario produced a duplicate mock-payment ledger row.

**Worker recovery time is not measured by these four scenarios** — they never kill a worker, so the
harness records `workerRecoveryTimeMs: null` rather than a zero. It is measured below.

## Real failure injection

```bash
python3 load-tests/chaos.py --scenario all --allow-worker-kill --output load-tests/results/chaos.json
```

`chaos.json` — `verified: true`, no errors.

| Check | Result |
|---|---|
| Cyclic DAG submission | rejected with HTTP 400 |
| Independent tasks | genuinely overlapping execution intervals; join waited for all four parents |
| Timeout exhaustion | attempts 1 and 2 both `TIMED_OUT`, workflow `FAILED`, dead-letter record queryable |
| Cancellation | workflow and unfinished tasks `CANCELLED`; downstream payment produced **0** ledger rows; `WORKFLOW_CANCELLED` retained in history |
| **Duplicate Kafka delivery** | 10 copies of the original envelope — same `executionId` — published through the real broker with `kafka-console-producer`; after consumer-group lag drained to 0 the ledger still held **1** row and the attempt count was still **1** (**0** duplicate side effects) |
| **Worker SIGKILL** | 4 tasks abandoned; 4 attempts failed with heartbeat expiry; replacements ran on the surviving worker; workflow reached `COMPLETED`; victim restarted in a `finally` block |

Worker recovery: **15 468 ms** to the first replacement attempt's `startedAt` (client-observed
15 643 ms). This is deliberately measured from `task_attempts.startedAt` of the replacement
execution, not `workflow_tasks.startedAt`, which remains the first attempt's start across retries
and would understate recovery. The figure is the sum of heartbeat expiry (15 s threshold),
scheduler poll interval, Kafka consumer-group reassignment, retry backoff, and waiting for free
capacity — not a fixed constant. It varies with how many tasks were abandoned and how saturated the
survivors are, so a single measurement is not a service-level objective.

## Dependency outages and restart persistence

```bash
python3 load-tests/resilience.py --scenario all --allow-service-restart --output load-tests/results/resilience.json
python3 load-tests/outages.py    --scenario all --allow-service-restart --output load-tests/results/outages.json
```

Both `verified: true`. Every container these scripts touch is label-verified against this Compose
project and service before being acted on, and restored in a `finally` block.

| Scenario | Evidence |
|---|---|
| **API + scheduler restarted mid-execution** | 3.63 s observed readiness outage. All 4 pre-restart history events retained, 27 events total afterwards. **0** tasks had completed before the interruption and all **8** completed after it, so scheduling and result application resumed from PostgreSQL rather than process memory. Terminal `COMPLETED`; ledger held 1 row. |
| **Redis stopped** | A workflow *submitted during the outage* reached `COMPLETED` in 3728 ms. API readiness stayed `UP` at both samples — the readiness group is deliberately `readinessState` + `db`. Parallel tasks still overlapped; ledger held 1 row. The scheduler lease fails open to the PostgreSQL advisory lock and the worker status cache is advisory, so losing Redis costs redundant-scan suppression, not correctness. |
| **Kafka stopped** | A result was **durably persisted while its attempt still read `RUNNING`** — the committed result waited in the outbox with no consumer to apply it, which is precisely the documented behaviour that Kafka result consumption is required to advance execution. API readiness stayed `UP`. Both the in-flight workflow and one submitted during the outage completed after recovery. |
| **PostgreSQL stopped** | `/actuator/health/readiness` correctly reported unavailable, because `db` is in the readiness group. After recovery the workflow completed and its history grew from 4 to 9 events. |
| **Worker paused (SIGSTOP)** | A paused worker keeps its connections but stops executing, so heartbeat loss alone would not catch it. The attempt was recovered through **deadline expiry** (`TIMED_OUT`) and completed elsewhere. This is a distinct failure mode from SIGKILL. |

## Restart from a fully stopped stack

`docker compose --profile observability down` followed by `docker compose up --build -d` reached all
eight healthy containers 24.6 s after the command returned. Every durable count was identical across
the restart — 2484 workflows, 17 966 tasks, 21 977 attempts, 77 414 events, 4059 payment-ledger
rows, 3 dead letters, 43 941 outbox rows — and the four Kafka topics retained their messages from
the named volumes. A subsequent example workflow completed 5/5, confirming the README's claim that
`docker compose down` retains execution state in volumes.

## Automated test suite

```bash
JAVA_HOME=/path/to/temurin-21 mvn verify          # 64 tests
cd load-tests && python3 -m unittest discover     # 14 tests
```

78 tests, 0 failures, 0 errors, 0 skipped.

| Module | Tests | Notes |
|---|---:|---|
| `shared` | 30 | Includes 20 `EngineIntegrationTest` cases on real Testcontainers PostgreSQL and Kafka |
| `api-service` | 8 | Controller validation, error mapping, request-body size limits |
| `scheduler-service` | 6 | Lease gating, recovery-before-admission ordering, lease release on failure, result fencing |
| `worker-service` | 20 | Handlers, claim/acknowledgement behaviour, listener wiring |
| `load-tests` | 14 | Clock and deadline validation, and benchmark result/provenance shape |

Docker is required for the integration tests; they are not silently replaced by mocks. The
`load-tests` cases are deterministic and need no running stack, so CI runs them on every push.

## Browser acceptance audit

```bash
PLAYWRIGHT_MODULE=<playwright> UI_BASE_URL=http://localhost:3000 node docs/verification/dashboard-audit.cjs
```

`docs/verification/dashboard-audit.json` — **12 passed, 0 failed, 0 findings, 0 page errors** in
Chromium 152. It submits and completes a real workflow, cancels a second and confirms the downstream
task was cancelled with 0 attempts, inspects live task payloads and results, drives dialog focus and
keyboard containment, checks all four views at 360/390/768/1440 px for page-level horizontal
panning, and injects 503s, empty arrays and delayed responses to exercise the stale, empty, loading
and error states.

Accessibility is measured separately with axe-core 4.10.2: **0 violations** across all five views
and the submit dialog, against WCAG 2.0/2.1 A and AA plus best-practice rules, with 19–38 rules
passing per view. Every one of the 13 badge variants the app can render was injected and audited, so
the result does not depend on which statuses happen to be in the database. One decorative chevron
remains flagged "needs review" because axe cannot measure contrast for glyph-only content.

## What was not measured or verified

- **No Kubernetes or AWS runtime verification.** Both overlays render with `kubectl kustomize`, and
  all seven workloads carry startup, readiness and liveness probes plus resource requests and
  limits. No cluster was available in this environment (`kubectl` has no configured context), so
  nothing was applied or scheduled. Offline schema validation is also impossible —
  `kubectl apply --dry-run=client` must download the cluster OpenAPI document. The AWS endpoints,
  credentials, certificate paths and ECR image locations are deliberate placeholders.
- Single runs only. No scenario was repeated enough times to report variance, and the machine was
  running an unrelated Docker project throughout.
- Docker VM CPU/memory allocation is recorded from `docker info`; the harness does not infer it.
- Recovery was exercised for a SIGKILLed worker, a paused worker, an API/scheduler restart, a full
  stack restart, and Kafka, Redis and PostgreSQL outages. A network partition, a PostgreSQL
  failover, a disk-full condition and multi-region recovery are separate experiments and are **not**
  implied here.
- No retention, archival or sustained-soak measurement. Completed attempts, events and published
  outbox rows accumulate, and every run above left its rows in place.
- Throughput here is bounded by a deliberately conservative configuration (global limit 32, 8
  handler slots, 500 ms scan, one broker, one database). These are not upper bounds for the design.
