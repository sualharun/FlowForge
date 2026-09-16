# Benchmark and verification results

Every number on this page was produced by a run recorded in `load-tests/results/`. Nothing is
estimated, extrapolated, or carried over from a different configuration. Where a metric was not
measured it is reported as *not measured*, not as zero. See
[benchmark methodology](docs/benchmark-methodology.md) for the definitions behind each field.

## Environment

All runs below executed on one machine against the Docker Compose topology, sequentially.

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
| Git revision | **none recorded** — these runs preceded the first commit, so the harness stored `gitCommit: null` |

The database was **not** reset between runs; earlier smoke traffic and examples were already
present. Correctness assertions are scoped to the workflow IDs each run submitted, so prior rows
cannot affect a run's verdict. The API metrics snapshots inside each JSON file
(`metricsBefore`/`metricsAfter`) describe the whole system and must not be read as per-run counters.

A warm-up run (`load-tests/results/warmup.json`, 10 workflows) was executed first and is retained
separately, because JVM start-up, JIT, and database page-cache effects make a cold run a different
experiment. It is excluded from the comparisons below.

## Throughput and latency

**Timing limitation:** the client monotonic clock used for execution duration disagrees with the
database wall-clock timestamps in all four runs. For example, `normal.json` reports an execution
duration of 145.46 s but a workflow p95 of 297 039 ms. Queueing cannot explain a workflow taking
longer than the reported duration of its entire run. The retained data does not establish the cause.
The duration and throughput columns below reproduce the recorded client-clock readings; **they are
not validated wall-clock throughput or capacity measurements**. Correctness assertions still passed.
Repeat performance experiments with wall and monotonic durations recorded together, and investigate
clock divergence before drawing performance conclusions. No historical JSON has been changed.

```bash
python3 load-tests/benchmark.py --scenario normal --workflows 1000 --output load-tests/results/normal.json
python3 load-tests/benchmark.py --scenario high-concurrency --workflows 100 --width 20 --output load-tests/results/concurrency.json
python3 load-tests/benchmark.py --scenario large-dag --workflows 2 --width 20 --layers 20 --output load-tests/results/large-dag.json
python3 load-tests/benchmark.py --scenario retry-storm --workflows 100 --width 10 --output load-tests/results/retry-storm.json
```

| Scenario | Workflows completed | Tasks | Attempts | Recorded client elapsed | Recorded workflows/s | Recorded tasks/s |
|---|---:|---:|---:|---:|---:|---:|
| `normal` (1000 × 5-task chain) | 1000 / 1000 | 5000 | 5000 | 145.46 s | 6.875 | 34.37 |
| `high-concurrency` (100 × 20 parallel) | 100 / 100 | 2000 | 2000 | 55.89 s | 1.789 | 35.79 |
| `large-dag` (2 × 20 layers × 20 wide) | 2 / 2 | 800 | 800 | 32.46 s | 0.062 | 24.65 |
| `retry-storm` (100 × 10 payments, each failing twice) | 100 / 100 | 1000 | 3000 | 74.97 s | 1.334 | 13.34 |

**Workflow latency and task-attempt duration are different intervals and are reported separately.**
Workflow latency is persisted `finishedAt − createdAt` for the whole DAG and therefore includes all
time queued behind the global admission limit. Task-attempt duration is a single attempt's
`finishedAt − startedAt`, measured from the durable claim through scheduler application of the
result. Workflow percentiles use that run's terminal workflows; attempt percentiles use its
successful attempts. Both use the nearest-rank method.

| Scenario | Workflow p50 | Workflow p95 | Attempt p50 | Attempt p95 |
|---|---:|---:|---:|---:|
| `normal` | 131 626 ms | 297 039 ms | 98.3 ms | 553.4 ms |
| `high-concurrency` | 30 801 ms | 55 886 ms | 101.7 ms | 554.9 ms |
| `large-dag` | 36 295 ms | 37 304 ms | 103.5 ms | 597.2 ms |
| `retry-storm` | 39 851 ms | 117 907 ms | 98.7 ms | 589.7 ms |

Workflow latency includes admission queueing, dependency waits, retries, and task execution.
Attempt duration covers only one claimed execution through result application. These different
intervals explain why workflow and attempt percentiles differ, but they do not resolve the
client/database clock discrepancy above. These single runs do not establish how changing
concurrency or replica count would affect performance.

## Correctness observed during the same runs

Every benchmark re-reads each completed workflow's tasks and attempt history and asserts dependency
ordering, exponential-backoff spacing, and payment-ledger uniqueness. A run is only marked
`verified: true` when all requested workflows completed and every assertion held.

| Scenario | Verified | Failure rate | Retries | Retry rate | Dependency edges checked | Backoff intervals checked | Duplicate side effects | Submission errors |
|---|:--:|---:|---:|---:|---:|---:|---:|---:|
| `normal` | yes | 0.0 | 0 | 0.0000 | 4000 | 0 | 0 | 0 |
| `high-concurrency` | yes | 0.0 | 0 | 0.0000 | 0 | 0 | 0 | 0 |
| `large-dag` | yes | 0.0 | 0 | 0.0000 | 15 200 | 0 | 0 | 0 |
| `retry-storm` | yes | 0.0 | 2000 | 0.6667 | 0 | 2000 | 0 | 0 |

`high-concurrency` and `retry-storm` workflows contain no dependency edges by construction, so their
edge counts are legitimately zero. `retry-storm`'s retry rate of 0.6667 is exactly the designed
shape: 1000 tasks each failed attempts 1 and 2 before succeeding on attempt 3, giving 2000 retried
attempts out of 3000, and all 2000 retry intervals satisfied the configured 200 ms × 2 backoff
floor. No scenario produced a duplicate mock-payment ledger row.

**Worker recovery time is not measured by these four scenarios** — they never kill a worker, so the
harness records `workerRecoveryTimeMs: null` rather than a zero. It is measured by the chaos run
below.

## Real failure injection

```bash
python3 load-tests/chaos.py --scenario all --allow-worker-kill --output load-tests/results/chaos.json
```

`load-tests/results/chaos.json` — `verified: true`, no errors. Run against the final images.

| Check | Result |
|---|---|
| Cyclic DAG submission | rejected with HTTP 400 |
| Independent tasks | genuinely overlapping execution intervals; join waited for all four parents |
| Timeout exhaustion | attempts 1 and 2 both `TIMED_OUT`, workflow `FAILED`, dead-letter record present and queryable |
| Cancellation | workflow and all unfinished tasks `CANCELLED`; downstream payment produced **0** ledger rows; `WORKFLOW_CANCELLED` retained in history |
| **Duplicate Kafka delivery** | 10 copies of the original envelope — same `executionId` — published through the real broker with `kafka-console-producer`; after consumer-group lag drained to 0 the ledger still held **1** row and the attempt count was still **1** (**0** duplicate side effects) |
| **Worker SIGKILL** | 4 tasks abandoned; 4 attempts failed with heartbeat expiry; replacement attempts ran on the surviving worker; workflow reached `COMPLETED`; victim container restarted in a `finally` block |

The worker-failure run also recorded **4 TIMED_OUT attempts** with `Execution deadline exceeded`,
in addition to the 4 heartbeat-expiry failures. The workflow recovered and completed, but this was
not an isolated measurement of heartbeat recovery without other attempt failures.

Worker recovery: **39 921 ms** to the first replacement attempt's `startedAt`, with a client-observed
40 006 ms. This is deliberately measured from `task_attempts.startedAt` of the replacement execution,
not from `workflow_tasks.startedAt` (which remains the first attempt's start across retries and would
understate recovery). The figure is a sum of heartbeat expiry (15 s threshold), scheduler poll
interval, Kafka consumer-group reassignment, retry backoff, and waiting for free capacity on the one
surviving worker — not a fixed constant. Recovery latency varies materially with how many tasks were
abandoned and how saturated the survivors are, so this single measurement should not be read as a
service-level objective.

## Restart persistence and Redis degradation

```bash
python3 load-tests/resilience.py --scenario all --allow-service-restart --output load-tests/results/resilience.json
```

`load-tests/results/resilience.json` — `verified: true`, no errors. Both scenarios restore every
container they touch in a `finally` block and refuse to act on any container whose Compose
project/service labels fall outside this project.

**API and scheduler restarted mid-execution.** A dependency chain was interrupted while one task was
`RUNNING` and others were still `PENDING`:

| Measurement | Value |
|---|---|
| Services restarted | `api`, `scheduler` |
| Observed API readiness outage | 4.18 s (5.75 s wall for the restart call) |
| History events before / after | 4 → 27, all 4 pre-restart events retained |
| Events recorded after the restart | 23 |
| Tasks completed before / after the restart | 0 / 8 (`step-0`…`step-5`, `charge`, `confirm`) |
| Terminal status | `COMPLETED` |
| Dependency edges re-verified | 7 |
| Payment ledger rows | 1 (0 duplicates) |
| Workflow latency | 21 976 ms, including the outage |

No task had completed before the interruption and all eight completed after it, so scheduling and
result application genuinely resumed from PostgreSQL rather than from process memory.

**Redis stopped.** The workflow below was *submitted while Redis was down*, so admission, dispatch,
claiming, and result application all had to work without it:

| Measurement | Value |
|---|---|
| Service stopped | `redis` |
| API readiness during the outage | `UP` at both samples |
| Terminal status | `COMPLETED` (3507 ms) |
| Parallel tasks overlapped | yes |
| Tasks / attempts / history events | 9 / 9 / 30 |
| Dependency edges checked | 8 |
| Payment ledger rows | 1 (0 duplicates) |
| Redis healthy after restore | yes |

This is the intended degradation path: the Redis scheduler lease fails open to the PostgreSQL
transaction-scoped advisory lock, the worker status cache is advisory only, and the readiness group
is deliberately `readinessState` + `db` so a Redis outage does not take the API out of service.
Losing Redis costs the redundant-scan suppression and the status cache, not correctness.

## Automated test suite

```bash
JAVA_HOME=/path/to/temurin-21 mvn verify
```

63 tests, 0 failures, 0 errors, 0 skipped, `BUILD SUCCESS`.

| Module | Tests | Notes |
|---|---:|---|
| `shared` | 29 | Includes 19 `EngineIntegrationTest` cases on real Testcontainers PostgreSQL and Kafka, plus DAG and retry unit tests |
| `api-service` | 8 | Controller validation, error mapping, and request-body size limits |
| `scheduler-service` | 6 | Lease gating, recovery-before-admission ordering, lease release on failure, result-notification fencing |
| `worker-service` | 20 | Handlers, claim/acknowledgement behaviour, listener wiring |

Docker is required; the integration tests are not silently replaced by mocks.

## What was not measured or verified

- **No Kubernetes or AWS runtime verification.** Both overlays render with
  `kubectl kustomize`, and all seven workloads were inspected to carry startup, readiness, and
  liveness probes plus resource requests and limits. No cluster was available in this environment
  (`kubectl` has no configured context), so nothing was applied, scheduled, or exercised. No offline
  schema validation was performed; the attempted `kubectl apply --dry-run=client` path required
  the cluster OpenAPI document. The AWS endpoints, credentials, certificate paths, and ECR image locations are
  deliberate placeholders.
- **No git revision is attached to these results**, because the runs preceded the first commit. Re-run the
  scenarios after the first commit if you need revision-pinned evidence.
- Single-run figures. No scenario was repeated enough times to report variance, and the machine was
  running an unrelated Docker project during these runs.
- Docker VM CPU/memory allocation is recorded from `docker info`; the harness does not infer it.
- Recovery was exercised for one SIGKILLed worker and for an API/scheduler restart. A Kafka outage,
  a PostgreSQL failover, a network partition, a paused (rather than killed) worker, and multi-region
  recovery are separate experiments and are **not** implied by these results.
- No retention, archival, or sustained-soak measurement. Completed attempts, events, and published
  outbox rows accumulate, and every run above left its rows in place.
- The client/database clock discrepancy prevents treating the recorded throughput as a validated
  capacity measurement. The configuration was global limit 32, 8 handler slots, 500 ms scans, one
  broker, and one database; neither sustainable capacity nor scaling limits were established.
