# Benchmark and resilience methodology

The scripts in `load-tests` send real HTTP requests to FlowForge, which then uses its real PostgreSQL, Kafka, Redis, scheduler, and workers. They use only Python's standard library. Results are measured JSON written after execution; there are no prefilled performance values. Consult [benchmark-results.md](../benchmark-results.md) for runs actually completed in this repository.

## Reproducible setup

1. Start the stack with `docker compose up --build -d` and confirm all services are healthy.
2. Record the git revision, Docker CPU/memory allocation, host model, worker replica count, global and per-worker concurrency, broker partition count, and any non-default scheduler settings. Scripts record OS/Python versions, command parameters, worker snapshots, and git revision when available; they do not infer Docker VM resource allocations.
3. Run a small warm-up first and store it separately. JVM startup/JIT and database page-cache effects make cold and warm runs different experiments.
4. Run scenarios sequentially. Competing tests change queueing and make results incomparable. Use unique JSON output paths so prior measurements remain available.
5. A fresh datastore is optional, but disclose whether prior workflows/history existed. Scripts scope correctness checks to the workflow IDs they submit. API metrics snapshots describe the entire system and must not be mistaken for per-run counters.

```bash
python3 load-tests/benchmark.py --workflows 10 \
  --output load-tests/results/warmup.json

python3 load-tests/benchmark.py --scenario normal --workflows 2000 \
  --submit-concurrency 16 --timeout 3600 \
  --output load-tests/results/normal-2000.json

python3 load-tests/benchmark.py --scenario high-concurrency --workflows 1000 \
  --submit-concurrency 64 --width 12 --timeout 3600 \
  --output load-tests/results/high-concurrency-1000.json

python3 load-tests/benchmark.py --scenario large-dag --workflows 20 \
  --width 10 --layers 20 --timeout 3600 \
  --output load-tests/results/large-dag-200-tasks.json

python3 load-tests/benchmark.py --scenario retry-storm --workflows 100 \
  --width 4 --timeout 3600 \
  --output load-tests/results/retry-storm.json
```

The commands are runnable experiments, not claims that those sizes have been executed. Adjust deadlines to the hardware and configured concurrency. All workflows contain at most 1,000 tasks, matching API validation.

## Workloads

| Scenario | Definition | Purpose |
| --- | --- | --- |
| `normal` | Five-step order workflow: transform, delay, payment, delay, transform | Thousands of small dependent workflows and persistent mock side effects |
| `high-concurrency` | `--width` independent DELAY tasks per workflow | Many simultaneously ready tasks; burst submission |
| `large-dag` | `--layers` layers of `--width` tasks; each layer depends on all tasks in its predecessor | Hundreds of tasks and many dependency edges |
| `retry-storm` | `--width` mock payments, each failing attempts 1 and 2, then succeeding | Exponential retry spacing under simultaneous transient failures |

The temporary-dependency failure is deterministic per task attempt, not a simulation of an external service with a shared wall-clock outage. Delay defaults to 20 ms; retry storm uses a 200 ms initial delay with multiplier 2. Scheduler polling, broker dispatch, persistence, and queueing are intentionally included in measured end-to-end latency.

## Metrics and denominators

| JSON field | Definition |
| --- | --- |
| `workflowsSubmitted` | POST requests with a successful response containing a workflow ID |
| `workflowsCompleted` | Submitted workflows observed in `COMPLETED` |
| `totalTasks` | Sum of submitted workflows' API-reported task counts |
| `executionDurationSeconds` | Client `time.monotonic()` elapsed time from before initial metrics reads/submission until terminal polling ends; excludes post-run detailed verification |
| `workflowsPerSecond` | Completed workflows divided by execution duration |
| `tasksPerSecond` | Successfully completed tasks in verified workflow details divided by execution duration |
| `p50WorkflowLatencyMs`, `p95WorkflowLatencyMs` | Nearest-rank percentiles of persisted workflow `finishedAt - createdAt` for observed terminal workflows |
| `p50TaskDurationMs`, `p95TaskDurationMs` | Nearest-rank percentiles of successful attempt `finishedAt - startedAt`; includes result persistence/application delay |
| `failureRate` | FAILED workflows divided by successfully submitted workflows; unfinished and cancelled counts are reported separately |
| `retryRate` | Attempts with `attemptNumber > 1` divided by all collected attempts |
| `duplicateSideEffectCount` | Extra mock-payment ledger rows beyond one per expected completed payment task; `null` if ledger verification was skipped or verification is incomplete |
| `workerRecoveryTimeMs` | Only in worker-failure results: first replacement attempt start minus recorded SIGKILL time |

Unavailable metrics are `null`, not zero. The ordinary benchmark does not kill workers, so its recovery metric is `null`. Task duration uses individual attempts; the task-level `startedAt` is the first start across all retries and measures a different interval. Percentiles are not averages of API-wide percentiles. A run with no successful task attempt has no successful-task duration percentile.

**Clock limitation in the retained runs:** client monotonic execution durations disagree with the
database wall-clock spans, including workflows whose durations exceed the reported whole-run
duration. The cause is not established by the artifacts. `verified: true` checks execution
correctness, not clock consistency, so it must not be interpreted as validation of those throughput
figures. For future capacity measurements, record wall and monotonic timestamps at both execution
boundaries, compare them with database spans, keep the host awake, and reject inconsistent timing
runs. The current harness does not yet automate that clock-consistency check.

Submission calls are never retried automatically. If an HTTP response is lost, the server may have created a workflow whose ID the client cannot recover; the failed request is recorded as an error and the run is not marked verified. This avoids silently creating extra workflows. Polling contributes read load and completion observation overhead to throughput. A deadline stops further polling; already-running HTTP calls are allowed to finish under their request timeout. Detailed post-run verification may extend total script runtime beyond the execution deadline.

## Correctness assertions

For every terminal workflow the benchmark retrieves tasks and attempt history. It checks that each started task's parents completed first, using PostgreSQL timestamps with a 1 ms comparison tolerance. It checks successive attempts were created no earlier than the configured exponential backoff permits, with a 2 ms timestamp-rounding tolerance. For completed mock payments it compares the persisted ledger's task IDs with the expected completed payment tasks and checks there is only one row per task.

The harness exits nonzero if submission fails, verification fails, any requested workflow fails/cancels, or workflows remain unfinished at the deadline. `verified: true` means all requested workflows completed and the listed invariants passed for that run. It is not proof of all possible failure interleavings.

## Real failure injection

```bash
python3 load-tests/chaos.py --scenario smoke \
  --output load-tests/results/smoke.json

python3 load-tests/chaos.py --scenario duplicate --copies 20 \
  --output load-tests/results/duplicate.json

python3 load-tests/chaos.py --scenario worker-failure --allow-worker-kill \
  --timeout 240 --output load-tests/results/worker-failure.json
```

`--scenario all --allow-worker-kill` runs all three checks sequentially. The smoke check verifies cycle rejection, actual overlapping task intervals and a dependent join, timeout retry exhaustion/dead-letter visibility, and cancellation/history with no downstream payment.

Duplicate delivery publishes repeated task envelopes with the already-completed task's **same execution identifier** to the actual ready Kafka topic using `kafka-console-producer`. The worker must reject the stale claim. The harness waits for ready-topic consumer lag to reach zero, then verifies attempt count and the payment ledger remained unchanged. This demonstrates redelivery deduplication through the broker; it does not establish exactly-once delivery to arbitrary external APIs. `MOCK_PAYMENT` is a transactional PostgreSQL ledger, so its side effect can share the fencing transaction. External integrations would need their own idempotency contract.

The worker-failure check requires two or more Compose workers. It waits until all worker replicas have active tasks, maps a persisted worker UUID to the matching container's structured logs, validates Compose project/service labels, sends SIGKILL to one worker, and observes replacement attempts. It requires an expired-heartbeat failure and successful completion, then restarts the killed container in a `finally` block. It does not stop unrelated containers. Recovery time includes heartbeat detection, scheduler polling, retry backoff, consumer reassignment, and available worker capacity. A second client-observed duration is recorded to expose polling overhead.

The recovery timestamp subtracts a local host wall-clock timestamp from a database timestamp. Docker's database and client normally share a host clock; remote benchmarks should synchronize clocks or use the client-observed monotonic duration as the conservative measurement. A worker CPU pause, a Kafka outage, network partition, PostgreSQL failover, and multi-region recovery are distinct experiments and are not implied by this test.

## Restart persistence and dependency degradation

```bash
python3 load-tests/resilience.py --scenario restart --allow-service-restart \
  --output load-tests/results/restart.json

python3 load-tests/resilience.py --scenario redis-outage --allow-service-restart \
  --output load-tests/results/redis-outage.json
```

`--scenario all` runs both sequentially. Like `chaos.py`, this harness label-verifies every container
against the Compose project and service before acting on it and restores it in a `finally` block.

The restart check submits a dependency chain, waits until one task is `RUNNING` while others are
still `PENDING`, then restarts the API and scheduler. It requires the workflow to reach `COMPLETED`,
every pre-restart `workflow_events` row to still be present afterwards, at least one event to be
recorded after the restart timestamp, at least one task to complete that had not completed before it,
and the payment ledger to hold exactly one row. GET requests are retried across the outage because
they are idempotent; the submission is never retried. The reported API outage is the client-observed
time until `/actuator/health/readiness` answers again, which includes JVM restart and Flyway
validation, not only container scheduling.

The Redis check stops Redis and then submits a workflow **during** the outage, so admission,
dispatch, claiming, and result application all have to proceed without it. It asserts API readiness
stays `UP` — the readiness group is deliberately `readinessState` + `db` — that parallel tasks still
overlap, that dependency ordering holds, and that the ledger holds exactly one row. This exercises
the documented degradation path only: the scheduler lease fails open to the PostgreSQL advisory lock
and the worker status cache is advisory. It is not a test of Redis data durability, because Redis
holds no authoritative state. Stopping PostgreSQL or Kafka is a different experiment and is not
covered here.

## Retaining evidence

Keep the result JSON files and captured command parameters next to any report. Large benchmark files contain task payloads, workflow IDs, worker IDs, and attempt history; treat them as execution data when sharing outside your environment. Update `benchmark-results.md` only from completed measurements. Report failed runs and limitations, including unfinished workflows, alongside successful runs rather than replacing them with synthetic numbers.
