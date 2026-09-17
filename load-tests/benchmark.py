#!/usr/bin/env python3
"""Submit and verify real workflows, then write measured results as JSON.

No dependency installation required. See --help and docs/benchmark-methodology.md.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys
import time
import uuid

from benchmark_timing import ExecutionClock
from flowforge_client import (Client, TERMINAL, assert_dependencies, percentile,
                             task, timestamp, utc_now, write_result)


def definition(scenario, index, run_id, width, layers, delay_ms):
    name = f"bench-{scenario}-{run_id}-{index}"
    if scenario == "normal":
        tasks = [
            task("validate", "DATA_TRANSFORM", {"data": "order", "operation": "UPPERCASE"}),
            task("reserve", payload={"durationMs": delay_ms}, dependencies=["validate"]),
            task("payment", "MOCK_PAYMENT", {"orderId": name, "amount": 10, "currency": "USD"}, ["reserve"]),
            task("shipment", payload={"durationMs": delay_ms}, dependencies=["payment"]),
            task("confirm", "DATA_TRANSFORM", {"data": "confirmed", "operation": "UPPERCASE"}, ["shipment"]),
        ]
    elif scenario == "retry-storm":
        tasks = [task(f"payment-{n}", "MOCK_PAYMENT", {
            "orderId": f"{name}-{n}", "amount": 10, "currency": "USD", "failUntilAttempt": 2,
        }, initialRetryDelayMs=200, maxRetries=3) for n in range(width)]
    elif scenario == "high-concurrency":
        tasks = [task(f"parallel-{n}", payload={"durationMs": delay_ms}) for n in range(width)]
    else:
        tasks = []
        parents = []
        for layer in range(layers):
            current = [f"layer-{layer}-task-{n}" for n in range(width)]
            tasks += [task(name, payload={"durationMs": delay_ms}, dependencies=parents) for name in current]
            parents = current
    return {"name": name, "concurrencyLimit": min(width, 1000), "tasks": tasks}


def verify_workflow(client, workflow_id, verify_payments):
    tasks = client.request(f"/api/workflows/{workflow_id}/tasks")
    attempts = client.request(f"/api/workflows/{workflow_id}/attempts")
    edges = assert_dependencies(tasks)
    delays = []
    by_task = defaultdict(list)
    for attempt in attempts:
        by_task[attempt["taskId"]].append(attempt)
    for item in tasks:
        ordered = sorted(by_task[item["id"]], key=lambda value: value["attemptNumber"])
        for previous, current in zip(ordered, ordered[1:]):
            if not previous.get("finishedAt") or not current.get("createdAt"):
                continue
            actual = (timestamp(current["createdAt"]) - timestamp(previous["finishedAt"])) * 1000
            minimum = min(3600000, item["initialRetryDelayMs"] * item["backoffMultiplier"] ** (previous["attemptNumber"] - 1))
            if actual + 2 < minimum:
                raise AssertionError(f"Retry before backoff elapsed: {workflow_id}/{item['name']}: {actual} < {minimum}ms")
            delays.append({"attemptNumber": current["attemptNumber"], "minimumMs": minimum, "observedMs": actual})
    duplicate_count = None
    if verify_payments:
        payments = client.request(f"/api/admin/payments?workflowId={workflow_id}")
        expected = {item["id"] for item in tasks if item["taskType"] == "MOCK_PAYMENT" and item["status"] == "COMPLETED"}
        counted = Counter(item["taskId"] for item in payments)
        if expected != set(counted):
            raise AssertionError(f"Payment ledger mismatch for workflow {workflow_id}")
        duplicate_count = sum(max(0, count - 1) for count in counted.values())
        if duplicate_count:
            raise AssertionError(f"Duplicate payment ledger entries: {workflow_id}")
    return {"workflowId": workflow_id, "tasks": tasks, "attempts": attempts,
            "dependencyEdgesChecked": edges, "retryDelays": delays,
            "duplicateSideEffectCount": duplicate_count}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--scenario", choices=["normal", "high-concurrency", "large-dag", "retry-storm"], default="normal")
    parser.add_argument("--workflows", type=int, default=1000)
    parser.add_argument("--submit-concurrency", type=int, default=16)
    parser.add_argument("--poll-concurrency", type=int, default=8)
    parser.add_argument("--width", type=int, default=10)
    parser.add_argument("--layers", type=int, default=20)
    parser.add_argument("--delay-ms", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=1800, help="Submission/polling deadline in seconds; accounts for observed wall-clock pauses")
    parser.add_argument("--poll-interval", type=float, default=1)
    parser.add_argument("--output", default="load-tests/results/latest.json")
    parser.add_argument("--skip-payment-verification", action="store_true", help="Report duplicate count as null")
    args = parser.parse_args()
    for name in ["workflows", "submit_concurrency", "poll_concurrency", "width", "layers", "timeout", "poll_interval"]:
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.width > 1000 or (args.scenario == "large-dag" and args.width * args.layers > 1000):
        parser.error("A workflow may contain at most 1000 tasks")
    if not 0 <= args.delay_ms < 30000:
        parser.error("--delay-ms must be 0..29999 for the default 30s task timeout")
    return args


def main():
    args = parse_args()
    client = Client(args.base_url)
    result = {"schemaVersion": 2, "startedAt": utc_now(), "configuration": vars(args),
              "environment": {"platform": platform.platform(), "python": platform.python_version()},
              "errors": [], "workflowIds": [], "timedOutWorkflowIds": [], "verified": False,
              "correctnessVerified": False, "timingVerified": False}
    repository = Path(__file__).resolve().parent.parent
    try:
        result["environment"]["gitCommit"] = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repository, capture_output=True, text=True, check=True).stdout.strip()
        status = subprocess.run(["git", "status", "--porcelain=v1", "--untracked-files=all"],
                                cwd=repository, capture_output=True, text=True, check=True).stdout
        result["environment"]["gitDirty"] = bool(status.strip())
        result["environment"]["gitStatusPorcelain"] = status.splitlines()
    except (OSError, subprocess.CalledProcessError):
        result["environment"].setdefault("gitCommit", None)
        result["environment"].setdefault("gitDirty", None)
    result["environment"]["harnessSourceSha256"] = {
        name: hashlib.sha256((repository / "load-tests" / name).read_bytes()).hexdigest()
        for name in ("benchmark.py", "benchmark_timing.py", "flowforge_client.py")
    }
    result["environment"]["monotonicClock"] = vars(time.get_clock_info("monotonic"))
    clock = None
    run_id = uuid.uuid4().hex[:10]
    submissions, finished, details = {}, {}, []
    try:
        result["metricsBefore"] = client.request("/api/metrics/summary")
        result["workersBefore"] = client.request("/api/workers")
        # These samples bracket execution exactly. Read-only setup and verification
        # cannot inflate the denominator used for throughput.
        clock = ExecutionClock()

        def submit_before_deadline(value):
            if clock.observe() >= args.timeout:
                raise TimeoutError("Execution deadline reached before this submission started")
            return client.submit(value)

        with ThreadPoolExecutor(max_workers=args.submit_concurrency) as pool:
            futures = {pool.submit(submit_before_deadline, definition(args.scenario, n, run_id, args.width, args.layers, args.delay_ms)): n
                       for n in range(args.workflows)}
            for future in as_completed(futures):
                clock.observe()
                try:
                    value = future.result()
                    submissions[value["id"]] = value
                except Exception as error:
                    result["errors"].append({"phase": "submit", "index": futures[future], "error": str(error)})
        clock.observe()
        result["submissionDurationSeconds"] = clock.monotonic_duration_seconds
        result["workflowIds"] = list(submissions)
        pending = set(submissions)

        def poll_before_deadline(workflow_id):
            if clock.observe() >= args.timeout:
                return None
            return client.request(f"/api/workflows/{workflow_id}")

        with ThreadPoolExecutor(max_workers=args.poll_concurrency) as pool:
            while pending and clock.observe() < args.timeout:
                futures = {pool.submit(poll_before_deadline, workflow_id): workflow_id for workflow_id in pending}
                for future in as_completed(futures):
                    clock.observe()
                    workflow = future.result()
                    if workflow is None:
                        continue
                    if workflow["status"] in TERMINAL:
                        finished[workflow["id"]] = workflow
                        pending.remove(workflow["id"])
                        if not pending:
                            clock.stop()
                elapsed = clock.observe()
                print(f"{args.scenario}: {len(finished)}/{len(submissions)} terminal, elapsed {elapsed:.1f}s", flush=True)
                if pending:
                    time.sleep(min(args.poll_interval, max(0, args.timeout - elapsed)))
            # Stop throughput timing before the read-only verification requests.
            clock.stop()
            result["timedOutWorkflowIds"] = sorted(pending)
            futures = {pool.submit(verify_workflow, client, workflow_id, not args.skip_payment_verification): workflow_id
                       for workflow_id in finished}
            for future in as_completed(futures):
                try:
                    details.append(future.result())
                except Exception as error:
                    result["errors"].append({"phase": "verify", "workflowId": futures[future], "error": str(error)})
        result["metricsAfter"] = client.request("/api/metrics/summary")
        result["workersAfter"] = client.request("/api/workers")
    except Exception as error:
        result["errors"].append({"phase": "run", "error": str(error)})
    if clock is not None:
        clock.stop()
    duration = clock.monotonic_duration_seconds if clock is not None else None
    result["executionDurationSeconds"] = duration
    statuses = Counter(item["status"] for item in finished.values())
    all_tasks = [item for workflow in details for item in workflow["tasks"]]
    attempts = [item for workflow in details for item in workflow["attempts"]]
    retries = sum(item["attemptNumber"] > 1 for item in attempts)
    latencies = [(timestamp(item["finishedAt"]) - timestamp(item["createdAt"])) * 1000
                 for item in finished.values() if item.get("finishedAt")]
    task_durations = [(timestamp(item["finishedAt"]) - timestamp(item["startedAt"])) * 1000
                      for item in attempts if item["status"] == "COMPLETED" and item.get("startedAt") and item.get("finishedAt")]
    result["correctnessVerified"] = (not result["errors"] and len(submissions) == args.workflows
                                     and statuses["COMPLETED"] == args.workflows and len(details) == args.workflows)
    result["timing"] = clock.report(list({**submissions, **finished}.values()), all_tasks, attempts) if clock else {
        "consistent": False, "issues": ["Execution never started; setup failed"]}
    result["timingVerified"] = result["timing"]["consistent"]
    for issue in result["timing"]["issues"]:
        result["errors"].append({"phase": "timing", "error": issue})
    result["verified"] = result["correctnessVerified"] and result["timingVerified"]
    result["summary"] = {
        "workflowsRequested": args.workflows, "workflowsSubmitted": len(submissions),
        "workflowsCompleted": statuses["COMPLETED"], "workflowsFailed": statuses["FAILED"],
        "workflowsCancelled": statuses["CANCELLED"], "workflowsUnfinished": len(submissions) - len(finished),
        "totalTasks": sum(int(item.get("totalTasks", 0)) for item in submissions.values()),
        "verifiedTasks": len(all_tasks), "completedTasks": sum(item["status"] == "COMPLETED" for item in all_tasks),
        "workflowsPerSecond": statuses["COMPLETED"] / duration if result["verified"] else None,
        "tasksPerSecond": sum(item["status"] == "COMPLETED" for item in all_tasks) / duration if result["verified"] else None,
        "p50WorkflowLatencyMs": percentile(latencies, 0.5), "p95WorkflowLatencyMs": percentile(latencies, 0.95),
        "p50TaskDurationMs": percentile(task_durations, 0.5), "p95TaskDurationMs": percentile(task_durations, 0.95),
        "failureRate": statuses["FAILED"] / len(submissions) if submissions else None,
        "submissionErrorCount": sum(item["phase"] == "submit" for item in result["errors"]),
        "attemptCount": len(attempts), "retryCount": retries,
        "retryRate": retries / len(attempts) if attempts else None,
        "dependencyEdgesChecked": sum(item["dependencyEdgesChecked"] for item in details),
        "backoffIntervalsChecked": sum(len(item["retryDelays"]) for item in details),
        "duplicateSideEffectCount": None if args.skip_payment_verification or len(details) != len(submissions)
            else sum(item["duplicateSideEffectCount"] for item in details),
        "workerRecoveryTimeMs": None,
    }
    result["workflowDetails"] = details
    result["finishedAt"] = utc_now()
    write_result(args.output, result)
    print(json.dumps(result["summary"], indent=2))
    print(f"Measured result: {args.output}; correctnessVerified={result['correctnessVerified']}; "
          f"timingVerified={result['timingVerified']}; verified={result['verified']}")
    return 0 if result["verified"] else 1


if __name__ == "__main__":
    sys.exit(main())
