#!/usr/bin/env python3
"""Submit and verify real workflows, then write measured results as JSON.

No dependency installation required. See --help and docs/benchmark-methodology.md.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import platform
import subprocess
import sys
import time
import uuid

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
    parser.add_argument("--timeout", type=float, default=1800, help="Whole-run wall time deadline in seconds")
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
    result = {"schemaVersion": 1, "startedAt": utc_now(), "configuration": vars(args),
              "environment": {"platform": platform.platform(), "python": platform.python_version()},
              "errors": [], "workflowIds": [], "timedOutWorkflowIds": [], "verified": False}
    try:
        result["environment"]["gitCommit"] = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        result["environment"]["gitCommit"] = None
    start = time.monotonic()
    run_id = uuid.uuid4().hex[:10]
    submissions, finished, details = {}, {}, []
    try:
        result["metricsBefore"] = client.request("/api/metrics/summary")
        result["workersBefore"] = client.request("/api/workers")
        with ThreadPoolExecutor(max_workers=args.submit_concurrency) as pool:
            futures = {pool.submit(client.submit, definition(args.scenario, n, run_id, args.width, args.layers, args.delay_ms)): n
                       for n in range(args.workflows)}
            for future in as_completed(futures):
                try:
                    value = future.result()
                    submissions[value["id"]] = value
                except Exception as error:
                    result["errors"].append({"phase": "submit", "index": futures[future], "error": str(error)})
        result["submissionDurationSeconds"] = time.monotonic() - start
        result["workflowIds"] = list(submissions)
        pending = set(submissions)
        with ThreadPoolExecutor(max_workers=args.poll_concurrency) as pool:
            while pending and time.monotonic() - start < args.timeout:
                futures = {pool.submit(client.request, f"/api/workflows/{workflow_id}"): workflow_id for workflow_id in pending}
                for future in as_completed(futures):
                    workflow = future.result()
                    if workflow["status"] in TERMINAL:
                        finished[workflow["id"]] = workflow
                        pending.remove(workflow["id"])
                print(f"{args.scenario}: {len(finished)}/{len(submissions)} terminal, elapsed {time.monotonic() - start:.1f}s", flush=True)
                if pending:
                    time.sleep(args.poll_interval)
            # Stop throughput timing before the read-only verification requests.
            result["executionDurationSeconds"] = time.monotonic() - start
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
    duration = result.get("executionDurationSeconds", time.monotonic() - start)
    statuses = Counter(item["status"] for item in finished.values())
    all_tasks = [item for workflow in details for item in workflow["tasks"]]
    attempts = [item for workflow in details for item in workflow["attempts"]]
    retries = sum(item["attemptNumber"] > 1 for item in attempts)
    latencies = [(timestamp(item["finishedAt"]) - timestamp(item["createdAt"])) * 1000
                 for item in finished.values() if item.get("finishedAt")]
    task_durations = [(timestamp(item["finishedAt"]) - timestamp(item["startedAt"])) * 1000
                      for item in attempts if item["status"] == "COMPLETED" and item.get("startedAt") and item.get("finishedAt")]
    result["summary"] = {
        "workflowsRequested": args.workflows, "workflowsSubmitted": len(submissions),
        "workflowsCompleted": statuses["COMPLETED"], "workflowsFailed": statuses["FAILED"],
        "workflowsCancelled": statuses["CANCELLED"], "workflowsUnfinished": len(submissions) - len(finished),
        "totalTasks": sum(int(item.get("totalTasks", 0)) for item in submissions.values()),
        "verifiedTasks": len(all_tasks), "completedTasks": sum(item["status"] == "COMPLETED" for item in all_tasks),
        "workflowsPerSecond": statuses["COMPLETED"] / duration if duration else None,
        "tasksPerSecond": sum(item["status"] == "COMPLETED" for item in all_tasks) / duration if duration else None,
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
    result["verified"] = (not result["errors"] and len(submissions) == args.workflows
                          and statuses["COMPLETED"] == args.workflows and len(details) == args.workflows)
    result["finishedAt"] = utc_now()
    write_result(args.output, result)
    print(json.dumps(result["summary"], indent=2))
    print(f"Measured result: {args.output}; verified={result['verified']}")
    return 0 if result["verified"] else 1


if __name__ == "__main__":
    sys.exit(main())
