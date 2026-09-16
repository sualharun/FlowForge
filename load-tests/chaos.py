#!/usr/bin/env python3
"""Real API, Kafka redelivery, and opt-in Docker worker-failure checks."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

from flowforge_client import Client, assert_dependencies, task, timestamp, utc_now, write_result

ROOT = Path(__file__).resolve().parents[1]


class Docker:
    def __init__(self, project):
        self.project = project

    def run(self, args, input_data=None, timeout=60):
        return subprocess.run(["docker", *args], input=input_data, capture_output=True,
                              text=True, check=True, timeout=timeout).stdout.strip()

    def compose(self, *args, input_data=None):
        return self.run(["compose", "-f", str(ROOT / "docker-compose.yml"), "-p", self.project, *args], input_data)

    def workers(self):
        ids = self.compose("ps", "--status", "running", "-q", "worker").splitlines()
        for container in ids:
            labels = json.loads(self.run(["inspect", "--format", "{{json .Config.Labels}}", container]))
            if labels.get("com.docker.compose.project") != self.project or labels.get("com.docker.compose.service") != "worker":
                raise RuntimeError(f"Refusing to affect container outside {self.project}/worker")
        return ids


def wait_tasks(client, workflow_id, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        tasks = client.request(f"/api/workflows/{workflow_id}/tasks")
        if predicate(tasks):
            return tasks
        time.sleep(0.2)
    raise TimeoutError(f"Task condition not reached: {workflow_id}")


def smoke(client, timeout):
    prefix = uuid.uuid4().hex[:10]
    cycle = {"name": "chaos-cycle-validation", "tasks": [task("a", dependencies=["b"]), task("b", dependencies=["a"])]}
    try:
        client.submit(cycle)
    except RuntimeError as error:
        if "HTTP 400" not in str(error):
            raise
    else:
        raise AssertionError("API accepted a cyclic DAG")
    definition = {"name": "chaos-parallel-" + prefix, "concurrencyLimit": 4,
                  "tasks": [task(f"parallel-{n}", payload={"durationMs": 1000}) for n in range(4)] +
                           [task("join", dependencies=[f"parallel-{n}" for n in range(4)])]}
    workflow = client.submit(definition)
    completed = client.wait(workflow["id"], timeout)
    if completed["status"] != "COMPLETED":
        raise AssertionError(f"Parallel DAG failed: {completed}")
    tasks = client.request(f"/api/workflows/{workflow['id']}/tasks")
    assert_dependencies(tasks)
    independent = [item for item in tasks if item["name"].startswith("parallel-")]
    overlap = any(max(timestamp(a["startedAt"]), timestamp(b["startedAt"])) <
                  min(timestamp(a["finishedAt"]), timestamp(b["finishedAt"]))
                  for i, a in enumerate(independent) for b in independent[i + 1:])
    if not overlap:
        raise AssertionError("Independent tasks never overlapped")

    timed = client.submit({"name": "chaos-timeout-" + prefix, "tasks": [
        task("timeout", payload={"durationMs": 2000}, timeoutMs=100, maxRetries=1)]})
    terminal = client.wait(timed["id"], timeout)
    attempts = client.request(f"/api/workflows/{timed['id']}/attempts")
    if terminal["status"] != "FAILED" or len(attempts) != 2 or any(item["status"] != "TIMED_OUT" for item in attempts):
        raise AssertionError(f"Timeout/retry exhaustion not preserved: {terminal}, {attempts}")
    dead_letters = client.request("/api/dead-letters?limit=500")
    if not any(item["workflowId"] == timed["id"] for item in dead_letters):
        raise AssertionError("Exhausted timeout task missing from dead-letter view")

    cancelled = client.submit({"name": "chaos-cancel-" + prefix, "tasks": [
        task("active", payload={"durationMs": 5000}),
        task("must-not-charge", "MOCK_PAYMENT", {"orderId": prefix, "amount": 1, "currency": "USD"}, ["active"])]})
    wait_tasks(client, cancelled["id"], lambda items: any(item["status"] == "RUNNING" for item in items), timeout)
    client.request(f"/api/workflows/{cancelled['id']}/cancel", {}, method="POST")
    final = client.wait(cancelled["id"], timeout)
    cancelled_tasks = client.request(f"/api/workflows/{cancelled['id']}/tasks")
    if final["status"] != "CANCELLED" or any(item["status"] != "CANCELLED" for item in cancelled_tasks):
        raise AssertionError("Cancellation did not propagate to unfinished tasks")
    time.sleep(1)  # Let a worker observe the cancellation and try to report its result.
    if client.request(f"/api/admin/payments?workflowId={cancelled['id']}"):
        raise AssertionError("Cancelled downstream payment produced a side effect")
    history = client.request(f"/api/workflows/{cancelled['id']}/history")
    if not any(item["type"] == "WORKFLOW_CANCELLED" for item in history):
        raise AssertionError("Cancellation history missing")
    return {"verified": True, "cycleRejected": True, "parallelTasksOverlapped": overlap,
            "workflowIds": [workflow["id"], timed["id"], cancelled["id"]],
            "timeoutAttempts": attempts, "deadLetterPresent": True, "cancelledDownstreamPayments": 0}


def duplicate_delivery(client, docker, timeout, copies):
    workflow = client.submit({"name": "chaos-duplicate-" + uuid.uuid4().hex[:10], "tasks": [
        task("charge", "MOCK_PAYMENT", {"orderId": uuid.uuid4().hex, "amount": 12.50, "currency": "USD"})]})
    if client.wait(workflow["id"], timeout)["status"] != "COMPLETED":
        raise AssertionError("Initial payment did not complete")
    item = client.request(f"/api/workflows/{workflow['id']}/tasks")[0]
    before = client.request(f"/api/workflows/{workflow['id']}/attempts")
    # Same execution identifier and envelope fields as the original dispatch. These
    # records go through the actual broker and the production worker consumer.
    envelope = {key: item[key] for key in ["workflowId", "executionId", "taskType", "payload", "timeoutMs", "maxRetries", "initialRetryDelayMs", "backoffMultiplier"]}
    envelope.update({"taskId": item["id"], "attemptNumber": item["attemptCount"], "createdAt": item["createdAt"]})
    encoded = item["executionId"] + "|" + json.dumps(envelope, separators=(",", ":")) + "\n"
    docker.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
                   "--bootstrap-server", "kafka:9092", "--topic", "flowforge.tasks.ready",
                   "--property", "parse.key=true", "--property", "key.separator=|", input_data=encoded * copies)
    # Observe group lag at zero, so a fast ledger query cannot race unconsumed duplicates.
    deadline = time.monotonic() + timeout
    lag_samples = []
    while time.monotonic() < deadline:
        listing = docker.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                                 "--bootstrap-server", "kafka:9092", "--all-groups", "--describe")
        rows = [line.split() for line in listing.splitlines()
                if "flowforge.tasks.ready" in line and not line.lstrip().startswith("GROUP")]
        # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        lags = [int(row[5]) for row in rows if len(row) > 5 and row[5].isdigit()]
        lag_samples.append({"at": utc_now(), "lag": sum(lags) if lags else None})
        if lags and sum(lags) == 0:
            break
        time.sleep(0.5)
    else:
        raise AssertionError("Duplicate records were not drained by a consumer group")
    after = client.request(f"/api/workflows/{workflow['id']}/attempts")
    payments = client.request(f"/api/admin/payments?workflowId={workflow['id']}")
    if len(payments) != 1 or len(after) != len(before) or after[0]["executionId"] != before[0]["executionId"]:
        raise AssertionError(f"Duplicate delivery mutated the execution/ledger: {after}, {payments}")
    return {"verified": True, "workflowId": workflow["id"], "executionId": item["executionId"],
            "duplicateMessagesPublished": copies, "paymentLedgerEntries": len(payments),
            "duplicateSideEffectCount": max(0, len(payments) - 1), "attemptsBefore": len(before),
            "attemptsAfter": len(after), "consumerLagSamples": lag_samples}


def worker_failure(client, docker, timeout):
    containers = docker.workers()
    if len(containers) < 2:
        raise RuntimeError("Worker failure test requires at least two running Compose worker replicas")
    workflow = client.submit({"name": "chaos-worker-failure-" + uuid.uuid4().hex[:10], "concurrencyLimit": 32,
                              "tasks": [task(f"recover-{n}", payload={"durationMs": 10000}, timeoutMs=60000, maxRetries=3)
                                        for n in range(max(16, len(containers) * 8))]})
    tasks = wait_tasks(client, workflow["id"],
                       lambda items: len({item.get("workerId") for item in items if item["status"] == "RUNNING"}) >= len(containers), timeout)
    active_workers = {item["workerId"] for item in tasks if item["status"] == "RUNNING"}
    victim = None
    victim_worker = None
    for container in containers:
        logs = subprocess.run(["docker", "logs", "--tail", "2000", container], capture_output=True, text=True, check=True)
        combined = logs.stdout + logs.stderr
        matches = [worker for worker in active_workers if worker in combined]
        if len(matches) == 1:
            victim, victim_worker = container, matches[0]
            break
    if victim is None:
        raise RuntimeError("Could not map active worker IDs to container logs; no container was killed")
    abandoned = {item["id"] for item in tasks if item["workerId"] == victim_worker and item["status"] == "RUNNING"}
    killed_at = time.time()
    killed_monotonic = time.monotonic()
    docker.run(["kill", "--signal", "KILL", victim])
    try:
        # Detection latency is measured to the first replacement attempt actually starting.
        # The task-level startedAt remains the first start across retries; use the
        # immutable attempt history to locate the replacement execution.
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            observed_attempts = client.request(f"/api/workflows/{workflow['id']}/attempts")
            candidates = [item for item in observed_attempts
                          if item["taskId"] in abandoned and item["attemptNumber"] > 1
                          and item.get("workerId") != victim_worker and item.get("startedAt")
                          and timestamp(item["startedAt"]) >= killed_at]
            if candidates:
                break
            time.sleep(0.2)
        else:
            raise TimeoutError("No replacement attempt started after killing the worker")
        recovery_ms = (min(timestamp(item["startedAt"]) for item in candidates) - killed_at) * 1000
        observed_ms = (time.monotonic() - killed_monotonic) * 1000
        terminal = client.wait(workflow["id"], timeout)
        attempts = client.request(f"/api/workflows/{workflow['id']}/attempts")
        recovered_failures = [item for item in attempts if item["taskId"] in abandoned
                              and "heartbeat" in (item.get("error") or "").lower()]
        if terminal["status"] != "COMPLETED" or not recovered_failures:
            raise AssertionError(f"Worker heartbeat recovery did not complete: {terminal}, {attempts}")
        return {"verified": True, "workflowId": workflow["id"], "killedContainer": victim,
                "killedWorkerId": victim_worker, "abandonedTaskIds": sorted(abandoned),
                "heartbeatExpiredAttempts": len(recovered_failures), "workerRecoveryTimeMs": recovery_ms,
                "clientObservedRecoveryTimeMs": observed_ms, "attempts": attempts}
    finally:
        docker.run(["start", victim])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--project-name", default="flowforge")
    parser.add_argument("--scenario", choices=["smoke", "duplicate", "worker-failure", "all"], default="smoke")
    parser.add_argument("--allow-worker-kill", action="store_true", help="Authorize SIGKILL of one identified worker in this Compose project")
    parser.add_argument("--copies", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", default="load-tests/results/chaos.json")
    args = parser.parse_args()
    if args.scenario in {"worker-failure", "all"} and not args.allow_worker_kill:
        parser.error("--allow-worker-kill is required for worker-failure/all")
    if args.copies < 1 or args.timeout <= 0:
        parser.error("--copies and --timeout must be positive")
    client, docker = Client(args.base_url), Docker(args.project_name)
    result = {"schemaVersion": 1, "startedAt": utc_now(), "configuration": vars(args), "checks": {}, "errors": [], "verified": False}
    for name, function in [("smoke", lambda: smoke(client, args.timeout)),
                           ("duplicate", lambda: duplicate_delivery(client, docker, args.timeout, args.copies)),
                           ("worker-failure", lambda: worker_failure(client, docker, args.timeout))]:
        if args.scenario not in {name, "all"}:
            continue
        print(f"Running {name}", flush=True)
        try:
            result["checks"][name] = function()
        except Exception as error:
            result["errors"].append({"scenario": name, "error": str(error)})
            print(f"FAILED {name}: {error}", file=sys.stderr, flush=True)
    result["verified"] = not result["errors"] and bool(result["checks"])
    result["finishedAt"] = utc_now()
    write_result(args.output, result)
    print(f"Measured result: {args.output}; verified={result['verified']}")
    return 0 if result["verified"] else 1


if __name__ == "__main__":
    sys.exit(main())
