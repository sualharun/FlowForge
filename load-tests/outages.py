#!/usr/bin/env python3
"""Opt-in Kafka/PostgreSQL outages and paused-worker recovery on this Compose project only."""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid

from flowforge_client import Client, assert_dependencies, task, utc_now, write_result
from resilience import Docker, await_api, reachable, wait_tasks, wait_terminal


def completed(client, workflow_id, timeout):
    final = wait_terminal(client, workflow_id, timeout)
    if final["status"] != "COMPLETED":
        raise AssertionError(f"Workflow did not recover: {final}")
    tasks = client.request(f"/api/workflows/{workflow_id}/tasks")
    attempts = client.request(f"/api/workflows/{workflow_id}/attempts")
    payments = client.request(f"/api/admin/payments?workflowId={workflow_id}")
    expected = {item["id"] for item in tasks if item["taskType"] == "MOCK_PAYMENT"}
    if len(payments) != len(expected) or {p["taskId"] for p in payments} != expected:
        raise AssertionError("Payment ledger does not match the recovered workflow")
    return {"workflow": final, "tasks": tasks, "attempts": attempts,
            "dependencyEdgesChecked": assert_dependencies(tasks), "payments": payments}


def payment_after_delay(name, milliseconds, timeout_ms):
    return {"name": name + "-" + uuid.uuid4().hex[:8], "tasks": [
        task("wait", payload={"durationMs": milliseconds}, timeoutMs=timeout_ms, maxRetries=3),
        task("charge", "MOCK_PAYMENT", {"amount": 1, "currency": "USD"}, ["wait"])]}


def kafka_outage(client, docker, timeout):
    containers = docker.containers("kafka")
    before = client.submit(payment_after_delay("audit-kafka", 4000, 30000))
    wait_tasks(client, before["id"], lambda items: items[0]["status"] == "RUNNING", timeout)
    try:
        docker.run(["stop", "--time", "5", *containers])
        during = client.submit({"name": "audit-during-kafka-outage", "tasks": [
            task("charge", "MOCK_PAYMENT", {"amount": 2, "currency": "USD"})]})
        deadline = time.monotonic() + timeout
        received = []
        while time.monotonic() < deadline:
            attempts = client.request(f"/api/workflows/{before['id']}/attempts")
            received = [a for a in attempts if a.get("resultReceivedAt")]
            if received:
                break
            time.sleep(0.25)
        if not received:
            raise AssertionError("Worker did not persist its result while Kafka was unavailable")
        if client.request(f"/api/workflows/{before['id']}")["status"] != "RUNNING":
            raise AssertionError("Workflow advanced without consuming its Kafka result notification")
        metrics = client.request("/api/metrics/summary")
        if metrics["pendingOutboxMessages"] < 1:
            raise AssertionError("Outage did not leave durable unpublished outbox messages")
        readiness = client.request("/actuator/health/readiness")
    finally:
        docker.run(["start", *containers])
    return {"verified": True, "resultDurableDuringOutage": received,
            "outageMetrics": metrics, "readinessDuringOutage": readiness,
            "recovered": [completed(client, before["id"], timeout), completed(client, during["id"], timeout)]}


def postgres_outage(client, docker, timeout):
    containers = docker.containers("postgres")
    workflow = client.submit(payment_after_delay("audit-postgres", 8000, 12000))
    wait_tasks(client, workflow["id"], lambda items: items[0]["status"] == "RUNNING", timeout)
    before = client.request(f"/api/workflows/{workflow['id']}/history")
    try:
        docker.run(["stop", "--time", "5", *containers])
        unavailable = not reachable(Client(client.base_url, request_timeout=2))
        if not unavailable:
            raise AssertionError("API remained ready with its authoritative database stopped")
    finally:
        docker.run(["start", *containers])
    await_api(client, timeout)
    recovered = completed(client, workflow["id"], timeout)
    after = client.request(f"/api/workflows/{workflow['id']}/history")
    if not {e["id"] for e in before}.issubset({e["id"] for e in after}):
        raise AssertionError("Database restart lost existing execution history")
    return {"verified": True, "readinessUnavailableDuringOutage": unavailable,
            "historyBefore": len(before), "historyAfter": len(after), "recovered": recovered}


def paused_worker(client, docker, timeout):
    containers = docker.containers("worker")
    if len(containers) < 2:
        raise RuntimeError("Paused-worker test requires at least two worker replicas")
    workflow = client.submit(payment_after_delay("audit-paused-worker", 8000, 10000))
    tasks = wait_tasks(client, workflow["id"], lambda items: items[0]["status"] == "RUNNING", timeout)
    running = tasks[0]
    victim = None
    for container in containers:
        # Log command merges stdout/stderr so structured startup output is available on either stream.
        import subprocess
        logs = subprocess.run(["docker", "logs", "--tail", "500", container], text=True, capture_output=True, check=True)
        if running["workerId"] in logs.stdout + logs.stderr:
            victim = container
            break
    if victim is None:
        raise RuntimeError("No verified worker container matched the active worker ID")
    try:
        docker.run(["pause", victim])
        wait_tasks(client, workflow["id"], lambda items: items[0]["attemptCount"] > 1
                   and items[0]["status"] in {"RUNNING", "COMPLETED"}
                   and items[0].get("workerId") != running["workerId"], timeout)
    finally:
        docker.run(["unpause", victim])
    recovered = completed(client, workflow["id"], timeout)
    old = next(a for a in recovered["attempts"] if a["executionId"] == running["executionId"])
    if old["status"] not in {"TIMED_OUT", "FAILED"}:
        raise AssertionError(f"Paused execution overwrote its superseded attempt: {old}")
    return {"verified": True, "pausedContainer": victim, "oldExecutionId": running["executionId"],
            "oldAttemptStatus": old["status"], "recovered": recovered}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--project-name", default="flowforge")
    parser.add_argument("--scenario", choices=["kafka", "postgres", "paused-worker", "all"], default="all")
    parser.add_argument("--allow-service-restart", action="store_true")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not args.allow_service_restart:
        parser.error("--allow-service-restart is required")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    result = {"schemaVersion": 1, "startedAt": utc_now(), "configuration": vars(args), "checks": {}, "errors": []}
    client, docker = Client(args.base_url), Docker(args.project_name)
    for name, test in [("kafka", kafka_outage), ("postgres", postgres_outage), ("paused-worker", paused_worker)]:
        if args.scenario not in {name, "all"}:
            continue
        print(f"Running {name}", flush=True)
        try:
            result["checks"][name] = test(client, docker, args.timeout)
        except Exception as error:
            result["errors"].append({"scenario": name, "error": str(error)})
            print(f"FAILED {name}: {error}", file=sys.stderr, flush=True)
    result["verified"] = not result["errors"] and bool(result["checks"])
    result["finishedAt"] = utc_now()
    write_result(args.output, result)
    print(f"Recorded {args.output}; verified={result['verified']}")
    return 0 if result["verified"] else 1


if __name__ == "__main__":
    sys.exit(main())
