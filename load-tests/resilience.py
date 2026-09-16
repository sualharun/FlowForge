#!/usr/bin/env python3
"""Restart-persistence and Redis-degradation checks against the real Compose stack.

Both scenarios interrupt live infrastructure, so every container this script
touches is label-verified as belonging to this Compose project and is restored in
a finally block. Nothing outside ``<project>/{api,scheduler,redis}`` is affected.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import uuid

from flowforge_client import (Client, TERMINAL, assert_dependencies, task, timestamp,
                              utc_now, write_result)

ROOT = Path(__file__).resolve().parents[1]


class Docker:
    def __init__(self, project):
        self.project = project

    def run(self, args, timeout=180):
        return subprocess.run(["docker", *args], capture_output=True, text=True,
                              check=True, timeout=timeout).stdout.strip()

    def compose(self, *args, timeout=180):
        return self.run(["compose", "-f", str(ROOT / "docker-compose.yml"),
                         "-p", self.project, *args], timeout=timeout)

    def containers(self, service):
        """Container IDs for one service, refusing anything outside this project."""
        ids = self.compose("ps", "-a", "-q", service).splitlines()
        if not ids:
            raise RuntimeError(f"No {self.project}/{service} container is present")
        for container in ids:
            labels = json.loads(self.run(["inspect", "--format", "{{json .Config.Labels}}", container]))
            if (labels.get("com.docker.compose.project") != self.project
                    or labels.get("com.docker.compose.service") != service):
                raise RuntimeError(f"Refusing to affect container outside {self.project}/{service}")
        return ids

    def verify_scope(self, services):
        return {service: self.containers(service) for service in services}


def reachable(client):
    """True when the API answers a readiness probe; connection loss is not an error here."""
    try:
        client.request("/actuator/health/readiness")
        return True
    except (urllib.error.URLError, OSError, RuntimeError):
        return False


def await_api(client, timeout):
    """Wait for the API to serve readiness again and return the observed outage seconds."""
    started = time.monotonic()
    deadline = started + timeout
    while time.monotonic() < deadline:
        if reachable(client):
            return time.monotonic() - started
        time.sleep(0.5)
    raise TimeoutError("API did not become ready again")


def tolerant_get(client, path, timeout=120):
    """GET is idempotent, so retrying across a service restart cannot duplicate work."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            return client.request(path)
        except (urllib.error.URLError, OSError, RuntimeError) as error:
            last = error
            time.sleep(0.5)
    raise TimeoutError(f"GET {path} never succeeded: {last}")


def wait_terminal(client, workflow_id, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        workflow = tolerant_get(client, f"/api/workflows/{workflow_id}")
        if workflow["status"] in TERMINAL:
            return workflow
        time.sleep(0.5)
    raise TimeoutError(f"Workflow {workflow_id} did not finish within {timeout}s")


def wait_tasks(client, workflow_id, predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        tasks = tolerant_get(client, f"/api/workflows/{workflow_id}/tasks")
        if predicate(tasks):
            return tasks
        time.sleep(0.25)
    raise TimeoutError(f"Task condition not reached for {workflow_id}")


def sequential_workflow(name, steps, step_ms, order_id):
    """A dependency chain, so progress after a restart requires fresh scheduling."""
    tasks = [task("step-0", payload={"durationMs": step_ms}, timeoutMs=60000)]
    for index in range(1, steps):
        tasks.append(task(f"step-{index}", payload={"durationMs": step_ms}, timeoutMs=60000,
                          dependencies=[f"step-{index - 1}"]))
    tasks.append(task("charge", "MOCK_PAYMENT", {"orderId": order_id, "amount": 31.50, "currency": "USD"},
                      [f"step-{steps - 1}"], timeoutMs=60000))
    tasks.append(task("confirm", "DATA_TRANSFORM", {"operation": "UPPERCASE", "data": "recovered"},
                      ["charge"], timeoutMs=60000))
    return {"name": name, "concurrencyLimit": 4, "tasks": tasks}


def restart_persistence(client, docker, timeout, steps, step_ms):
    """Restart the API and scheduler mid-execution; PostgreSQL must carry the workflow."""
    scope = docker.verify_scope(["api", "scheduler"])
    order_id = uuid.uuid4().hex
    definition = sequential_workflow("resilience-restart-" + order_id[:10], steps, step_ms, order_id)
    workflow = client.submit(definition)
    workflow_id = workflow["id"]

    # Interrupt while the chain is genuinely unfinished: something running, something pending.
    wait_tasks(client, workflow_id,
               lambda items: any(item["status"] == "RUNNING" for item in items)
               and any(item["status"] == "PENDING" for item in items), timeout)
    before_tasks = tolerant_get(client, f"/api/workflows/{workflow_id}/tasks")
    before_history = tolerant_get(client, f"/api/workflows/{workflow_id}/history")
    before_event_ids = {event["id"] for event in before_history}
    before_completed = [item["name"] for item in before_tasks if item["status"] == "COMPLETED"]
    restarted_at = utc_now()
    restart_monotonic = time.monotonic()

    try:
        # `compose restart` stops and starts only these two services' containers.
        docker.compose("restart", "api", "scheduler", timeout=300)
    finally:
        docker.compose("start", "api", "scheduler", timeout=300)
    outage_seconds = await_api(client, timeout)
    restart_wall_seconds = time.monotonic() - restart_monotonic

    terminal = wait_terminal(client, workflow_id, timeout)
    after_tasks = tolerant_get(client, f"/api/workflows/{workflow_id}/tasks")
    after_history = tolerant_get(client, f"/api/workflows/{workflow_id}/history")
    after_event_ids = {event["id"] for event in after_history}
    payments = tolerant_get(client, f"/api/admin/payments?workflowId={workflow_id}")
    attempts = tolerant_get(client, f"/api/workflows/{workflow_id}/attempts")

    if terminal["status"] != "COMPLETED":
        raise AssertionError(f"Workflow did not survive the restart: {terminal}, {after_tasks}")
    missing = before_event_ids - after_event_ids
    if missing:
        raise AssertionError(f"Pre-restart history was lost: {sorted(missing)}")
    progressed = [event for event in after_history
                  if timestamp(event["createdAt"]) > timestamp(restarted_at)]
    if not progressed:
        raise AssertionError("No execution events were recorded after the restart")
    scheduled_after = [item["name"] for item in after_tasks
                       if item["name"] not in before_completed and item["status"] == "COMPLETED"]
    if not scheduled_after:
        raise AssertionError("No task completed after the restart; scheduling did not resume")
    if len(payments) != 1:
        raise AssertionError(f"Restart changed the payment ledger: {payments}")
    dependency_edges = assert_dependencies(after_tasks)

    return {"verified": True, "workflowId": workflow_id, "restartedServices": sorted(scope),
            "restartedContainers": scope, "restartedAt": restarted_at,
            "apiOutageSecondsObserved": outage_seconds, "restartWallSeconds": restart_wall_seconds,
            "historyEventsBeforeRestart": len(before_history),
            "historyEventsAfterRestart": len(after_history),
            "historyEventsRecordedAfterRestart": len(progressed),
            "preRestartEventsRetained": len(before_event_ids),
            "tasksCompletedBeforeRestart": sorted(before_completed),
            "tasksCompletedAfterRestart": sorted(scheduled_after),
            "dependencyEdgesChecked": dependency_edges,
            "paymentLedgerEntries": len(payments),
            "duplicateSideEffectCount": max(0, len(payments) - 1),
            "attemptCount": len(attempts), "terminalStatus": terminal["status"],
            "workflowLatencyMs": (timestamp(terminal["finishedAt"]) - timestamp(terminal["createdAt"])) * 1000}


def redis_degradation(client, docker, timeout, width):
    """Stop Redis and require workflows to keep executing on PostgreSQL fencing alone."""
    scope = docker.verify_scope(["redis"])
    order_id = uuid.uuid4().hex
    stopped_at = None
    readiness_samples = []
    try:
        docker.compose("stop", "redis", timeout=180)
        stopped_at = utc_now()
        if not reachable(client):
            # The readiness group is readinessState+db on purpose; Redis is advisory.
            raise AssertionError("API readiness depends on Redis; degradation is not graceful")
        readiness_samples.append({"at": utc_now(), "ready": True})

        # Submitted entirely during the outage: admission, dispatch, claiming, results.
        definition = {"name": "resilience-redis-" + order_id[:10], "concurrencyLimit": width,
                      "tasks": [task(f"parallel-{n}", payload={"durationMs": 700}, timeoutMs=60000)
                                for n in range(width)]
                      + [task("charge", "MOCK_PAYMENT",
                              {"orderId": order_id, "amount": 17.25, "currency": "USD"},
                              [f"parallel-{n}" for n in range(width)], timeoutMs=60000)]}
        workflow = client.submit(definition)
        workflow_id = workflow["id"]
        terminal = wait_terminal(client, workflow_id, timeout)
        readiness_samples.append({"at": utc_now(), "ready": reachable(client)})
        tasks = tolerant_get(client, f"/api/workflows/{workflow_id}/tasks")
        payments = tolerant_get(client, f"/api/admin/payments?workflowId={workflow_id}")
        attempts = tolerant_get(client, f"/api/workflows/{workflow_id}/attempts")
        history = tolerant_get(client, f"/api/workflows/{workflow_id}/history")
        workers = tolerant_get(client, "/api/workers")

        if terminal["status"] != "COMPLETED":
            raise AssertionError(f"Workflow stalled during the Redis outage: {terminal}, {tasks}")
        if len(payments) != 1:
            raise AssertionError(f"Redis outage disturbed the payment ledger: {payments}")
        dependency_edges = assert_dependencies(tasks)
        overlapping = [item for item in tasks if item["name"].startswith("parallel-")]
        concurrent = any(max(timestamp(a["startedAt"]), timestamp(b["startedAt"]))
                         < min(timestamp(a["finishedAt"]), timestamp(b["finishedAt"]))
                         for i, a in enumerate(overlapping) for b in overlapping[i + 1:])
        if not concurrent:
            raise AssertionError("No parallel overlap during the Redis outage")
        result = {"verified": True, "workflowId": workflow_id, "stoppedServices": sorted(scope),
                  "stoppedContainers": scope, "redisStoppedAt": stopped_at,
                  "submittedDuringOutage": True, "terminalStatus": terminal["status"],
                  "apiReadinessDuringOutage": readiness_samples,
                  "parallelTasksOverlapped": concurrent, "taskCount": len(tasks),
                  "dependencyEdgesChecked": dependency_edges, "attemptCount": len(attempts),
                  "historyEvents": len(history), "paymentLedgerEntries": len(payments),
                  "duplicateSideEffectCount": max(0, len(payments) - 1),
                  "workersReportedDuringOutage": len(workers),
                  "workflowLatencyMs": (timestamp(terminal["finishedAt"]) - timestamp(terminal["createdAt"])) * 1000}
    finally:
        docker.compose("start", "redis", timeout=180)
        # Confirm the dependency is genuinely healthy again before the next scenario.
        restored = False
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            state = docker.compose("ps", "--format", "{{.Service}} {{.Health}}")
            if any(line.startswith("redis") and "healthy" in line for line in state.splitlines()):
                restored = True
                break
            time.sleep(1)
        restored_at, restored_healthy = utc_now(), restored
    # Only reached when the body succeeded; a failure propagates after the restore above.
    result["redisRestoredAt"] = restored_at
    result["redisHealthyAfterRestore"] = restored_healthy
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--project-name", default="flowforge")
    parser.add_argument("--scenario", choices=["restart", "redis-outage", "all"], default="all")
    parser.add_argument("--allow-service-restart", action="store_true",
                        help="Authorize restarting api/scheduler and stopping redis in this Compose project")
    parser.add_argument("--steps", type=int, default=6, help="Sequential DELAY steps before the payment")
    parser.add_argument("--step-ms", type=int, default=2500, help="Duration of each sequential step")
    parser.add_argument("--width", type=int, default=8, help="Parallel tasks in the Redis-outage workflow")
    parser.add_argument("--timeout", type=float, default=420)
    parser.add_argument("--output", default="load-tests/results/resilience.json")
    args = parser.parse_args()
    if not args.allow_service_restart:
        parser.error("--allow-service-restart is required: this test interrupts running containers")
    if args.steps < 2 or args.step_ms < 1 or args.width < 2 or args.timeout <= 0:
        parser.error("--steps/--step-ms/--width/--timeout must be positive (steps>=2, width>=2)")

    client, docker = Client(args.base_url), Docker(args.project_name)
    result = {"schemaVersion": 1, "startedAt": utc_now(), "configuration": vars(args),
              "checks": {}, "errors": [], "verified": False}
    for name, function in [("restart", lambda: restart_persistence(client, docker, args.timeout,
                                                                   args.steps, args.step_ms)),
                           ("redis-outage", lambda: redis_degradation(client, docker, args.timeout, args.width))]:
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
