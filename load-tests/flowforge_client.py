"""Small standard-library client shared by the real-system test runners."""
from __future__ import annotations

import datetime as dt
import json
import math
import time
import urllib.error
import urllib.request
from pathlib import Path

TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


class Client:
    def __init__(self, base_url="http://localhost:8088", request_timeout=30):
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout

    def request(self, path, data=None, method=None):
        body = None if data is None else json.dumps(data).encode()
        request = urllib.request.Request(
            self.base_url + path, data=body, method=method or ("POST" if body else "GET"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.request_timeout) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as error:
            detail = error.read().decode(errors="replace")[:1000]
            raise RuntimeError(f"{request.method} {path}: HTTP {error.code}: {detail}") from error

    def submit(self, definition):
        # Deliberately never retry a POST: response loss does not prove creation failed.
        return self.request("/api/workflows", definition)

    def wait(self, workflow_id, timeout=180, interval=0.25):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            workflow = self.request(f"/api/workflows/{workflow_id}")
            if workflow["status"] in TERMINAL:
                return workflow
            time.sleep(interval)
        raise TimeoutError(f"Workflow {workflow_id} did not finish within {timeout}s")


def task(name, task_type="DELAY", payload=None, dependencies=None, **overrides):
    result = {"name": name, "taskType": task_type,
              "payload": payload if payload is not None else {"durationMs": 20},
              "dependsOn": dependencies or [], "timeoutMs": 30000, "maxRetries": 3,
              "initialRetryDelayMs": 100, "backoffMultiplier": 2.0}
    result.update(overrides)
    return result


def timestamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() if value else None


def percentile(values, fraction):
    """Nearest-rank percentile; no interpolation and no fabricated empty value."""
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def assert_dependencies(tasks):
    by_id = {item["id"]: item for item in tasks}
    checked = 0
    for item in tasks:
        if not item.get("startedAt"):
            continue
        for parent_id in item.get("dependencies", []):
            parent = by_id[parent_id]
            if parent["status"] != "COMPLETED" or not parent.get("finishedAt"):
                raise AssertionError(f"Task {item['name']} ran before parent {parent['name']} succeeded")
            if timestamp(item["startedAt"]) + 0.001 < timestamp(parent["finishedAt"]):
                raise AssertionError(f"Dependency ordering violated: {parent['name']} -> {item['name']}")
            checked += 1
    return checked


def write_result(path, result):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    temporary.replace(destination)


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()
