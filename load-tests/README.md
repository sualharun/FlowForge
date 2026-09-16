# Real-system test runners

Requires Python 3.10+ and a running FlowForge API; no pip packages. The failure-injection scripts also require the Docker CLI and the repository's Compose project.

```bash
python3 load-tests/benchmark.py --help
python3 load-tests/chaos.py --help
python3 load-tests/resilience.py --help
```

The default API URL is `http://localhost:8088`. `benchmark.py` supports normal load, high concurrency, retry storms, and large layered DAGs. `chaos.py` verifies API invariants, actual Kafka redelivery, and opt-in worker SIGKILL/recovery. `resilience.py` restarts the API and scheduler mid-execution and separately stops Redis, requiring durable recovery and continued progress on PostgreSQL fencing alone. Every run writes measured JSON and exits nonzero on an unmet invariant. A smoke run starts at 10 workflows; the normal benchmark default is 1,000.

`chaos.py` needs `--allow-worker-kill` and `resilience.py` needs `--allow-service-restart`; both refuse to run otherwise. Before stopping or killing anything they read the target container's `com.docker.compose.project` and `com.docker.compose.service` labels and abort if it is not owned by this project, then restore it in a `finally` block. Unrelated containers on the same daemon are never touched.

See [benchmark methodology](../docs/benchmark-methodology.md) for commands, definitions, denominators, and limitations. Run scenarios sequentially against a dedicated environment. Keep unique output paths to preserve evidence.
