# Running and deploying FlowForge

## Docker Compose

Install Docker with Compose v2 and allocate enough memory for PostgreSQL, Kafka, four JVMs, and the frontend. The initial build downloads Java/Maven and npm dependencies. Run from the repository root:

```bash
docker compose up --build -d
docker compose ps
```

This starts PostgreSQL 16, Redis 7, Kafka 3.9.1 in single-node KRaft mode, one API, one scheduler, **two worker replicas**, and the React dashboard served by Nginx. Services wait for datastore health checks. Spring Boot services create the explicit Kafka topics and apply Flyway migrations at startup; Flyway coordinates concurrent schema migration attempts.

| Interface | Local address |
| --- | --- |
| Dashboard | http://localhost:3000 |
| API | http://localhost:8088/api |
| API readiness | http://localhost:8088/actuator/health/readiness |
| API Prometheus metrics | http://localhost:8088/actuator/prometheus |
| PostgreSQL | localhost:5433, database/user `flowforge` |
| Redis | localhost:6380 |
| Kafka host bootstrap server | localhost:29092 |

Published ports bind to loopback. The dashboard proxies `/api` and `/actuator` to the API on the internal Compose network. Kafka advertises separate internal (`kafka:9092`) and host (`localhost:29092`) listeners. Containers must use the internal addresses; advertising only `localhost` to other containers prevents consumer connectivity.

Optional configuration:

```bash
cp .env.example .env
# Edit .env before the first startup if you need different ports or limits.
docker compose config --quiet
docker compose up --build -d
```

`.env` defaults are deliberately for local development. `DB_PASSWORD` is shared by PostgreSQL and applications. Changing it after PostgreSQL has initialized an existing volume does not change the existing database role's password; rotate the role's password explicitly. `DB_PORT`, `API_PORT`, `FRONTEND_PORT`, `REDIS_PORT`, and `KAFKA_EXTERNAL_PORT` change host ports. Application-to-container Redis remains on port 6379.

### Concurrency and scaling

```bash
docker compose up -d --scale worker=4
docker compose logs --tail=100 worker scheduler
```

Each worker defaults to four concurrent task executions. Global admission defaults to 32 READY/RUNNING tasks across all workflows. A workflow's `concurrencyLimit` applies within that global bound. Twelve partitions per task topic allow parallel consumers; useful scaling is bounded by partition ownership, local execution slots, database throughput, and global admission. Changing replica count alone does not raise those limits. Redis is an optimization/cache, while PostgreSQL retains authoritative admission and execution state.

Useful environment variables for backend deployments:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` | Local Compose datastore | JDBC connection and credentials |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` in Compose | Broker discovery address |
| `REDIS_HOST` / `REDIS_PORT` | `redis` / `6379` in Compose | Cache endpoint |
| `FLOWFORGE_WORKER_CONCURRENCY` | `4` | Execution slots per worker |
| `FLOWFORGE_GLOBAL_CONCURRENCY` | `32` | Global READY/RUNNING admission bound |
| `WORKER_TIMEOUT_MS` | `15000` | Time since last persisted heartbeat before recovery |
| `KAFKA_TOPIC_PARTITIONS` | `12` | Partition count when creating topics |
| `KAFKA_TOPIC_REPLICAS` | `1` locally | Replication factor when creating topics |
| `PORT` | API 8080, scheduler 8081, worker 8082 | Internal HTTP port |
| `JAVA_TOOL_OPTIONS` | Max heap 70% of container memory | JVM settings |

The Compose file passes its supported `.env` settings explicitly. To pass additional Spring environment properties, add them to a Compose override or your Kubernetes ConfigMap/Secret. Do not expose credentials through command-line arguments or committed configuration.

### Metrics and troubleshooting

```bash
docker compose --profile observability up -d
curl --fail http://localhost:9090/-/healthy
curl --fail http://localhost:8088/api/metrics/summary
docker compose logs --tail=100 api scheduler worker
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
```

Prometheus is at http://localhost:9090. It scrapes API/scheduler metrics and discovers worker addresses through Compose DNS. Structured JSON logs carry workflow, task, execution, and worker identifiers on execution events. Readiness includes database connectivity; liveness describes the process rather than a dependency outage. Kafka/Redis health checks remain separate from application readiness, so inspect broker/cache connectivity and consumer lag when processes are ready but progress stops.

`docker compose down` stops the environment and retains named database, Kafka, and Redis volumes. Starting again preserves workflows and execution history. `docker compose down --volumes` deletes those volumes and **all local persisted workflow data**; use it only for an intentional fresh environment.

## Local Kubernetes

The reusable `k8s/base` contains only application deployments, services, configuration, worker HPA, and disruption budgets. `k8s/overlays/local` adds single-node PostgreSQL, Redis, and Kafka StatefulSets with persistent volume claims, plus a development Secret. This is a local demonstration topology; the datastore replicas are not highly available.

Build the images and make them available to your cluster. For a kind cluster:

```bash
docker compose build
kind load docker-image flowforge/api:local flowforge/scheduler:local \
  flowforge/worker:local flowforge/frontend:local
kubectl kustomize k8s/overlays/local > /tmp/flowforge-local.yaml
kubectl apply -f /tmp/flowforge-local.yaml
kubectl -n flowforge get pods,pvc
kubectl -n flowforge rollout status deployment/api --timeout=300s
kubectl -n flowforge rollout status deployment/scheduler --timeout=300s
kubectl -n flowforge rollout status deployment/worker --timeout=300s
kubectl -n flowforge port-forward service/frontend 3000:8080
```

For another cluster, push the images to a reachable registry and change Kustomize image names/tags. A default StorageClass must provision the three local StatefulSet volume claims. The local Secret contains the same non-production password as Compose. Backend pods use non-root users, read-only root filesystems, `/tmp` scratch volumes, dropped capabilities, resource requests/limits, and startup/readiness/liveness probes.

The worker HPA keeps 2–6 replicas and uses CPU utilization at 70% of the CPU request. It requires metrics-server; without resource metrics it cannot make scaling decisions. CPU is a weak signal for DELAY or I/O-bound work, so use manual scale tests or add queue-lag autoscaling for those workloads. See the [Kubernetes HPA documentation](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/).

The two schedulers in the Kubernetes base exercise database-coordinated admission. API and scheduler have two replicas; the local frontend has one. Pod disruption budgets preserve one available API, scheduler, and worker during voluntary disruption, subject to cluster capacity.

Kubernetes manifests have been rendered and statically inspected. That validates configuration composition, not an actual Kubernetes deployment. Run server-side validation and the real-system harnesses against your cluster before relying on it.

## AWS

Use `k8s/overlays/aws` with existing EKS, RDS PostgreSQL, ElastiCache, and MSK. The overlay excludes every local datastore and the development Secret. It configures verified database TLS, Redis TLS, Kafka SASL/SCRAM over TLS, and replication factor 3. Endpoints, credentials, CA certificates, ECR account/region, and immutable image tags must be supplied.

Follow [the AWS deployment input guide](../infra/aws/README.md). It explicitly separates application manifests from infrastructure that the target environment must provide. Neither this repository nor its benchmark results claim an AWS deployment, multi-AZ failover verification, or public-production readiness.
