# AWS deployment inputs

This directory and `k8s/overlays/aws` provide an application deployment structure for an existing AWS environment. They do **not** provision a VPC, EKS cluster, database, cache, broker, IAM policies, load balancer, or TLS certificates. No AWS deployment is claimed.

## Service mapping

| Local component | AWS component | Required configuration |
| --- | --- | --- |
| API, scheduler, workers, frontend | EKS deployments | Private worker nodes, metrics-server, ECR image pull permissions |
| PostgreSQL | RDS PostgreSQL 16 | Multi-AZ for availability, database `flowforge`, user credentials, TLS trust bundle, backups |
| Redis | ElastiCache Redis OSS compatible primary endpoint | TLS and AUTH token; non-cluster-mode endpoint for this client configuration |
| Kafka | Amazon MSK provisioned | Three brokers, TLS + SASL/SCRAM, bootstrap addresses, topic permissions |
| Images | Four ECR repositories | `flowforge/api`, `flowforge/scheduler`, `flowforge/worker`, `flowforge/frontend` |
| Credentials | Your Secrets Manager integration | A Kubernetes Secret named `flowforge-secrets` with keys from `secrets.example.yaml` |
| Metrics | Your Prometheus installation | Scrape pod annotations on port 8080/8081/8082 at `/actuator/prometheus` |

Keep the application and managed services in private subnets with security groups allowing only the required traffic. The dashboard/API have no built-in authentication or tenant isolation; this overlay creates ClusterIP Services only. Configure an authenticated ingress and TLS before exposing them outside a trusted network.

## Publish images

Create the four ECR repositories with image scanning and an immutable-tag policy. Then run from the repository root:

```bash
export AWS_ACCOUNT_ID=123456789012
export AWS_REGION=us-east-1
export IMAGE_TAG="$(git rev-parse HEAD)"
export IMAGE_PLATFORM=linux/amd64  # Match the EKS node architecture.
bash infra/aws/publish-images.sh
```

The script uses your existing AWS CLI identity and pushes images. It does not create repositories or change account permissions. An ARM fleet can use `linux/arm64`; a mixed fleet can use `linux/amd64,linux/arm64` with a builder that supports both platforms. Pin validated images by digest for a release.

## Configure and apply

1. Replace account, region, and image tags in `k8s/overlays/aws/kustomization.yaml`. Replace RDS, ElastiCache, and MSK endpoints in `config.yaml`. MSK addresses must be the **SASL/SCRAM** bootstrap string for the cluster, not its plaintext or IAM endpoints.
2. Create namespace `flowforge`. Populate `flowforge-secrets` using your secret manager integration. `secrets.example.yaml` documents the expected keys and is deliberately not included by Kustomize. The Redis token and SCRAM JAAS settings are Spring Boot environment properties; no credentials belong in ConfigMaps or committed files.
3. Obtain the current RDS trust bundle using the [RDS certificate documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html), verify its source, and make it available in the namespace:

   ```bash
   kubectl -n flowforge create configmap flowforge-rds-ca \
     --from-file=global-bundle.pem=/path/to/verified/global-bundle.pem
   ```

   The JDBC URL uses `sslmode=verify-full` and the mounted CA to verify the database hostname. See [RDS PostgreSQL SSL configuration](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.SSL.html).
4. Create the database and a least-privilege migration/application role, or use a dedicated migration process and runtime role in a hardened deployment. The current services apply Flyway migrations at startup, so the configured database role needs schema migration permissions. Flyway coordinates concurrent startup migrations.
5. Give the MSK SCRAM principal permission to create/describe the four `flowforge.tasks.*` topics and read/write the service consumer groups, or precreate the topics and adjust broker policies. The AWS overlay requests replication factor 3 and retains 12 partitions. MSK TLS and authentication must match the client settings; see [MSK encryption](https://docs.aws.amazon.com/msk/latest/developerguide/msk-encryption.html).
6. Render and inspect before applying:

   ```bash
   kubectl kustomize k8s/overlays/aws > /tmp/flowforge-aws.yaml
   kubectl apply --dry-run=server -f /tmp/flowforge-aws.yaml
   kubectl apply -f /tmp/flowforge-aws.yaml
   kubectl -n flowforge rollout status deployment/api
   kubectl -n flowforge rollout status deployment/scheduler
   kubectl -n flowforge rollout status deployment/worker
   ```

## Operations to provide in the target environment

Use RDS backups and point-in-time recovery, broker retention sized for outage recovery, and tested restore procedures. Budget PostgreSQL connection pools across all replicas: the default is 16 connections per backend pod. Rotate credentials and the RDS CA on a schedule; environment variables require pod rollout after a Secret change. Protect payment-ledger and history data through access control and retention rules appropriate for your deployment.

The worker HPA uses CPU as a portable starting point. DELAY and external-I/O tasks consume little CPU, so CPU alone cannot track queue demand. Add a Kafka lag or pending-task metric autoscaler after measuring the target workload. Kafka partition counts, per-pod worker concurrency, and the global admission limit remain upper bounds on useful scaling. Scheduler replicas coordinate via PostgreSQL; more scheduler pods do not remove database admission serialization.
