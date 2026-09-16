#!/usr/bin/env bash
# Builds and publishes to pre-existing private ECR repositories. No infrastructure creation.
set -euo pipefail

: "${AWS_ACCOUNT_ID:?Set the target twelve-digit AWS account ID}"
: "${AWS_REGION:?Set the target AWS region}"
: "${IMAGE_TAG:?Set an immutable version or git commit tag}"
[[ "$AWS_ACCOUNT_ID" =~ ^[0-9]{12}$ ]] || { echo 'AWS_ACCOUNT_ID must contain twelve digits' >&2; exit 2; }
[[ "$IMAGE_TAG" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || { echo 'IMAGE_TAG is not a valid container image tag' >&2; exit 2; }
[[ "$AWS_REGION" =~ ^[a-z]{2}(-[a-z]+)+-[0-9]+$ ]] || { echo 'AWS_REGION is invalid' >&2; exit 2; }

task_repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
task_registry="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
task_platform="${IMAGE_PLATFORM:-linux/amd64}"
aws ecr get-login-password --region "$AWS_REGION" |
  docker login --username AWS --password-stdin "$task_registry"

for service in api scheduler worker; do
  docker buildx build --platform "$task_platform" \
    --file "$task_repo_root/docker/Dockerfile.backend" \
    --build-arg "SERVICE=${service}-service" \
    --tag "$task_registry/flowforge/$service:$IMAGE_TAG" --push "$task_repo_root"
done
docker buildx build --platform "$task_platform" \
  --file "$task_repo_root/docker/Dockerfile.frontend" \
  --tag "$task_registry/flowforge/frontend:$IMAGE_TAG" --push "$task_repo_root"
