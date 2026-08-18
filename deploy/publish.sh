#!/usr/bin/env bash
set -euo pipefail

REGISTRY="containers.wheeli.ca"
IMAGE_NAME="${REGISTRY}/stocker-informer/stocker-informer"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "==> Building image from ${PROJECT_DIR}"
podman build -t stocker-informer:latest -f "${PROJECT_DIR}/Containerfile" "${PROJECT_DIR}"

tag_and_push() {
  local tag="$1"
  echo "==> Tagging and pushing ${IMAGE_NAME}:${tag}"
  podman tag stocker-informer:latest "${IMAGE_NAME}:${tag}"
  podman push "${IMAGE_NAME}:${tag}"
}

tag_and_push latest
tag_and_push "$(date -u +'%Y%m%d%H%M%S')"

echo ""
echo "Done! Images pushed:"
echo "  ${IMAGE_NAME}:latest"
echo "  ${IMAGE_NAME}:$(date -u +'%Y%m%d%H%M%S')"
