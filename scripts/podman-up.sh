#!/bin/sh
set -eu

IMAGE_NAME="${IMAGE_NAME:-localhost/seci-fdre-v-web:latest}"
CONTAINER_NAME="${CONTAINER_NAME:-seci-fdre-v-web}"
HOST_PORT="${SECI_FDRE_V_PORT:-8000}"
WORKSPACE_DIR="${SECI_FDRE_V_WORKSPACE_HOST:-$(pwd)/.workspace}"

mkdir -p "$WORKSPACE_DIR"

if podman container exists "$CONTAINER_NAME"; then
  podman rm -f "$CONTAINER_NAME" >/dev/null
fi

podman build -t "$IMAGE_NAME" -f Dockerfile .

exec podman run \
  --name "$CONTAINER_NAME" \
  --replace \
  -p "${HOST_PORT}:8000" \
  -e SECI_FDRE_V_WORKSPACE=/workspace \
  -e SECI_FDRE_V_SOURCE_CONFIG=/app/config/project.yaml \
  -v "${WORKSPACE_DIR}:/workspace" \
  "$IMAGE_NAME"
