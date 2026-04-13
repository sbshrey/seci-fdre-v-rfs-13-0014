#!/bin/sh
set -eu

CONTAINER_NAME="${CONTAINER_NAME:-seci-fdre-v-web}"

if podman container exists "$CONTAINER_NAME"; then
  exec podman rm -f "$CONTAINER_NAME"
fi

echo "Container '$CONTAINER_NAME' does not exist."
