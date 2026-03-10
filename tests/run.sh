#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Build the binary via Nix and stage it for Docker (skip if pre-built binary exists)
if [ -x "$PROJECT_DIR/bin/zfs-cloud-backup" ]; then
  echo "Using existing binary at bin/zfs-cloud-backup"
else
  echo "Building binary via nix..."
  nix build --out-link "$PROJECT_DIR/result"
  mkdir -p "$PROJECT_DIR/bin"
  cp "$PROJECT_DIR/result/bin/zfs-cloud-backup" "$PROJECT_DIR/bin/"
fi

compose() {
  docker compose -f "$SCRIPT_DIR/docker-compose.yml" \
    up --build --abort-on-container-exit --exit-code-from test
}

case "$(uname -s)" in
  Linux)
    compose
    ;;
  Darwin)
    LIMA_VM="zcb-test"

    # Create Lima VM if it doesn't exist
    if ! limactl list --format '{{.Name}}' 2>/dev/null | grep -q "^${LIMA_VM}$"; then
      echo "Creating Lima VM '${LIMA_VM}'..."
      limactl create --name="$LIMA_VM" "$SCRIPT_DIR/lima.yaml"
    fi

    # Start VM if not already running
    STATUS=$(limactl list --format '{{.Status}}' --filter "Name=${LIMA_VM}" 2>/dev/null || echo "")
    if [ "$STATUS" != "Running" ]; then
      echo "Starting Lima VM '${LIMA_VM}'..."
      limactl start "$LIMA_VM"
    fi

    # Run docker compose inside the VM (avoids flaky socket forwarding)
    # Lima mounts ~ read-only by default, so the project dir is accessible
    limactl shell "$LIMA_VM" -- sudo docker compose \
      -f "$SCRIPT_DIR/docker-compose.yml" \
      up --build --abort-on-container-exit --exit-code-from test
    ;;
  *)
    echo "Unsupported OS: $(uname -s)" >&2
    exit 1
    ;;
esac
