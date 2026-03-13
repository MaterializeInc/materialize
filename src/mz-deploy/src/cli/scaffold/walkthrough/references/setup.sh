#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_NAME="$(basename "$(cd "$SCRIPT_DIR/../../../.." && pwd)")"

echo "Starting Docker infrastructure..."

docker compose -f "$SCRIPT_DIR/docker-compose.yml" --project-name "$PROJECT_NAME" up -d

echo "Waiting for Materialize to become healthy..."
TIMEOUT=90
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    if docker compose -f "$SCRIPT_DIR/docker-compose.yml" --project-name "$PROJECT_NAME" \
        ps materialized --format '{{.Health}}' 2>/dev/null | grep -q "healthy"; then
        echo "Materialize is healthy!"
        exit 0
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    echo "  ...still waiting (${ELAPSED}s / ${TIMEOUT}s)"
done

echo "ERROR: Materialize did not become healthy within ${TIMEOUT}s"
docker compose -f "$SCRIPT_DIR/docker-compose.yml" --project-name "$PROJECT_NAME" logs materialized
exit 1
