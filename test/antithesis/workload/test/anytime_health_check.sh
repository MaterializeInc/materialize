#!/usr/bin/env bash
set -euo pipefail

# Basic health check — verifies materialized is responding to SQL.
# This is a minimal placeholder; the antithesis-workload skill will add
# real test commands with property assertions.

PGHOST="${PGHOST:-materialized}"
PGPORT="${PGPORT:-6875}"
PGUSER="${PGUSER:-materialize}"

result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -tAc "SELECT 1" 2>&1)
if [ "$result" = "1" ]; then
    echo "Health check passed"
    exit 0
else
    echo "Health check failed: $result"
    exit 1
fi
