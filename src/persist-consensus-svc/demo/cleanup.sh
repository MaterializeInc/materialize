#!/usr/bin/env bash
# Clean up all demo objects from Materialize.
set -euo pipefail

MZ_PORT="${MZ_PORT:-6875}"
PSQL_MZ="psql postgres://materialize@localhost:${MZ_PORT}/materialize"

echo "Cleaning up demo objects..."

for i in $(seq 1 200); do
    $PSQL_MZ -c "DROP MATERIALIZED VIEW IF EXISTS demo_mv_${i};" 2>/dev/null || true
done

$PSQL_MZ -c "DROP SOURCE IF EXISTS pg_source CASCADE;" 2>/dev/null || true
$PSQL_MZ -c "DROP CONNECTION IF EXISTS pg_conn;" 2>/dev/null || true
$PSQL_MZ -c "DROP SECRET IF EXISTS pgpass;" 2>/dev/null || true

echo "Cleanup complete."
