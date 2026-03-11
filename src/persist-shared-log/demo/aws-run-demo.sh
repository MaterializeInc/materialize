#!/usr/bin/env bash
# aws-run-demo.sh — Run the consensus demo workload on an EC2 scratch instance.
#
# Prerequisites: aws-setup.sh has been run and environmentd is ready.
#
# This script creates the Materialize PG source, starts a background UPDATE
# loop, and creates materialized views in stages to show shard scaling with
# flat S3 writes.

set -euo pipefail

PG_PORT="${PG_PORT:-5432}"
MZ_PORT="${MZ_PORT:-6875}"
PSQL_MZ="psql postgres://materialize@localhost:${MZ_PORT}/materialize -v ON_ERROR_STOP=1"
PSQL_PG="psql -h localhost -p ${PG_PORT} -d demo"

PIDS_TO_KILL=()
cleanup() {
    echo ""
    echo "Cleaning up..."
    for pid in "${PIDS_TO_KILL[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    echo "Done."
}
trap cleanup EXIT

echo "=== Group Commit Consensus Demo ==="
echo ""

# =========================================================================
# Stage 0: Create Materialize source
# =========================================================================
echo "Stage 0: Creating Materialize PG source..."

# Drop stale replication slots from previous demo runs.
$PSQL_PG -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;" 2>/dev/null || true

$PSQL_MZ <<SQL
CREATE SECRET IF NOT EXISTS pgpass AS '';
CREATE CONNECTION IF NOT EXISTS pg_conn TO POSTGRES (
    HOST 'localhost',
    PORT $PG_PORT,
    DATABASE 'demo',
    USER '$(whoami)',
    PASSWORD SECRET pgpass
);
SQL

$PSQL_MZ -c "DROP SOURCE IF EXISTS pg_source CASCADE;" 2>/dev/null || true
$PSQL_MZ -c "CREATE SOURCE pg_source FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source') FOR TABLES (tick);" 2>/dev/null || true
echo "  Source ready."

# =========================================================================
# Background UPDATE loop
# =========================================================================
echo "  Starting background UPDATE loop (~20 updates/sec)..."
(
    while true; do
        $PSQL_PG -c "UPDATE tick SET val = val + 1;" 2>/dev/null || true
        sleep 0.05
    done
) &
PIDS_TO_KILL+=($!)

sleep 3  # Let the source catch up.

# =========================================================================
# Staged MV creation
# =========================================================================
echo ""
echo "Stage 1: Creating 5 materialized views..."
for i in $(seq 1 5); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT upper(val::text) AS v FROM tick;"
done
echo "  5 MVs active. Watch the dashboard for 30s..."
sleep 30

echo "Stage 2: Adding 15 more MVs (20 total)..."
for i in $(seq 6 20); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT upper(val::text) AS v FROM tick;"
done
echo "  20 MVs active. Watch the dashboard for 30s..."
sleep 30

echo "Stage 3: Adding 30 more MVs (50 total)..."
for i in $(seq 21 50); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT upper(val::text) AS v FROM tick;"
done
echo "  50 MVs active. Watch the dashboard for 30s..."
sleep 30

echo "Stage 4: Adding 50 more MVs (100 total)..."
for i in $(seq 51 100); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT upper(val::text) AS v FROM tick;"
done
echo "  100 MVs active. Watch the dashboard for 30s..."
sleep 30

echo "Stage 5: Adding 100 more MVs (200 total)..."
for i in $(seq 101 200); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT upper(val::text) AS v FROM tick;"
done
echo "  200 MVs active!"
echo ""
echo "  The S3 PUTs/s line should be FLAT while Active Shards climbed."
echo "  Press Ctrl+C to stop."
sleep infinity
