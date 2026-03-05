#!/usr/bin/env bash
# Demo: Group Commit Consensus — flat S3 writes as shards scale.
#
# This script is fully self-contained. It will:
#   1. Start a local Postgres instance with logical replication
#   2. Create the source database, table, and publication
#   3. Set up the Materialize PG source
#   4. Run a background INSERT loop
#   5. Create materialized views in stages to show shard scaling
#
# Before running, start the other three services:
#
#   # Terminal 1: Consensus service
#   AWS_PROFILE=mz-scratch-admin cargo run -p mz-persist-consensus-svc -- \
#     --s3-bucket phemberger-s3-directory-test--use1-az4--x-s3 \
#     --s3-prefix consensus/ \
#     --s3-region us-east-1
#
#   # Terminal 2: Monitoring stack (Prometheus + Grafana)
#   cd misc/monitoring && ./mzcompose run default
#
#   # Terminal 3: environmentd
#   ./bin/environmentd --reset -- \
#     --persist-consensus-url='rpc://localhost:6890' \
#     --system-parameter-default=default_timestamp_interval=100ms
#
#   # Terminal 4: This script
#   src/persist-consensus-svc/demo/demo.sh
#
# Then open Grafana: http://localhost:3000/d/consensus-svc-demo

set -euo pipefail

MZ_PORT="${MZ_PORT:-6875}"
PG_PORT="${PG_PORT:-5433}"
PG_DATADIR="${PG_DATADIR:-/tmp/mz-demo/pgdata}"
PSQL_MZ="psql postgres://materialize@localhost:${MZ_PORT}/materialize -v ON_ERROR_STOP=1"
PSQL_PG="psql postgres://localhost:${PG_PORT}/demo"

PIDS_TO_KILL=()
cleanup() {
    echo ""
    echo "Cleaning up..."
    for pid in "${PIDS_TO_KILL[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    # Stop Postgres if we started it.
    if [[ -n "${PG_STARTED:-}" ]]; then
        pg_ctl -D "$PG_DATADIR" stop -m fast 2>/dev/null || true
    fi
    echo "Done."
}
trap cleanup EXIT

echo "=== Group Commit Consensus Demo ==="
echo "Grafana: http://localhost:3000/d/consensus-svc-demo"
echo ""

# =======================================================================
# Stage 0: Ensure Postgres is running with logical replication
# =======================================================================
echo "Stage 0: Setting up Postgres..."

# Detect postgres binary (brew postgresql@14 or @16 or system).
PG_BIN=""
for ver in 18 17 16 15 14; do
    candidate="$(brew --prefix postgresql@${ver} 2>/dev/null)/bin"
    if [[ -d "$candidate" ]]; then
        PG_BIN="$candidate"
        break
    fi
done
if [[ -z "$PG_BIN" ]]; then
    # Fall back to whatever is on PATH.
    PG_BIN="$(dirname "$(command -v pg_ctl || true)")"
fi
if [[ -z "$PG_BIN" || ! -x "$PG_BIN/pg_ctl" ]]; then
    echo "ERROR: Could not find pg_ctl. Install Postgres via 'brew install postgresql@16'."
    exit 1
fi
export PATH="$PG_BIN:$PATH"
echo "  Using Postgres from: $PG_BIN"

# Check if Postgres is already running on the target port.
if pg_isready -h localhost -p "$PG_PORT" -q 2>/dev/null; then
    echo "  Postgres already running on port $PG_PORT."
else
    echo "  Starting Postgres on port $PG_PORT (datadir: $PG_DATADIR)..."
    # Initialize if needed.
    if [[ ! -f "$PG_DATADIR/PG_VERSION" ]]; then
        mkdir -p "$PG_DATADIR"
        initdb -D "$PG_DATADIR" --no-locale --encoding=UTF8 -A trust >/dev/null
        # Enable logical replication.
        cat >> "$PG_DATADIR/postgresql.conf" <<PGCONF
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
port = $PG_PORT
PGCONF
    fi
    pg_ctl -D "$PG_DATADIR" -l "$PG_DATADIR/logfile" -o "-p $PG_PORT" start >/dev/null
    PG_STARTED=1
    # Wait for it.
    for i in $(seq 1 30); do
        pg_isready -h localhost -p "$PG_PORT" -q 2>/dev/null && break
        sleep 0.2
    done
    echo "  Postgres started."
fi

# Create database and table.
psql postgres://localhost:${PG_PORT}/postgres -c "CREATE DATABASE demo;" 2>/dev/null || true
$PSQL_PG -c "CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, name TEXT NOT NULL, ts TIMESTAMPTZ DEFAULT now());"
$PSQL_PG -c "ALTER TABLE events REPLICA IDENTITY FULL;"
$PSQL_PG -c "CREATE PUBLICATION mz_source FOR TABLE events;" 2>/dev/null || true
$PSQL_PG -c "INSERT INTO events (name) SELECT 'init-' || i FROM generate_series(1, 10) AS i;" 2>/dev/null || true
echo "  Postgres ready (database: demo, table: events, publication: mz_source)."

# =======================================================================
# Stage 0b: Create Materialize source
# =======================================================================
echo "  Creating Materialize source..."

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

$PSQL_MZ -c "CREATE SOURCE IF NOT EXISTS pg_source FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source') FOR TABLES (events);" 2>/dev/null || true
echo "  Source ready."

# =======================================================================
# Background INSERT loop
# =======================================================================
echo "  Starting background INSERT loop (~1 row/sec)..."
(
    while true; do
        $PSQL_PG -c "INSERT INTO events (name) VALUES ('demo-' || extract(epoch from now())::int);" 2>/dev/null || true
        sleep 1
    done
) &
PIDS_TO_KILL+=($!)

sleep 3  # Let the source catch up.

# =======================================================================
# Staged MV creation
# =======================================================================
echo ""
echo "Stage 1: Creating 5 materialized views..."
for i in $(seq 1 5); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT count(*) AS c FROM events WHERE id % ${i} = 0;"
done
echo "  5 MVs active. Watch the dashboard for 30s..."
sleep 30

echo "Stage 2: Adding 15 more MVs (20 total)..."
for i in $(seq 6 20); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT count(*) AS c FROM events WHERE id % ${i} = 0;"
done
echo "  20 MVs active. Watch the dashboard for 30s..."
sleep 30

echo "Stage 3: Adding 30 more MVs (50 total)..."
for i in $(seq 21 50); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT count(*) AS c FROM events WHERE id % ${i} = 0;"
done
echo "  50 MVs active. Watch the dashboard for 30s..."
sleep 30

echo "Stage 4: Adding 50 more MVs (100 total)..."
for i in $(seq 51 100); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT count(*) AS c FROM events WHERE id % ${i} = 0;"
done
echo "  100 MVs active!"
echo ""
echo "  The S3 PUTs/s line should be FLAT while Active Shards climbed."
echo "  Press Ctrl+C to stop."
sleep infinity
