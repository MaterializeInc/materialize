#!/usr/bin/env bash
# Demo: Group Commit Consensus — flat S3 writes as shards scale.
#
# Prerequisites:
#   1. Postgres running on localhost:5433 with logical replication (database: demo,
#      table: events, publication: mz_source)
#   2. persist-consensus-svc running (port 6890 gRPC, port 6891 metrics)
#   3. environmentd running with --persist-consensus-url='rpc://localhost:6890'
#   4. Monitoring stack: cd misc/monitoring && ./mzcompose run default
#
# Open Grafana: http://localhost:3000/d/consensus-svc-demo

set -euo pipefail

MZ_PORT="${MZ_PORT:-6875}"
PG_PORT="${PG_PORT:-5433}"
PSQL_MZ="psql postgres://materialize@localhost:${MZ_PORT}/materialize -v ON_ERROR_STOP=1"
PSQL_PG="psql postgres://localhost:${PG_PORT}/demo"

echo "=== Group Commit Consensus Demo ==="
echo "Grafana: http://localhost:3000/d/consensus-svc-demo"
echo ""

# --- Stage 0: Set up Postgres source in Materialize ---
echo "Stage 0: Setting up Postgres source..."

# Ensure PG table exists with data.
$PSQL_PG -c "CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, name TEXT NOT NULL, ts TIMESTAMPTZ DEFAULT now());" 2>/dev/null || true
$PSQL_PG -c "ALTER TABLE events REPLICA IDENTITY FULL;" 2>/dev/null || true
# Publication may already exist.
$PSQL_PG -c "CREATE PUBLICATION mz_source FOR TABLE events;" 2>/dev/null || true
# Seed some initial rows.
$PSQL_PG -c "INSERT INTO events (name) SELECT 'init-' || i FROM generate_series(1, 10) AS i;" 2>/dev/null || true

# Create Materialize source objects (idempotent via IF NOT EXISTS).
$PSQL_MZ <<'SQL'
CREATE SECRET IF NOT EXISTS pgpass AS '';
CREATE CONNECTION IF NOT EXISTS pg_conn TO POSTGRES (
    HOST 'localhost',
    PORT 5433,
    DATABASE 'demo',
    USER 'phemberger',
    PASSWORD SECRET pgpass
);
SQL

# The source itself — skip if already exists.
$PSQL_MZ -c "CREATE SOURCE IF NOT EXISTS pg_source FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source') FOR TABLES (events);" 2>/dev/null || true

echo "  Source ready."

# --- Background INSERT loop ---
echo "  Starting background INSERT loop (~1 row/sec)..."
(
    while true; do
        $PSQL_PG -c "INSERT INTO events (name) VALUES ('demo-' || extract(epoch from now())::int);" 2>/dev/null || true
        sleep 1
    done
) &
INSERT_PID=$!
trap "kill $INSERT_PID 2>/dev/null; echo 'Stopped background inserts.'" EXIT

sleep 3  # Let the source catch up.

# --- Stage 1: 5 MVs ---
echo ""
echo "Stage 1: Creating 5 materialized views..."
for i in $(seq 1 5); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT count(*) AS c FROM events WHERE id % ${i} = 0;"
done
echo "  5 MVs active. Watch the dashboard for 30s..."
sleep 30

# --- Stage 2: 15 more MVs (20 total) ---
echo "Stage 2: Adding 15 more MVs (20 total)..."
for i in $(seq 6 20); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT count(*) AS c FROM events WHERE id % ${i} = 0;"
done
echo "  20 MVs active. Watch the dashboard for 30s..."
sleep 30

# --- Stage 3: 30 more MVs (50 total) ---
echo "Stage 3: Adding 30 more MVs (50 total)..."
for i in $(seq 21 50); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT count(*) AS c FROM events WHERE id % ${i} = 0;"
done
echo "  50 MVs active. Watch the dashboard for 30s..."
sleep 30

# --- Stage 4: 50 more MVs (100 total) ---
echo "Stage 4: Adding 50 more MVs (100 total)..."
for i in $(seq 51 100); do
    $PSQL_MZ -c "CREATE MATERIALIZED VIEW IF NOT EXISTS demo_mv_${i} AS SELECT count(*) AS c FROM events WHERE id % ${i} = 0;"
done
echo "  100 MVs active!"
echo ""
echo "  The S3 PUTs/s line should be FLAT while Active Shards climbed."
echo "  Press Ctrl+C to stop."
sleep infinity
