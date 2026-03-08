#!/usr/bin/env bash
# aws-setup.sh — Set up and run the consensus service demo on an EC2 scratch instance.
#
# Usage:
#   1. bin/scratch create consensus-svc-demo
#   2. Wait for provisioning (~5 min)
#   3. bin/scratch ssh <instance-id>
#   4. cd materialize
#   5. ./src/persist-consensus-svc/demo/aws-setup.sh
#
# What this does:
#   - Installs and configures PostgreSQL (logical replication, trust auth)
#   - Starts the monitoring stack (Prometheus + Grafana via Docker)
#   - Builds the consensus service (release mode)
#   - Starts consensus-svc and environmentd in a tmux session
#
# After setup completes, run the demo:
#   ./src/persist-consensus-svc/demo/aws-run-demo.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$REPO_ROOT"

S3_BUCKET="${S3_BUCKET:-phemberger-s3-directory-test--use1-az4--x-s3}"
S3_PREFIX="${S3_PREFIX:-consensus/}"
S3_REGION="${S3_REGION:-us-east-1}"
FLUSH_INTERVAL_MS="${FLUSH_INTERVAL_MS:-5}"
PG_PORT="${PG_PORT:-5432}"

# =========================================================================
# [1/5] Install PostgreSQL
# =========================================================================
echo "=== [1/5] Installing PostgreSQL server ==="
if ! pg_lsclusters -h 2>/dev/null | grep -q .; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq postgresql postgresql-contrib
fi

PG_VER=$(pg_lsclusters -h | awk '{print $1}' | head -1)
PG_CLUSTER=$(pg_lsclusters -h | awk '{print $2}' | head -1)
PG_CONF="/etc/postgresql/$PG_VER/$PG_CLUSTER"
echo "  PostgreSQL $PG_VER ($PG_CLUSTER) installed."

# =========================================================================
# [2/5] Configure PostgreSQL
# =========================================================================
echo "=== [2/5] Configuring PostgreSQL ==="
sudo pg_ctlcluster "$PG_VER" "$PG_CLUSTER" stop 2>/dev/null || true

# Enable logical replication.
if ! sudo grep -q "^wal_level = logical" "$PG_CONF/postgresql.conf"; then
    sudo bash -c "cat >> $PG_CONF/postgresql.conf <<CONF
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
CONF"
fi

# Trust local connections (no password prompts).
sudo bash -c "cat > $PG_CONF/pg_hba.conf <<HBA
local   all   all                 trust
host    all   all   127.0.0.1/32  trust
host    all   all   ::1/128       trust
local   replication   all         trust
host    replication   all   127.0.0.1/32  trust
HBA"

sudo pg_ctlcluster "$PG_VER" "$PG_CLUSTER" start

# Wait for Postgres to be ready.
for i in $(seq 1 30); do
    pg_isready -p "$PG_PORT" -q 2>/dev/null && break
    sleep 0.5
done

# Create databases and roles.
psql -h localhost -p "$PG_PORT" -U postgres -c "CREATE USER ubuntu WITH SUPERUSER REPLICATION;" 2>/dev/null || true
psql -h localhost -p "$PG_PORT" -U postgres -c "CREATE DATABASE materialize OWNER ubuntu;" 2>/dev/null || true
psql -h localhost -p "$PG_PORT" -U postgres -c "CREATE DATABASE demo OWNER ubuntu;" 2>/dev/null || true

# Set up the demo source table + publication.
psql -h localhost -p "$PG_PORT" -d demo <<'SQL'
CREATE TABLE IF NOT EXISTS tick (id INT PRIMARY KEY, val INT NOT NULL);
ALTER TABLE tick REPLICA IDENTITY FULL;
INSERT INTO tick VALUES (1, 0) ON CONFLICT DO NOTHING;
DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR TABLE tick;
SQL
echo "  PostgreSQL ready on port $PG_PORT."

# =========================================================================
# [3/5] Start monitoring stack
# =========================================================================
echo "=== [3/5] Starting monitoring stack (Prometheus + Grafana) ==="
mkdir -p mzdata/prometheus mzdata/tempo
./bin/mzcompose --find monitoring run default
echo "  Monitoring stack up (Grafana :3001, Prometheus :9090)."

# =========================================================================
# [4/5] Build consensus service
# =========================================================================
echo "=== [4/5] Building consensus service (release) ==="
source "$HOME/.cargo/env" 2>/dev/null || true
cargo build --release -p mz-persist-consensus-svc
echo "  Consensus service built."

# =========================================================================
# [5/5] Start services in tmux
# =========================================================================
echo "=== [5/5] Starting services in tmux ==="
tmux kill-session -t demo 2>/dev/null || true

# Window 0: Consensus service
tmux new-session -d -s demo -n consensus
tmux send-keys -t demo:consensus \
  "cd $REPO_ROOT && ./target/release/mz-persist-consensus-svc \
    --s3-bucket $S3_BUCKET \
    --s3-prefix '$S3_PREFIX' \
    --s3-region $S3_REGION \
    --flush-interval-ms $FLUSH_INTERVAL_MS" Enter

sleep 3

# Window 1: environmentd (bin/environmentd handles build + schema setup + run)
tmux new-window -t demo -n environmentd
tmux send-keys -t demo:environmentd \
  "cd $REPO_ROOT && MZDEV_POSTGRES=postgres://ubuntu@localhost:$PG_PORT/materialize \
    ./bin/environmentd --reset -- \
    --persist-consensus-url='rpc://localhost:6890' \
    --system-parameter-default=default_timestamp_interval=100ms" Enter

PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "<public-ip>")

echo ""
echo "============================================================"
echo "  Services starting in tmux session 'demo'."
echo ""
echo "  tmux attach -t demo           # view service logs"
echo "  Grafana: http://$PUBLIC_IP:3001/d/consensus-svc-demo"
echo ""
echo "  environmentd is building + starting (may take 10-30 min"
echo "  on first run). Watch progress: tmux attach -t demo"
echo ""
echo "  Once environmentd is ready (you'll see 'listening on'), run:"
echo "    ./src/persist-consensus-svc/demo/aws-run-demo.sh"
echo "============================================================"
