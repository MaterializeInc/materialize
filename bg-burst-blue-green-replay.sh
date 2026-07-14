#!/usr/bin/env bash
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
# Blue/green cutover + hydration-burst interaction test.
#
# Question under test: does the hydration-burst autoscaling strategy shorten
# the "deploy_await" phase of a blue/green deployment (the wait for the green
# cluster's dataflows to hydrate before the atomic SWAP)?
#
# Assumes environmentd is already up (fresh --reset) with these flags set:
#   enable_cluster_controller, enable_auto_scaling_strategy,
#   enable_background_alter_cluster, cluster_controller_tick_interval='1s',
#   unsafe_enable_unsafe_functions.
#
# Method: two back-to-back blue/green deployments against the same base data.
#   round 1 (baseline): plain green cluster at the steady size.
#   round 2 (burst):    green cluster carrying
#                       AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE=8x)).
# Both poll the exact readiness query from the in-repo blue/green test
# (test/cluster/blue-green-deployment/deploy.td): a dataflow is ready when it
# is hydrated on *some* replica and its lag is under threshold. We time each
# deploy_await (create green dataflow -> readiness), then perform the real SWAP.
set -uo pipefail

OUT=/tmp/bg-burst-test
mkdir -p "$OUT"
: > "$OUT/transcript.log"
: > "$OUT/results.txt"

PSQL_USER="psql -p 6875 -h localhost -U materialize -X -q -P pager=off -v ON_ERROR_STOP=0"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT/transcript.log"; }
sql() { $PSQL_USER -t -A -c "$1" materialize 2>/dev/null; }
run() { echo "  SQL> ${1//$'\n'/ }" | tee -a "$OUT/transcript.log"; $PSQL_USER -c "$1" materialize 2>&1 | grep -vE '^(NOTICE|DETAIL|HINT|Time):|connected to|Environment ID|Region:|User:|Cluster:|Database:|Schema:|Session UUID|Issue a SQL|View doc|Join our|^\s*$' | tee -a "$OUT/transcript.log"; }
show() { run "$1"; }
elapsed() { awk "BEGIN{printf \"%.1f\", $2 - $1}"; }

# Fingerprint workload. The `SELECT DISTINCT x` forces an exchange that spreads
# the heavy md5 map across all workers of a replica, so a larger replica
# hydrates it proportionally faster (that is what makes the burst worthwhile).
fp_body() { echo "SELECT '$1' AS version, max(md5(repeat(y::text, 2000))) AS fp FROM (SELECT DISTINCT x AS y FROM events)"; }

# Readiness query from test/cluster/blue-green-deployment/deploy.td, keyed by
# cluster name; returns the count of not-yet-ready dataflows on that cluster.
pending_count() {
  sql "WITH
    dataflows AS (
      SELECT mz_indexes.id FROM mz_indexes
      JOIN mz_clusters ON mz_indexes.cluster_id = mz_clusters.id
      WHERE mz_clusters.name = '$1'
      UNION ALL
      SELECT mz_materialized_views.id FROM mz_materialized_views
      JOIN mz_clusters ON mz_materialized_views.cluster_id = mz_clusters.id
      WHERE mz_clusters.name = '$1'
    ),
    ready_dataflows AS (
      SELECT id FROM dataflows d
      JOIN mz_internal.mz_compute_hydration_statuses h ON (h.object_id = d.id)
      LEFT JOIN mz_internal.mz_materialization_lag l ON (l.object_id = d.id)
      WHERE h.hydrated AND (l.local_lag <= '4s' OR l.local_lag IS NULL)
    ),
    pending_dataflows AS (SELECT id FROM dataflows EXCEPT SELECT id FROM ready_dataflows)
  SELECT count(*) FROM pending_dataflows"
}
dataflow_count() {
  sql "SELECT count(*) FROM (
     SELECT mz_indexes.id FROM mz_indexes JOIN mz_clusters ON mz_indexes.cluster_id=mz_clusters.id WHERE mz_clusters.name='$1'
     UNION ALL
     SELECT mz_materialized_views.id FROM mz_materialized_views JOIN mz_clusters ON mz_materialized_views.cluster_id=mz_clusters.id WHERE mz_clusters.name='$1')"
}
await_ready() { # await_ready <cluster> <timeout_s>; 0 ok, 1 timeout
  # The user MV+index are created synchronously before this is called, and a
  # freshly created heavy dataflow is never instantly hydrated, so pending>0
  # holds until real hydration completes. The cluster's introspection indexes
  # are also counted by the readiness query but hydrate near-instantly, so
  # pending==0 is reached only once the user objects hydrate.
  local cluster=$1 deadline=$(( $(date +%s) + $2 ))
  while [ "$(pending_count "$cluster")" != "0" ]; do
    [ "$(date +%s)" -gt "$deadline" ] && return 1; sleep 0.4
  done
}

HEAVY_ROWS=${HEAVY_ROWS:-320000}
HEAVY_CHUNK=${HEAVY_CHUNK:-40000}
STEADY=${STEADY:-'scale=1,workers=1'}
BURST=${BURST:-'scale=1,workers=8'}

############################################################################
log "=== reset test objects ==="
for c in prod prod_deploy; do run "DROP CLUSTER IF EXISTS $c CASCADE"; run "DROP SCHEMA IF EXISTS $c CASCADE"; done
run "DROP TABLE IF EXISTS events CASCADE"

log "=== base data: events table, ${HEAVY_ROWS} rows in chunks of ${HEAVY_CHUNK} ==="
run "CREATE TABLE events (x int)"
lo=1
while [ "$lo" -le "$HEAVY_ROWS" ]; do
  hi=$(( lo + HEAVY_CHUNK - 1 )); [ "$hi" -gt "$HEAVY_ROWS" ] && hi=$HEAVY_ROWS
  sql "INSERT INTO events SELECT generate_series($lo, $hi)" >/dev/null
  lo=$(( hi + 1 ))
done
log "events rows: $(sql 'SELECT count(*) FROM events')"

log "=== blue (prod) cluster with the current-version dataflow ==="
run "CREATE CLUSTER prod (SIZE = '$STEADY')"
run "CREATE SCHEMA prod"
run "CREATE MATERIALIZED VIEW prod.fingerprint IN CLUSTER prod AS $(fp_body v0)"
run "CREATE DEFAULT INDEX IN CLUSTER prod ON prod.fingerprint"
log "waiting for blue to hydrate once (a realistic running production)"
s=$(date +%s.%N); await_ready prod 600; e=$(date +%s.%N)
log "blue initial hydration at steady $STEADY: $(elapsed "$s" "$e")s"
show "SELECT version, fp FROM prod.fingerprint"

############################################################################
swap_with_retry() {
  local i
  for i in 1 2 3 4 5; do
    if $PSQL_USER -v ON_ERROR_STOP=1 materialize >/dev/null 2>>"$OUT/transcript.log" <<'SQL'
BEGIN;
ALTER SCHEMA prod SWAP WITH prod_deploy;
ALTER CLUSTER prod SWAP WITH prod_deploy;
COMMIT;
SQL
    then log "swap succeeded (attempt $i)"; return 0; fi
    log "swap attempt $i failed (retrying)"; sleep 1
  done
  return 1
}

deploy() { # deploy <label> <green_cluster_opts> <version>
  local label=$1 opts=$2 ver=$3
  log "======================================================================"
  log "=== DEPLOY [$label]: green cluster prod_deploy ($opts) ==="
  run "CREATE CLUSTER prod_deploy ($opts)"
  run "CREATE SCHEMA prod_deploy"
  log "--- create green dataflow; start deploy_await timer ---"
  local start end total; start=$(date +%s.%N)
  sql "CREATE MATERIALIZED VIEW prod_deploy.fingerprint IN CLUSTER prod_deploy AS $(fp_body "$ver")" >/dev/null
  sql "CREATE DEFAULT INDEX IN CLUSTER prod_deploy ON prod_deploy.fingerprint" >/dev/null
  await_ready prod_deploy 600
  end=$(date +%s.%N); total=$(elapsed "$start" "$end")
  log ">>> [$label] deploy_await (create green dataflow -> readiness): ${total}s"
  # At readiness, which replica satisfied it? (snapshot immediately)
  show "SHOW CLUSTERS WHERE name = 'prod_deploy'"
  show "SELECT cr.name AS replica, cr.size, mv.name AS object, h.hydrated
        FROM mz_internal.mz_hydration_statuses h
        JOIN mz_cluster_replicas cr ON cr.id = h.replica_id
        JOIN mz_clusters c ON c.id = cr.cluster_id
        LEFT JOIN mz_materialized_views mv ON mv.id = h.object_id
        WHERE c.name = 'prod_deploy' ORDER BY cr.name, object"
  log "--- atomic cutover: SWAP schema + cluster ---"
  swap_with_retry
  show "SELECT version, fp FROM prod.fingerprint"
  log "--- retire old environment (now named prod_deploy) ---"
  run "DROP CLUSTER prod_deploy CASCADE"
  run "DROP SCHEMA prod_deploy CASCADE"
  echo "$label $total" >> "$OUT/results.txt"
}

deploy "baseline" "SIZE = '$STEADY'" "v1"
deploy "burst" "SIZE = '$STEADY', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '$BURST', LINGER DURATION = '5s'))" "v2"

############################################################################
log "======================================================================"
log "=== after burst deploy: confirm burst tears down, prod back to steady ==="
log "waiting for burst teardown (steady hydration + 5s linger)"
# The view carries a row only while a reconfiguration or burst is active, so a
# scalar subquery + coalesce is used to always get 'none' once prod settles.
until [ "$(sql "SELECT coalesce((SELECT burst_size FROM mz_internal.mz_cluster_reconfigurations r JOIN mz_clusters c ON c.id=r.cluster_id WHERE c.name='prod'), 'none')")" = "none" ]; do sleep 1; done
show "SHOW CLUSTERS WHERE name = 'prod'"
show "SHOW CLUSTER REPLICAS WHERE cluster = 'prod'"
show "SELECT event_type, details->>'replica_name' AS replica, details->>'logical_size' AS size, details->>'reason' AS reason
      FROM mz_catalog.mz_audit_events
      WHERE object_type='cluster-replica' AND details->>'reason'='hydration-burst'
        AND details->>'cluster_name' IN ('prod','prod_deploy') ORDER BY id"

############################################################################
log "======================================================================"
log "=== RESULTS ==="
b=$(awk '/^baseline/{print $2}' "$OUT/results.txt")
u=$(awk '/^burst/{print $2}' "$OUT/results.txt")
log "baseline deploy_await: ${b}s"
log "burst    deploy_await: ${u}s"
[ -n "$b" ] && [ -n "$u" ] && log "speedup: $(awk "BEGIN{printf \"%.1f\", $b/$u}")x"
log "=== done ==="
