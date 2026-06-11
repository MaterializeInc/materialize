#!/usr/bin/env bash
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Clean end-to-end replay of the cluster-controller demo. Produces verbatim
# transcripts in /tmp/demo/clean/. Assumes environmentd is NOT running.
set -euo pipefail

OUT=/tmp/demo/clean
mkdir -p "$OUT"
rm -f "$OUT"/*.txt

PSQL_USER="psql -p 6875 -h localhost -U materialize -X -q -P pager=off"
PSQL_SYS="psql -p 6877 -h localhost -U mz_system -X -q -P pager=off"

log() { echo "[replay $(date +%H:%M:%S)] $*"; }

# Run SQL with statement echo, capture to transcript file.
cap() { # cap <outfile>  (sql on stdin)
  $PSQL_USER -e materialize 2>&1 | grep -vE "NOTICE:  connected|^  (Environment ID|Region|User|Cluster|Database|Schema|Session UUID)|^Issue a SQL|View documentation|Join our Slack|^ *$" > "$OUT/$1"
}

sql() { $PSQL_USER -t -A -c "$1" materialize 2>/dev/null; }

wait_ready() {
  until $PSQL_USER -t -A -c 'SELECT 1' materialize >/dev/null 2>&1; do sleep 2; done
}

poll() { # poll <description> <sql> <expected> <timeout_s>
  local desc=$1 q=$2 want=$3 deadline=$(( $(date +%s) + $4 ))
  until [ "$(sql "$q")" = "$want" ]; do
    if [ "$(date +%s)" -gt "$deadline" ]; then echo "TIMEOUT waiting for: $desc"; exit 1; fi
    sleep 2
  done
  log "reached: $desc"
}

############################################################################
log "starting environmentd with --reset"
(cd /home/ubuntu/materialize && nohup bin/environmentd --optimized --reset -- --unsafe-mode > /tmp/demo/environmentd-clean.log 2>&1 &)
wait_ready
log "environmentd up"

############################################################################
log "applying system flags"
$PSQL_SYS materialize <<'EOF' >/dev/null 2>&1
ALTER SYSTEM SET enable_cluster_controller = true;
ALTER SYSTEM SET enable_zero_downtime_cluster_reconfiguration = true;
ALTER SYSTEM SET enable_background_alter_cluster = true;
ALTER SYSTEM SET enable_auto_scaling_strategy = true;
ALTER SYSTEM SET cluster_controller_tick_interval = '1s';
ALTER SYSTEM SET unsafe_enable_unsafe_functions = true;
ALTER SYSTEM SET enable_cluster_schedule_refresh = true;
ALTER SYSTEM SET enable_refresh_every_mvs = true;
EOF

############################################################################
log "scenario 1 setup (slow-hydrating index, ~55s initial hydration)"
cap 01-setup.txt <<'EOF'
CREATE CLUSTER prod_analytics (SIZE = 'scale=1,workers=1');
CREATE TABLE orders (id int, amount numeric);
INSERT INTO orders SELECT generate_series(1, 1000), 100;
CREATE TABLE events (x int);
INSERT INTO events SELECT generate_series(1, 250000);
INSERT INTO events SELECT generate_series(250001, 500000);
INSERT INTO events SELECT generate_series(500001, 750000);
INSERT INTO events SELECT generate_series(750001, 1000000);
INSERT INTO events SELECT generate_series(1000001, 1250000);
INSERT INTO events SELECT generate_series(1250001, 1500000);
INSERT INTO events SELECT generate_series(1500001, 1750000);
INSERT INTO events SELECT generate_series(1750001, 2000000);
SET cluster = prod_analytics;
CREATE VIEW order_totals AS SELECT id, sum(amount) AS total FROM orders GROUP BY id;
CREATE INDEX order_totals_idx ON order_totals (id);
CREATE VIEW event_fingerprint AS
  SELECT max(md5(repeat(x::text, 2000))) AS fp FROM events;
CREATE INDEX event_fingerprint_idx ON event_fingerprint (fp);
EOF
# Block until the heavy index is hydrated (peek waits for it).
$PSQL_USER -c "SET cluster = prod_analytics" -c "SELECT fp FROM event_fingerprint" materialize >/dev/null 2>&1
log "initial hydration done"

############################################################################
log "S1a: background ALTER + mid-flight introspection"
cap 02-reconfig-midflight.txt <<'EOF'
SET cluster = prod_analytics;
\timing on
ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=2');
\timing off
SHOW CLUSTERS WHERE name = 'prod_analytics';
SELECT c.name, r.current_size, r.target_size,
       r.reconfiguration_in_flight AS in_flight,
       to_timestamp(r.reconfiguration_deadline::text::numeric / 1000) AS deadline
FROM mz_internal.mz_cluster_reconfigurations r
JOIN mz_clusters c ON c.id = r.cluster_id
WHERE c.name = 'prod_analytics';
SHOW CLUSTER REPLICAS WHERE cluster = 'prod_analytics';
\timing on
SELECT total FROM order_totals WHERE id = 42;
SELECT fp FROM event_fingerprint;
\timing off
SELECT size FROM mz_clusters WHERE name = 'prod_analytics';
EOF

poll "S1 cut-over" "SELECT reconfiguration_in_flight FROM mz_internal.mz_cluster_reconfigurations r JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.name = 'prod_analytics'" "f" 180

log "S1b: settled state + audit trail"
cap 03-reconfig-settled.txt <<'EOF'
SHOW CLUSTERS WHERE name = 'prod_analytics';
SHOW CLUSTER REPLICAS WHERE cluster = 'prod_analytics';
SELECT size FROM mz_clusters WHERE name = 'prod_analytics';
SELECT details->>'transition' AS transition,
       details->>'target_size' AS target_size, occurred_at
FROM mz_catalog.mz_audit_events
WHERE event_type = 'alter' AND object_type = 'cluster'
  AND details->>'cluster_name' = 'prod_analytics'
  AND details->>'transition' IS NOT NULL
ORDER BY id;
SELECT event_type, details->>'replica_name' AS replica,
       details->>'logical_size' AS size, details->>'reason' AS reason
FROM mz_catalog.mz_audit_events
WHERE object_type = 'cluster-replica'
  AND details->>'cluster_name' = 'prod_analytics'
ORDER BY id;
EOF

############################################################################
log "S1c: ALTER-back cancellation"
cap 04-cancel.txt <<'EOF'
ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=4');
SHOW CLUSTERS WHERE name = 'prod_analytics';
ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=2');
SELECT mz_unsafe.mz_sleep(5);
SHOW CLUSTERS WHERE name = 'prod_analytics';
SELECT details->>'transition' AS transition, details->>'target_size' AS target_size
FROM mz_catalog.mz_audit_events
WHERE event_type = 'alter' AND object_type = 'cluster'
  AND details->>'cluster_name' = 'prod_analytics'
  AND details->>'transition' IS NOT NULL
ORDER BY id;
EOF

############################################################################
log "S1d: timeout + ON TIMEOUT ROLLBACK (parked state)"
cap 05-timeout-rollback.txt <<'EOF'
ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=1')
  WITH (WAIT UNTIL READY (TIMEOUT '10s', ON TIMEOUT 'ROLLBACK'));
SHOW CLUSTER REPLICAS WHERE cluster = 'prod_analytics';
SELECT mz_unsafe.mz_sleep(15);
SHOW CLUSTERS WHERE name = 'prod_analytics';
SELECT c.name, r.current_size, r.target_size,
       r.reconfiguration_in_flight AS in_flight,
       to_timestamp(r.reconfiguration_deadline::text::numeric / 1000) < now() AS deadline_passed
FROM mz_internal.mz_cluster_reconfigurations r
JOIN mz_clusters c ON c.id = r.cluster_id
WHERE c.name = 'prod_analytics';
SHOW CLUSTER REPLICAS WHERE cluster = 'prod_analytics';
SELECT total FROM order_totals WHERE id = 42;
EOF

log "S1e: clear parked record via ALTER-back"
cap 06-clear-tombstone.txt <<'EOF'
ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=2');
SELECT mz_unsafe.mz_sleep(3);
SHOW CLUSTERS WHERE name = 'prod_analytics';
EOF

############################################################################
log "S1f: restart survival — start reconfiguration"
cap 07-restart-part1.txt <<'EOF'
ALTER CLUSTER prod_analytics SET (SIZE = 'scale=1,workers=1');
SELECT mz_unsafe.mz_sleep(8);
SHOW CLUSTERS WHERE name = 'prod_analytics';
EOF

log "killing environmentd mid-reconfiguration"
pgrep -af 'clusterd|environmentd' | grep -vE 'grep|pgrep|replay' | awk '{print $1}' | xargs -r kill -9 || true
sleep 2
(cd /home/ubuntu/materialize && nohup bin/environmentd --optimized -- --unsafe-mode > /tmp/demo/environmentd-clean-restart.log 2>&1 &)
wait_ready
log "environmentd back up"

cap 08-restart-part2.txt <<'EOF'
SHOW CLUSTERS WHERE name = 'prod_analytics';
EOF

poll "post-restart cut-over" "SELECT reconfiguration_in_flight FROM mz_internal.mz_cluster_reconfigurations r JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.name = 'prod_analytics'" "f" 300

cap 09-restart-settled.txt <<'EOF'
SHOW CLUSTERS WHERE name = 'prod_analytics';
SELECT details->>'transition' AS transition,
       details->>'target_size' AS target_size, occurred_at
FROM mz_catalog.mz_audit_events
WHERE event_type = 'alter' AND object_type = 'cluster'
  AND details->>'cluster_name' = 'prod_analytics'
  AND details->>'transition' IS NOT NULL
ORDER BY id DESC LIMIT 2;
EOF

############################################################################
log "S2: burst cluster creation"
cap 10-burst-create.txt <<'EOF'
CREATE CLUSTER fast_start (
  SIZE = 'scale=1,workers=1',
  AUTO SCALING STRATEGY = (ON HYDRATION (
    HYDRATION SIZE = 'scale=1,workers=8',
    LINGER DURATION = '15s'))
);
SHOW CREATE CLUSTER fast_start;
EOF

# Let the transient at-creation burst settle before the real demo.
poll "at-creation burst settled" "SELECT coalesce(burst_size, 'none') FROM mz_internal.mz_cluster_reconfigurations r JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.name = 'fast_start'" "none" 120

cap 11-burst-steady.txt <<'EOF'
SHOW CLUSTERS WHERE name = 'fast_start';
EOF

log "S2: load fresh data for the burst demo (uncaptured prep)"
$PSQL_USER materialize >/dev/null 2>&1 <<'EOF'
CREATE TABLE pageviews (x int);
INSERT INTO pageviews SELECT generate_series(1, 250000);
INSERT INTO pageviews SELECT generate_series(250001, 500000);
INSERT INTO pageviews SELECT generate_series(500001, 750000);
INSERT INTO pageviews SELECT generate_series(750001, 1000000);
INSERT INTO pageviews SELECT generate_series(1000001, 1250000);
INSERT INTO pageviews SELECT generate_series(1250001, 1500000);
INSERT INTO pageviews SELECT generate_series(1500001, 1750000);
INSERT INTO pageviews SELECT generate_series(1750001, 2000000);
CREATE VIEW pageview_fingerprint AS
  SELECT max(md5(repeat(x::text, 2000))) AS fp FROM pageviews;
EOF

log "S2: attach heavy index, burst fires"
cap 12-burst-fires.txt <<'EOF'
CREATE INDEX pageview_fingerprint_idx IN CLUSTER fast_start ON pageview_fingerprint (fp);
SELECT mz_unsafe.mz_sleep(4);
SHOW CLUSTERS WHERE name = 'fast_start';
SHOW CLUSTER REPLICAS WHERE cluster = 'fast_start';
EOF

# Catch the moment the burst replica has hydrated the index while the steady
# replica is still computing.
poll "burst hydrated, steady not" "
SELECT coalesce(bool_or(h.hydrated) FILTER (WHERE cr.size = 'scale=1,workers=8'), false)
   AND NOT coalesce(bool_or(h.hydrated) FILTER (WHERE cr.size = 'scale=1,workers=1'), false)
FROM mz_internal.mz_hydration_statuses h
JOIN mz_indexes i ON i.id = h.object_id
JOIN mz_cluster_replicas cr ON cr.id = h.replica_id
JOIN mz_clusters c ON c.id = cr.cluster_id
WHERE c.name = 'fast_start' AND i.name = 'pageview_fingerprint_idx'" "t" 120

log "S2: burst serving while steady hydrates"
cap 13-burst-serving.txt <<'EOF'
SELECT i.name AS index, cr.name AS replica, cr.size, h.hydrated
FROM mz_internal.mz_hydration_statuses h
JOIN mz_indexes i ON i.id = h.object_id
JOIN mz_cluster_replicas cr ON cr.id = h.replica_id
JOIN mz_clusters c ON c.id = cr.cluster_id
WHERE c.name = 'fast_start' AND i.name = 'pageview_fingerprint_idx'
ORDER BY cr.name;
SET cluster = fast_start;
\timing on
SELECT fp FROM pageview_fingerprint;
\timing off
EOF

poll "burst teardown" "SELECT coalesce(burst_size, 'none') FROM mz_internal.mz_cluster_reconfigurations r JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.name = 'fast_start'" "none" 240

log "S2: teardown state + audit"
cap 14-burst-teardown.txt <<'EOF'
SHOW CLUSTER REPLICAS WHERE cluster = 'fast_start';
SHOW CLUSTERS WHERE name = 'fast_start';
SELECT details->>'transition' AS transition,
       details->>'burst_size' AS burst_size, occurred_at
FROM mz_catalog.mz_audit_events
WHERE event_type = 'alter' AND object_type = 'cluster'
  AND details->>'cluster_name' = 'fast_start'
  AND details->>'transition' IS NOT NULL
ORDER BY id;
SELECT event_type, details->>'replica_name' AS replica,
       details->>'logical_size' AS size, details->>'reason' AS reason
FROM mz_catalog.mz_audit_events
WHERE object_type = 'cluster-replica' AND details->>'cluster_name' = 'fast_start'
ORDER BY id;
EOF

log "S2: guardrails + policy management"
cap 15-burst-guardrails.txt <<'EOF'
CREATE CLUSTER bad_burst (
  SIZE = 'scale=1,workers=2',
  AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=2'))
);
CREATE CLUSTER bad_burst (
  SIZE = 'scale=1,workers=1',
  SCHEDULE = ON REFRESH,
  AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=2'))
);
ALTER CLUSTER fast_start RESET (AUTO SCALING STRATEGY);
SHOW CREATE CLUSTER fast_start;
ALTER CLUSTER fast_start SET (AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=4')));
SHOW CREATE CLUSTER fast_start;
EOF

############################################################################
log "S3: ON REFRESH scheduling"
cap 16-on-refresh.txt <<'EOF'
CREATE CLUSTER nightly (
  SIZE = 'scale=1,workers=1',
  SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '60 seconds')
);
SELECT name, replication_factor FROM mz_clusters WHERE name = 'nightly';
SHOW CLUSTER REPLICAS WHERE cluster = 'nightly';
CREATE MATERIALIZED VIEW order_summary IN CLUSTER nightly
  WITH (REFRESH EVERY '1 minute')
  AS SELECT count(*) AS n, sum(amount) AS volume FROM orders;
SELECT mz_unsafe.mz_sleep(4);
SELECT name, replication_factor FROM mz_clusters WHERE name = 'nightly';
SHOW CLUSTER REPLICAS WHERE cluster = 'nightly';
SELECT n, volume FROM order_summary;
EOF

log "S3: WAIT FOR '0s' opt-out + full SHOW CLUSTERS"
cap 17-wait-for-0s.txt <<'EOF'
CREATE CLUSTER batch_jobs (SIZE = 'scale=1,workers=1');
SELECT mz_unsafe.mz_sleep(2);
ALTER CLUSTER batch_jobs SET (SIZE = 'scale=1,workers=2') WITH (WAIT FOR '0s');
SELECT mz_unsafe.mz_sleep(3);
SHOW CLUSTER REPLICAS WHERE cluster = 'batch_jobs';
SHOW CLUSTERS;
EOF

log "replay complete; transcripts in $OUT"
