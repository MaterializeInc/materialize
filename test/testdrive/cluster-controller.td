# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Boundary test for the cluster controller. With the master gate forced on and
# the tick interval driven down, the controller reconciles managed clusters many
# times within the test window. Convergence is asynchronous (a config-shape ALTER
# returns before the realized config settles), so every readback below is a
# retrying testdrive query (`>` re-runs until it matches or the SQL timeout
# elapses), never a fixed-duration sleep. Where a transient in-flight state must
# be observed, the controller is frozen first so the state cannot move; where the
# assertion is a negative ("the controller does nothing"), a sibling
# reconfiguration is used as a liveness synchronizer, proving the reconcile loop
# ran many ticks without acting on the cluster under test.

# Give convergence (provision + hydrate + cut-over) ample room on a loaded CI host
# without ever sleeping the full budget. `>` returns as soon as it matches.
$ set-sql-timeout duration=120s

# Force the gate on and drive the tick interval down so the controller ticks
# ~hundreds of times across the waits below. Both are re-read each tick, so the
# flips take effect without a restart. The graceful cases use the WITH (WAIT ...)
# surface, whose planner acceptance is gated on enable_zero_downtime.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_cluster_controller = true
ALTER SYSTEM SET cluster_controller_tick_interval = '5ms'
ALTER SYSTEM SET enable_zero_downtime_cluster_reconfiguration = true

# ----- Baseline reconcile is a no-op (PR 2) -----
#
# With only the implicit baseline strategy the desired replica set equals the
# realized one, so the controller never creates, drops, or renames a steady
# cluster's replicas no matter how often it ticks.

> CREATE CLUSTER cc_steady (SIZE 'scale=1,workers=1', REPLICATION FACTOR 2)

> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'cc_steady'
cc_steady r1 scale=1,workers=1
cc_steady r2 scale=1,workers=1

# Liveness synchronizer: an RF increase on a scratch cluster forces the controller
# to act (bring a replica up), proving the reconcile loop is live and has run many
# ticks. The controller reconciles every cluster on the same tick, so once the
# scratch cluster reaches its new RF, cc_steady has had every chance to be churned.
> CREATE CLUSTER cc_tick (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)
> ALTER CLUSTER cc_tick SET (REPLICATION FACTOR 2)
> SELECT count(*) FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'cc_tick'
2
> DROP CLUSTER cc_tick

# A controller that churned a steady replica, dropped and recreated it, even
# reusing the same name, would leave a dropped replica in the history. The
# baseline controller never does, so cc_steady has no dropped replica recorded.
# (Materialize forbids copying these system rows into a user table, so we assert
# no-drop directly.) This is not satisfied by gate-off legacy behavior, which
# never runs the reconcile loop at all.
> SELECT count(*) FROM mz_internal.mz_cluster_replica_history WHERE cluster_name = 'cc_steady' AND dropped_at IS NOT NULL
0

# A replication-factor change is a normal, immediate config update (PR 2 does not
# reshape ALTER); the controller then holds the new steady set without churn.
> ALTER CLUSTER cc_steady SET (REPLICATION FACTOR 1)

> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'cc_steady'
cc_steady r1 scale=1,workers=1

# The controller retired the surplus replica, so the drop is audited `retired`
# even though the replication-factor change was user-initiated.
> SELECT event_type, details->>'reason' FROM mz_catalog.mz_audit_events WHERE event_type = 'drop' AND object_type = 'cluster-replica' AND details->>'cluster_name' = 'cc_steady'
drop retired

> DROP CLUSTER cc_steady

# ----- Graceful background reconfiguration (PR 3) -----
#
# With the master gate on and background ALTER enabled, a config-shape ALTER (SIZE
# change) writes a durable reconfiguration record and returns immediately; the
# controller creates the target replica, and once it hydrates (an empty cluster
# hydrates promptly) cuts the realized size over to the target and drops the old
# replica.

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_background_alter_cluster = true

> CREATE CLUSTER cc_graceful (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

# Background ALTER returns immediately; the readbacks retry until cut-over.
> ALTER CLUSTER cc_graceful SET (SIZE 'scale=1,workers=2')

> SELECT size FROM mz_clusters WHERE name = 'cc_graceful'
scale=1,workers=2

# The cluster settled back to exactly REPLICATION FACTOR replicas at the new size,
# under a fresh name (r1 -> r2): a reshape never renames back.
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'cc_graceful'
cc_graceful r2 scale=1,workers=2

# The record was cleared after cut-over, so a fresh config-shape ALTER writes a new
# record and reshapes again (r2 -> r3) rather than folding onto a stale one. (An
# identical no-op ALTER would early-return before the reshape branch.)
> ALTER CLUSTER cc_graceful SET (SIZE 'scale=1,workers=1')

> SELECT size FROM mz_clusters WHERE name = 'cc_graceful'
scale=1,workers=1

> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'cc_graceful'
cc_graceful r3 scale=1,workers=1

# Replica lifecycle attribution, in order: the CREATE CLUSTER replica is `manual`,
# each reconfiguration's target replica is `reconfiguration`, and every controller
# drop (the old set retired at cut-over) is `retired`. testdrive sorts result rows
# (discarding ORDER BY), so we prepend a row_number() over the audit id, a
# deterministic ordinal that sorts the rows back into chronological order.
> SELECT row_number() OVER (ORDER BY id), event_type, details->>'reason' FROM mz_catalog.mz_audit_events WHERE object_type = 'cluster-replica' AND details->>'cluster_name' = 'cc_graceful'
1 create manual
2 create reconfiguration
3 drop retired
4 create reconfiguration
5 drop retired

> DROP CLUSTER cc_graceful

# A logging (INTROSPECTION) change is also a config-shape change: the controller
# reshapes it in the background and advances the realized config at cut-over.
# INTROSPECTION INTERVAL = 0 round-trips to NULL (Option::None for the interval).
> CREATE CLUSTER cc_logging (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, INTROSPECTION INTERVAL = '5s')

> SELECT introspection_interval::text FROM mz_clusters WHERE name = 'cc_logging'
00:00:05

> ALTER CLUSTER cc_logging SET (INTROSPECTION INTERVAL = 0)

> SELECT introspection_interval IS NULL FROM mz_clusters WHERE name = 'cc_logging'
true

> DROP CLUSTER cc_logging

# ----- Per-ALTER timeout / ON TIMEOUT (PR 3) -----
#
# The deadline and the on-timeout action come from the WITH (WAIT ...) surface and
# are written into the durable record, so the controller enforces them. These
# cases use a promptly-hydrating (empty) cluster, so success precedence cuts over
# before any deadline matters; they assert the three spellings are accepted under
# the gate and drive the record to a successful cut-over. ROLLBACK-vs-COMMIT
# behavior *at* a timeout (a target that never hydrates) is asserted
# deterministically in the controller's kernel unit tests.

> CREATE CLUSTER cc_timeout (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

# Omitting WITH (WAIT ...): default deadline and default ON TIMEOUT (now ROLLBACK).
# The target hydrates, so it cuts over regardless.
> ALTER CLUSTER cc_timeout SET (SIZE 'scale=1,workers=2')
> SELECT size FROM mz_clusters WHERE name = 'cc_timeout'
scale=1,workers=2

# Explicit ON TIMEOUT ROLLBACK with a generous deadline: success precedence cuts over.
> ALTER CLUSTER cc_timeout SET (SIZE 'scale=1,workers=4') WITH (WAIT UNTIL READY (TIMEOUT '60s', ON TIMEOUT 'ROLLBACK'))
> SELECT size FROM mz_clusters WHERE name = 'cc_timeout'
scale=1,workers=4

# Explicit ON TIMEOUT COMMIT, and WAIT FOR (which desugars to ON TIMEOUT COMMIT):
# both accepted under the gate and drive a cut-over.
> ALTER CLUSTER cc_timeout SET (SIZE 'scale=1,workers=2') WITH (WAIT UNTIL READY (TIMEOUT '60s', ON TIMEOUT 'COMMIT'))
> SELECT size FROM mz_clusters WHERE name = 'cc_timeout'
scale=1,workers=2

> ALTER CLUSTER cc_timeout SET (SIZE 'scale=1,workers=1') WITH (WAIT FOR '60s')
> SELECT size FROM mz_clusters WHERE name = 'cc_timeout'
scale=1,workers=1

> DROP CLUSTER cc_timeout

# ----- Observability: introspection relations + SHOW CLUSTERS (PR 4) -----
#
# mz_internal.mz_cluster_reconfigurations is sparse: a row only while a graceful
# reconfiguration is in flight, carrying the in-flight delta (target shape as JSON,
# deadline, on_timeout). SHOW CLUSTERS folds that and the auto-scaling state into a
# single `activity` summary, NULL when steady.

> CREATE CLUSTER cc_obs (SIZE 'scale=1,workers=1', REPLICATION FACTOR 2)

# Steady state: nothing in flight, so the sparse relation has no row ...
> SELECT count(*) FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_obs'
0

# ... and SHOW CLUSTERS reports no activity.
> SELECT name, activity FROM (SHOW CLUSTERS) WHERE name = 'cc_obs'
cc_obs <null>

# A completed background reconfiguration advances the realized config and clears
# the in-flight record, so the cluster settles back to no activity.
> ALTER CLUSTER cc_obs SET (SIZE 'scale=1,workers=2')
> SELECT size FROM mz_clusters WHERE name = 'cc_obs'
scale=1,workers=2
> SELECT name, activity FROM (SHOW CLUSTERS) WHERE name = 'cc_obs'
cc_obs <null>

# The audit log captured the lifecycle: `started` when the record was written,
# `finalized` at cut-over. Every transition records the active deadline.
> SELECT row_number() OVER (ORDER BY id), details->>'transition', details->>'deadline' IS NOT NULL FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_obs' AND details->>'transition' IS NOT NULL
1 started true
2 finalized true

# The event carries the full target shape, not just size and replication factor,
# so an AZ- or introspection-only reconfiguration (which leaves size and rf
# unchanged) is still legible here. cc_obs pins no zones and overrides no logging,
# so those read as their defaults: an empty zone list, introspection debugging
# off, and the default 1s introspection interval.
> SELECT details->>'target_size', details->>'target_replication_factor', details->>'target_availability_zones', details->'target_logging'->>'log_logging', details->'target_logging'->>'interval' FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_obs' AND details->>'transition' = 'started'
scale=1,workers=2 2 [] false "{\"nanos\":0,\"secs\":1}"

> DROP CLUSTER cc_obs

# An ALTER-back cancel: a reconfiguration is started, then re-targeted back to the
# cluster's still-realized size before it cuts over. The lifecycle records a
# `started`, then `cancelled` at the ALTER-back, then `finalized` as the now-trivial
# (target == realized) reconfiguration clears. The two ALTERs are issued back to
# back so the controller cannot cut the first over (a cut-over needs a target
# replica to boot and hydrate) before the re-target lands.
> CREATE CLUSTER cc_cancel (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)
> SELECT count(*) FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'cc_cancel'
1

> ALTER CLUSTER cc_cancel SET (SIZE 'scale=1,workers=2')
> ALTER CLUSTER cc_cancel SET (SIZE 'scale=1,workers=1')

> SELECT size FROM mz_clusters WHERE name = 'cc_cancel'
scale=1,workers=1
> SELECT name, activity FROM (SHOW CLUSTERS) WHERE name = 'cc_cancel'
cc_cancel <null>
> SELECT row_number() OVER (ORDER BY id), details->>'transition' FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_cancel' AND details->>'transition' IS NOT NULL
1 started
2 cancelled
3 finalized

> DROP CLUSTER cc_cancel

# ----- Rollback at the deadline: record cleared + timed-out event (PR 8b) -----
#
# A reconfiguration whose deadline passes before the target hydrates under ON
# TIMEOUT ROLLBACK is abandoned: the controller drops the target set and clears the
# record without touching the realized config, and the audit log records a
# `timed-out` transition, the timeout's only papertrail. A TIMEOUT of '0s' puts
# the deadline in the past, so the rollback happens on the controller's first tick.

> CREATE CLUSTER cc_rollback (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> ALTER CLUSTER cc_rollback SET (SIZE 'scale=1,workers=2') WITH (WAIT UNTIL READY (TIMEOUT '0s', ON TIMEOUT 'ROLLBACK'))

# The `timed-out` transition is the definitive rollback signal; poll for it, then
# read back the settled state. The deadline rides along on every transition.
> SELECT row_number() OVER (ORDER BY id), details->>'transition', details->>'deadline' IS NOT NULL FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_rollback' AND details->>'transition' IS NOT NULL
1 started true
2 timed-out true

# Rolled back: realized config untouched, nothing in flight, replica set unchanged.
> SELECT size FROM mz_clusters WHERE name = 'cc_rollback'
scale=1,workers=1
> SELECT count(*) FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_rollback'
0
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'cc_rollback'
cc_rollback r1 scale=1,workers=1

# A forced cut-over: TIMEOUT '0s' with ON TIMEOUT COMMIT advances the realized
# config to the (necessarily un-hydrated) target on the first tick. The clear
# classifies `finalized` with the deadline carried, so a reader can tell it was a
# past-deadline (forced) cut-over.
> ALTER CLUSTER cc_rollback SET (SIZE 'scale=1,workers=2') WITH (WAIT UNTIL READY (TIMEOUT '0s', ON TIMEOUT 'COMMIT'))
> SELECT size FROM mz_clusters WHERE name = 'cc_rollback'
scale=1,workers=2
> SELECT count(*) FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_rollback'
0

# An in-flight reconfiguration observed deterministically: freeze the controller by
# flipping the master gate off right after the record is written (both are re-read
# each tick), so the record cannot move. The flip lands within milliseconds, while
# a cut-over first needs a target replica to boot and report hydration.
> ALTER CLUSTER cc_rollback SET (SIZE 'scale=1,workers=4') WITH (WAIT UNTIL READY (TIMEOUT '60s', ON TIMEOUT 'ROLLBACK'))

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_cluster_controller = false

# The sparse relation has a row: the in-flight target (as JSON), the deadline, and
# the on_timeout action, while the realized size stays at the pre-ALTER shape.
> SELECT recon.on_timeout, recon.target->>'size', mz_clusters.size FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_rollback'
rollback scale=1,workers=4 scale=1,workers=2

# SHOW CLUSTERS folds that into the single `activity` column. (Compared as a
# boolean because the multi-word activity string cannot be matched as a bare value.)
> SELECT name, activity = 'reconfiguring to scale=1,workers=4' FROM (SHOW CLUSTERS) WHERE name = 'cc_rollback'
cc_rollback true

# Unfreeze: the controller converges the in-flight record and settles.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_cluster_controller = true

> SELECT size FROM mz_clusters WHERE name = 'cc_rollback'
scale=1,workers=4
> SELECT count(*) FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_rollback'
0

# The full lifecycle history, in order: the rolled-back attempt, the forced COMMIT
# cut-over, and the frozen-then-converged success. Every transition carries the deadline.
> SELECT row_number() OVER (ORDER BY id), details->>'transition', details->>'deadline' IS NOT NULL FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_rollback' AND details->>'transition' IS NOT NULL
1 started true
2 timed-out true
3 started true
4 finalized true
5 started true
6 finalized true

> DROP CLUSTER cc_rollback
