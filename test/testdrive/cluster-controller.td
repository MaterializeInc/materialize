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

# ----- Baseline reconcile is a no-op -----
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

# A replication-factor change is a normal, immediate config update, not a
# reshape: only config-shape changes need a hydrate-overlap. The controller
# then holds the new steady set without churn.
> ALTER CLUSTER cc_steady SET (REPLICATION FACTOR 1)

> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'cc_steady'
cc_steady r1 scale=1,workers=1

# The controller retired the surplus replica, so the drop is audited `retired`
# even though the replication-factor change was user-initiated.
> SELECT event_type, details->>'reason' FROM mz_catalog.mz_audit_events WHERE event_type = 'drop' AND object_type = 'cluster-replica' AND details->>'cluster_name' = 'cc_steady'
drop retired

> DROP CLUSTER cc_steady

# ----- Graceful background reconfiguration -----
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

# The record settled as finalized at cut-over, so a fresh config-shape ALTER
# writes a new record and reshapes again (r2 -> r3) rather than folding onto the
# settled one. (An identical no-op ALTER would early-return before the reshape
# branch.)
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

# The reconfiguration lifecycle itself is audited: each reshape records a
# `started` when its record is written and a `finalized` at cut-over. Both
# cut-overs here were reached by hydration, not by a deadline commit, so
# `forced` reads false (it is absent on `started` events).
> SELECT row_number() OVER (ORDER BY id), details->>'transition', details->>'forced' FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_graceful' AND details->>'transition' IS NOT NULL
1 started <null>
2 finalized false
3 started <null>
4 finalized false

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

# ----- ALTER-back settles at the original shape -----
#
# An ALTER to a new size immediately followed by an ALTER back to the original
# one settles at the original size with a single replica. Whether the second
# ALTER cancels the in-flight reconfiguration (the common case) or the first
# has already cut over and the second reshapes back is a matter of timing, so
# only the convergent outcome is asserted here. The cancel semantics themselves
# (immediate terminal cancel, no churn) are pinned deterministically in the
# controller's kernel unit tests.
> CREATE CLUSTER cc_altback (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> ALTER CLUSTER cc_altback SET (SIZE 'scale=1,workers=2')
> ALTER CLUSTER cc_altback SET (SIZE 'scale=1,workers=1')

> SELECT size FROM mz_clusters WHERE name = 'cc_altback'
scale=1,workers=1

# A single replica at the realized size. Its name is timing-dependent (a
# cancel keeps the original replica, a cut-over-and-back advances to a fresh
# name), so no name is asserted.
> SELECT r.size FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'cc_altback'
scale=1,workers=1

> DROP CLUSTER cc_altback

# ----- Non-shape changes while a reconfiguration is in flight -----
#
# The WAIT-without-shape rejection is planner-level and covered in
# managed_cluster.slt. Asserted here: the replication factor is refused while
# a reconfiguration is in flight (it is one of the four dimensions the
# cut-over sets atomically from the record's target), and a WORKLOAD CLASS
# change applies immediately without disturbing the reconfiguration.
#
# The refused ALTER is issued immediately after the SIZE change, so the
# reconfiguration is still in flight when it lands. If the reconfiguration
# somehow cut over first, the ALTER would succeed and this fails loudly
# rather than hangs.
> CREATE CLUSTER cc_nonshape (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> ALTER CLUSTER cc_nonshape SET (SIZE 'scale=1,workers=2')

! ALTER CLUSTER cc_nonshape SET (REPLICATION FACTOR 2)
contains:cannot change replication factor while a reconfiguration is in progress

# WORKLOAD CLASS (system-role-only) applies immediately, whether or not the
# reconfiguration is still in flight.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER CLUSTER cc_nonshape SET (WORKLOAD CLASS 'test')

> SELECT workload_class FROM mz_internal.mz_cluster_workload_classes WHERE id = (SELECT id FROM mz_clusters WHERE name = 'cc_nonshape')
test

# The reconfiguration converges normally: the WORKLOAD CLASS change did not
# disturb the record, and its lifecycle is exactly `started`, `finalized`.
> SELECT size FROM mz_clusters WHERE name = 'cc_nonshape'
scale=1,workers=2

> SELECT row_number() OVER (ORDER BY id), details->>'transition' FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_nonshape' AND details->>'transition' IS NOT NULL
1 started
2 finalized

# A shape option set to its already-realized value with nothing in flight
# reconfigures nothing: the non-shape change applies directly, no record is
# written, and no lifecycle transition is recorded.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER CLUSTER cc_nonshape SET (SIZE 'scale=1,workers=2', WORKLOAD CLASS 'test2')

> SELECT workload_class FROM mz_internal.mz_cluster_workload_classes WHERE id = (SELECT id FROM mz_clusters WHERE name = 'cc_nonshape')
test2

> SELECT count(*) FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_nonshape' AND details->>'transition' IS NOT NULL
2

> DROP CLUSTER cc_nonshape

# ----- Per-ALTER timeout / ON TIMEOUT -----
#
# The deadline and the on-timeout action come from the WITH (WAIT ...) surface and
# are written into the durable record, so the controller enforces them. These
# cases use a promptly-hydrating (empty) cluster, so success precedence cuts over
# before any deadline matters. They assert the three spellings are accepted under
# the gate and drive the record to a successful cut-over. ROLLBACK-vs-COMMIT
# behavior *at* a timeout (a target that never hydrates) is pinned in the
# controller's kernel unit tests and asserted end-to-end in the deadline
# scenarios below.

> CREATE CLUSTER cc_timeout (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

# Omitting WITH (WAIT ...): default deadline and the implicit ON TIMEOUT
# (ROLLBACK on the controller path). The target hydrates, so it cuts over
# regardless.
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

# ----- Deadline elapses un-hydrated: ON TIMEOUT ROLLBACK and COMMIT -----
#
# A materialized view that sleeps for an hour per input row keeps any fresh
# replica from hydrating within a 1s deadline, so the deadline genuinely
# fires. The block is wall-clock (`mz_sleep`), not a CPU-speed bet, so it
# holds on any host and burns no CPU. The sleep argument depends on table
# data, which keeps it out of reach of constant folding, mirroring the
# established sleep-in-dataflow pattern. The definitive signal for the
# resolution is the terminal audit transition, which the implicit query retry
# below waits for.

# The sleeping views reference `mz_unsafe`, an unstable schema, which durable
# objects may only depend on under this flag.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unstable_dependencies = true

# ROLLBACK: the controller drops the target set at the deadline, leaves the
# realized config untouched, and audits the record as timed-out.
> CREATE CLUSTER cc_deadline (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> CREATE TABLE cc_deadline_t (id int)

> INSERT INTO cc_deadline_t VALUES (1)

> CREATE MATERIALIZED VIEW cc_deadline_slow IN CLUSTER cc_deadline AS
  SELECT mz_unsafe.mz_sleep(id * 3600) AS s FROM cc_deadline_t

> ALTER CLUSTER cc_deadline SET (SIZE 'scale=1,workers=4') WITH (WAIT UNTIL READY (TIMEOUT '1s', ON TIMEOUT 'ROLLBACK'))

> SELECT details->>'transition' FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_deadline' AND details->>'transition' IS NOT NULL ORDER BY id
started
timed-out

# The realized config is untouched and only the original replica remains.
> SELECT size FROM mz_clusters WHERE name = 'cc_deadline'
scale=1,workers=1

> SELECT r.size FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'cc_deadline'
scale=1,workers=1

> DROP CLUSTER cc_deadline CASCADE

# COMMIT: the controller cuts over to the un-hydrated target at the deadline
# and audits the cut-over as forced. This is the one path that sets the
# `forced` field in the audit details.
> CREATE CLUSTER cc_forced (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> CREATE TABLE cc_forced_t (id int)

> INSERT INTO cc_forced_t VALUES (1)

> CREATE MATERIALIZED VIEW cc_forced_slow IN CLUSTER cc_forced AS
  SELECT mz_unsafe.mz_sleep(id * 3600) AS s FROM cc_forced_t

> ALTER CLUSTER cc_forced SET (SIZE 'scale=1,workers=4') WITH (WAIT UNTIL READY (TIMEOUT '1s', ON TIMEOUT 'COMMIT'))

> SELECT details->>'transition', details->>'forced' FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_forced' AND details->>'transition' IS NOT NULL ORDER BY id
started <null>
finalized true

# The realized config advanced to the target despite the un-hydrated replica.
> SELECT size FROM mz_clusters WHERE name = 'cc_forced'
scale=1,workers=4

> DROP CLUSTER cc_forced CASCADE

# A second ALTER to the same target can force an in-flight reconfiguration to
# cut over without replacing the target replica.
> CREATE CLUSTER cc_force_existing (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> CREATE TABLE cc_force_existing_t (id int)

> INSERT INTO cc_force_existing_t VALUES (1)

> CREATE MATERIALIZED VIEW cc_force_existing_slow IN CLUSTER cc_force_existing AS
  SELECT mz_unsafe.mz_sleep(id * 3600) AS s FROM cc_force_existing_t

> ALTER CLUSTER cc_force_existing SET (SIZE 'scale=1,workers=4') WITH (WAIT UNTIL READY (TIMEOUT '600s', ON TIMEOUT 'ROLLBACK'))

# The long deadline leaves both the realized and un-hydrated target replicas
# online while the reconfiguration remains in progress.
> SELECT r.size, bool_and(status.status = 'online')
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id
  JOIN mz_internal.mz_cluster_replica_statuses status ON status.replica_id = r.id
  WHERE c.name = 'cc_force_existing'
  GROUP BY r.size
scale=1,workers=1 true
scale=1,workers=4 true

> SELECT recon.status
  FROM mz_internal.mz_cluster_reconfigurations recon
  JOIN mz_clusters c ON c.id = recon.cluster_id
  WHERE c.name = 'cc_force_existing'
in-progress

$ set-from-sql var=cc_force_existing_target_id
SELECT r.id::text
FROM mz_cluster_replicas r
JOIN mz_clusters c ON c.id = r.cluster_id
WHERE c.name = 'cc_force_existing' AND r.size = 'scale=1,workers=4'

# Replace only the deadline and timeout action. The existing target shape and
# replica are reused, and the elapsed deadline forces cut-over on the next tick.
> ALTER CLUSTER cc_force_existing SET (SIZE 'scale=1,workers=4') WITH (WAIT UNTIL READY (TIMEOUT '0s', ON TIMEOUT 'COMMIT'))

> SELECT r.id::text = '${cc_force_existing_target_id}', r.size
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id
  WHERE c.name = 'cc_force_existing'
true scale=1,workers=4

> SELECT row_number() OVER (ORDER BY id), details->>'transition', details->>'forced'
  FROM mz_catalog.mz_audit_events
  WHERE event_type = 'alter'
    AND object_type = 'cluster'
    AND details->>'cluster_name' = 'cc_force_existing'
    AND details->>'transition' IS NOT NULL
1 started <null>
2 started <null>
3 finalized true

# Only the first ALTER created a target replica.
> SELECT count(*)
  FROM mz_catalog.mz_audit_events
  WHERE event_type = 'create'
    AND object_type = 'cluster-replica'
    AND details->>'cluster_name' = 'cc_force_existing'
    AND details->>'reason' = 'reconfiguration'
1

> DROP CLUSTER cc_force_existing CASCADE

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unstable_dependencies = false

# ----- A single-replica sink does not block graceful reconfiguration -----
#
# With enable_background_alter_cluster off, a config-shape ALTER blocks the session
# on the wait-shim instead of returning immediately. The shim keeps no timer of its
# own: it waits for the controller to resolve the durable record, then reports
# success once the realized config reached the requested shape. A singleton sink
# remains on the old replica until cut-over, so readiness must not wait for the
# sink to hydrate on the target replica.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_background_alter_cluster = false
ALTER SYSTEM SET cluster_alter_check_ready_interval = '100ms'

> CREATE CLUSTER cc_fg (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> CREATE TABLE cc_fg_t (id int)

> INSERT INTO cc_fg_t VALUES (1)

> CREATE CONNECTION cc_fg_kafka TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    SECURITY PROTOCOL PLAINTEXT
  )

> CREATE SINK cc_fg_sink
  IN CLUSTER cc_fg
  FROM cc_fg_t
  INTO KAFKA CONNECTION cc_fg_kafka (TOPIC 'testdrive-cluster-controller-sink-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.cc_fg_sink key=false sort-messages=true
{"before": null, "after": {"id": 1}}

# Blocks until the controller cuts the realized size over, then returns success.
> ALTER CLUSTER cc_fg SET (SIZE 'scale=1,workers=2')

> SELECT r.size
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id
  WHERE c.name = 'cc_fg'
scale=1,workers=2

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'cc_fg_sink'
running

> INSERT INTO cc_fg_t VALUES (2)

$ kafka-verify-data format=json sink=materialize.public.cc_fg_sink key=false sort-messages=true
{"before": null, "after": {"id": 2}}

# Background mode exercises the same readiness check without the wait shim.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_background_alter_cluster = true
ALTER SYSTEM RESET cluster_alter_check_ready_interval

> ALTER CLUSTER cc_fg SET (SIZE 'scale=1,workers=1')

> SELECT r.size
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id
  WHERE c.name = 'cc_fg'
scale=1,workers=1

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'cc_fg_sink'
running

> INSERT INTO cc_fg_t VALUES (3)

$ kafka-verify-data format=json sink=materialize.public.cc_fg_sink key=false sort-messages=true
{"before": null, "after": {"id": 3}}

> DROP CLUSTER cc_fg CASCADE
> DROP TABLE cc_fg_t
> DROP CONNECTION cc_fg_kafka

# ----- A reshape whose transient peak exceeds the replica budget is rejected -----
#
# A shape-change reshape runs the realized and target replica sets side by side
# until cut-over, so the transient peak is realized_rf + target_rf. The ALTER
# validates that peak, matching the legacy wait path (which creates the full
# target set as pending replicas at ALTER time), and rejects synchronously: no
# record is written and nothing has to be aborted. Apply-time exhaustion, and
# the controller's abort reaction, remains reachable only when a limit shrinks
# or the environment grows after the record is written, which is covered by
# kernel unit tests.

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET max_replicas_per_cluster = 3

> CREATE CLUSTER cc_budget (SIZE 'scale=1,workers=1', REPLICATION FACTOR 2)

# The settled target (2 replicas) fits the limit of 3, but the peak
# (2 realized + 2 target = 4) does not.
! ALTER CLUSTER cc_budget SET (SIZE 'scale=1,workers=2')
contains:would violate max_replicas_per_cluster limit

# The ALTER was rejected outright: no record was written, no lifecycle
# transition was audited, and the realized shape is untouched.
> SELECT count(*) FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_budget' AND details->>'transition' IS NOT NULL
0

> SELECT size, replication_factor FROM mz_clusters WHERE name = 'cc_budget'
scale=1,workers=1 2

> DROP CLUSTER cc_budget

# Reset the limit after this section.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM RESET max_replicas_per_cluster

# ----- The credit budget is validated the same way, against the live environment -----
#
# The credit peak is likewise credit(realized) + credit(target), but its budget
# is environment-wide, so the limit here is computed from the live replica set.

> CREATE CLUSTER cc_credit (SIZE 'scale=1,workers=1', REPLICATION FACTOR 2)

# Compute a limit that fits the settled target but not the peak. Test-env
# credits are scale*workers, so cc_credit's realized set costs 2 and the
# target below costs 4. The current environment-wide rate includes cc_credit's
# realized 2, so adding 3 yields others + 5: the settled state (others + 4)
# fits, the peak (others + 6) does not.
$ set-from-sql var=credit_limit
SELECT (sum(s.credits_per_hour) + 3)::text FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id JOIN mz_catalog.mz_cluster_replica_sizes s ON r.size = s.size WHERE c.id LIKE 'u%'

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET max_credit_consumption_rate = ${credit_limit}

! ALTER CLUSTER cc_credit SET (SIZE 'scale=1,workers=2')
contains:would violate max_credit_consumption_rate limit

# The ALTER was rejected outright: no record was written, no lifecycle
# transition was audited, and the realized shape is untouched.
> SELECT count(*) FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_credit' AND details->>'transition' IS NOT NULL
0

> SELECT size, replication_factor FROM mz_clusters WHERE name = 'cc_credit'
scale=1,workers=1 2

> DROP CLUSTER cc_credit

# Reset the limit after this section.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM RESET max_credit_consumption_rate

# ----- Converting to unmanaged is refused while a reconfiguration is in flight -----
#
# The unmanaged variant has no reconfiguration field, so the conversion would
# silently drop an in-progress record with no terminal status and no audit
# event, and strand any overlap replicas. The conversion is refused instead.
# The controller is frozen (master gate off) right after the record is written
# so the in-flight state holds still while the rejection is asserted.

> CREATE CLUSTER cc_unmanaged (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)
> ALTER CLUSTER cc_unmanaged SET (SIZE 'scale=1,workers=2')

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_cluster_controller = false

! ALTER CLUSTER cc_unmanaged SET (MANAGED = false)
contains:cannot convert cluster to unmanaged while a reconfiguration is in progress

# Unfreeze and cancel by ALTERing back to the realized size. Once the record
# settles (the retrying `>` rides out the interim), the conversion goes
# through.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_cluster_controller = true

> ALTER CLUSTER cc_unmanaged SET (SIZE 'scale=1,workers=1')
> ALTER CLUSTER cc_unmanaged SET (MANAGED = false)

> DROP CLUSTER cc_unmanaged CASCADE

# ----- Observability: introspection relations + SHOW CLUSTERS -----
#
# mz_internal.mz_cluster_reconfigurations retains one row per cluster that has been
# reconfigured: the target delta (target shape as JSON, deadline, on_timeout) and a
# `status` that is `in-progress` while the reconfiguration runs, then a terminal
# `finalized` / `timed-out` / `cancelled`. The row is never cleared by settling,
# only overwritten by the next ALTER. SHOW CLUSTERS folds an in-progress
# reconfiguration and the auto-scaling state into a single `activity` summary, NULL
# when steady.

> CREATE CLUSTER cc_obs (SIZE 'scale=1,workers=1', REPLICATION FACTOR 2)

# Never reconfigured yet, so the relation has no row for this cluster ...
> SELECT count(*) FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_obs'
0

# ... and SHOW CLUSTERS reports no activity.
> SELECT name, activity FROM (SHOW CLUSTERS) WHERE name = 'cc_obs'
cc_obs <null>

# A completed background reconfiguration advances the realized config and retains
# the record with status `finalized`, so activity settles back to NULL (only an
# in-progress reconfiguration reads as activity).
> ALTER CLUSTER cc_obs SET (SIZE 'scale=1,workers=2')
> SELECT size FROM mz_clusters WHERE name = 'cc_obs'
scale=1,workers=2
> SELECT recon.status FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_obs'
finalized
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

# An ALTER-back: a reconfiguration is started, then re-targeted back to the
# cluster's original size. Whether the second ALTER cancels the in-flight
# record or the first has already cut over and the second reshapes back is a
# matter of timing, so only convergent facts are asserted: the cluster
# settles at the original size with a single replica, the lifecycle's first
# transition is `started` and its last is terminal, and the retained record
# reports a terminal status. The exact cancel semantics are pinned in the
# controller's kernel unit tests.
> CREATE CLUSTER cc_cancel (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> ALTER CLUSTER cc_cancel SET (SIZE 'scale=1,workers=2')
> ALTER CLUSTER cc_cancel SET (SIZE 'scale=1,workers=1')

> SELECT size FROM mz_clusters WHERE name = 'cc_cancel'
scale=1,workers=1
> SELECT name, activity FROM (SHOW CLUSTERS) WHERE name = 'cc_cancel'
cc_cancel <null>

> SELECT (SELECT details->>'transition' FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_cancel' AND details->>'transition' IS NOT NULL ORDER BY id ASC LIMIT 1), (SELECT details->>'transition' IN ('cancelled', 'finalized') FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_cancel' AND details->>'transition' IS NOT NULL ORDER BY id DESC LIMIT 1)
started true

> SELECT recon.status IN ('cancelled', 'finalized') FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_cancel'
true
> SELECT count(*) FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'cc_cancel'
1

> DROP CLUSTER cc_cancel

# ----- Rollback at the deadline: record retained as timed-out + timed-out event -----
#
# A reconfiguration whose deadline passes before the target hydrates under ON
# TIMEOUT ROLLBACK is abandoned: the controller drops the target set and retains the
# record with status `timed-out` without touching the realized config, and the audit
# log records a matching `timed-out` transition. A TIMEOUT of '0s' puts the deadline
# in the past, so the rollback happens on the controller's first tick.

> CREATE CLUSTER cc_rollback (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> ALTER CLUSTER cc_rollback SET (SIZE 'scale=1,workers=2') WITH (WAIT UNTIL READY (TIMEOUT '0s', ON TIMEOUT 'ROLLBACK'))

# The `timed-out` transition is the definitive rollback signal; poll for it, then
# read back the settled state. The deadline rides along on every transition.
> SELECT row_number() OVER (ORDER BY id), details->>'transition', details->>'deadline' IS NOT NULL FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_rollback' AND details->>'transition' IS NOT NULL
1 started true
2 timed-out true

# Rolled back: realized config untouched, the record retained as timed-out, replica set unchanged.
> SELECT size FROM mz_clusters WHERE name = 'cc_rollback'
scale=1,workers=1
> SELECT recon.status FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_rollback'
timed-out
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'cc_rollback'
cc_rollback r1 scale=1,workers=1

# A forced cut-over: TIMEOUT '0s' with ON TIMEOUT COMMIT advances the realized
# config to the (necessarily un-hydrated) target on the first tick. The retained
# record carries status `finalized` with the deadline, so a reader can tell it was a
# past-deadline (forced) cut-over.
> ALTER CLUSTER cc_rollback SET (SIZE 'scale=1,workers=2') WITH (WAIT UNTIL READY (TIMEOUT '0s', ON TIMEOUT 'COMMIT'))
> SELECT size FROM mz_clusters WHERE name = 'cc_rollback'
scale=1,workers=2
> SELECT recon.status FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_rollback'
finalized

# An in-flight reconfiguration observed deterministically: a sleeping materialized
# view in the cluster (the same mz_sleep pattern as above) keeps the target set
# from hydrating, so the record stays in-progress. The deadline is far away, so no
# timeout can interfere. There is no unblock-and-converge step: the sleep runs on
# the replica's worker thread, so even dropping the view cannot unwedge the
# replicas. The scenario instead ends by dropping the cluster with the record in
# flight, which must succeed and leaves the lifecycle at `started`.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unstable_dependencies = true

> CREATE TABLE cc_rollback_t (id int)

> INSERT INTO cc_rollback_t VALUES (1)

> CREATE MATERIALIZED VIEW cc_rollback_slow IN CLUSTER cc_rollback AS
  SELECT mz_unsafe.mz_sleep(id * 3600) AS s FROM cc_rollback_t

# Change several dimensions at once: size and introspection interval are shape
# dimensions (each alone would route through a record), and the replication
# factor rides along in the same statement's record.
> ALTER CLUSTER cc_rollback SET (SIZE 'scale=1,workers=4', REPLICATION FACTOR 2, INTROSPECTION INTERVAL '2s') WITH (WAIT UNTIL READY (TIMEOUT '600s', ON TIMEOUT 'ROLLBACK'))

# The relation shows the in-flight reconfiguration: status `in-progress`, the target
# (as JSON), the deadline, and the on_timeout action, while the realized size stays
# at the pre-ALTER shape.
> SELECT recon.status, recon.on_timeout, recon.target->>'size', mz_clusters.size FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_rollback'
in-progress rollback scale=1,workers=4 scale=1,workers=2

# `changes` diffs the target against the realized config per dimension: all three
# altered dimensions appear with their target values, and the untouched
# availability zones do not.
> SELECT recon.changes->>'size', recon.changes->>'replication_factor', recon.changes->'logging'->'interval'->>'secs', recon.changes->'availability_zones' IS NULL FROM mz_internal.mz_cluster_reconfigurations recon JOIN mz_clusters ON mz_clusters.id = recon.cluster_id WHERE mz_clusters.name = 'cc_rollback'
scale=1,workers=4 2 2 true

# SHOW CLUSTERS folds that into the single `activity` column, naming exactly the
# changed dimensions. (Compared as a boolean because the multi-word activity
# string cannot be matched as a bare value.)
> SELECT name, activity = 'reconfiguring size to scale=1,workers=4, replication factor to 2, introspection settings' FROM (SHOW CLUSTERS) WHERE name = 'cc_rollback'
cc_rollback true

# Dropping the cluster with the record in flight must succeed (the sleeping view
# rides along via CASCADE), and the controller's stale in-flight decisions are
# rejected rather than applied against the vanished cluster.
> DROP CLUSTER cc_rollback CASCADE

> DROP TABLE cc_rollback_t

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unstable_dependencies = false

# The audit history survives the drop, in order: the rolled-back attempt, the
# forced COMMIT cut-over, and the in-flight attempt that ended with the cluster
# dropped. Every transition carries the deadline.
> SELECT row_number() OVER (ORDER BY id), details->>'transition', details->>'deadline' IS NOT NULL FROM mz_catalog.mz_audit_events WHERE event_type = 'alter' AND object_type = 'cluster' AND details->>'cluster_name' = 'cc_rollback' AND details->>'transition' IS NOT NULL
1 started true
2 timed-out true
3 started true
4 finalized true
5 started true

# ----- Hydration burst: AUTO SCALING STRATEGY SQL surface -----

# SQL acceptance is gated by the enable_auto_scaling_strategy feature flag. The
# break-glass burst strategy is disabled so the in-flight burst state here is
# deterministically empty regardless of arming semantics. This section exercises
# parse/render/validation, not arming.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_auto_scaling_strategy = true
ALTER SYSTEM SET enable_hydration_burst = false

# A managed cluster can carry an ON HYDRATION burst policy.
> CREATE CLUSTER cc_burst (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2', LINGER DURATION = '600s')))

# SHOW CREATE CLUSTER renders the policy back, and the rendered statement re-parses
# (the planner's create-roundtrip asserts unplan->parse->plan equality).
> SELECT create_sql LIKE '%AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = ''scale=2,workers=2''%' FROM (SHOW CREATE CLUSTER cc_burst)
true

# The cluster serves a steady replica set at its own size (the baseline).
> SELECT replication_factor FROM mz_catalog.mz_clusters WHERE name = 'cc_burst'
1

# mz_cluster_auto_scaling_strategies surfaces the configured policy (the strategy
# JSON carries the HYDRATION SIZE) and the in-flight state. With the burst strategy
# disabled no burst is ever armed, so `state` is NULL.
> SELECT mz_clusters.name, scaling.strategy->'on_hydration'->>'hydration_size', scaling.state IS NULL FROM mz_internal.mz_cluster_auto_scaling_strategies scaling JOIN mz_clusters ON mz_clusters.id = scaling.cluster_id WHERE mz_clusters.name = 'cc_burst'
cc_burst scale=2,workers=2 true

# With nothing in flight, SHOW CLUSTERS reports no activity.
> SELECT name, activity FROM (SHOW CLUSTERS) WHERE name = 'cc_burst'
cc_burst <null>

# RESET disables autoscaling; SHOW CREATE no longer renders the block.
> ALTER CLUSTER cc_burst RESET (AUTO SCALING STRATEGY)
> SELECT create_sql LIKE '%AUTO SCALING STRATEGY%' FROM (SHOW CREATE CLUSTER cc_burst)
false

# Setting it back via ALTER also works (omitting LINGER DURATION).
> ALTER CLUSTER cc_burst SET (AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2')))
> SELECT create_sql LIKE '%AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = ''scale=2,workers=2''))%' FROM (SHOW CREATE CLUSTER cc_burst)
true

> DROP CLUSTER cc_burst

# HYDRATION SIZE equal to the cluster SIZE is rejected (a burst at the same size
# would not accelerate hydration).
! CREATE CLUSTER cc_burst_bad (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=1')))
contains:HYDRATION SIZE must differ from the cluster SIZE

# A HYDRATION SIZE must name a real replica size, validated at DDL time like
# SIZE itself, so a typo cannot park an unsatisfiable burst on the controller.
! CREATE CLUSTER cc_burst_typo (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'notasize')))
contains:unknown cluster replica size notasize

# AUTO SCALING STRATEGY combined with a non-MANUAL SCHEDULE is rejected.
! CREATE CLUSTER cc_burst_sched (SIZE 'scale=1,workers=1', SCHEDULE = ON REFRESH, AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2')))
contains:AUTO SCALING STRATEGY cannot be combined with a SCHEDULE other than MANUAL

# The two invariants are also enforced when an ALTER changes the *other* side of
# the constraint on a cluster that already carries a strategy.
> CREATE CLUSTER cc_burst_alt (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2')))

# Setting SIZE equal to the existing HYDRATION SIZE would make the burst a no-op.
! ALTER CLUSTER cc_burst_alt SET (SIZE = 'scale=2,workers=2')
contains:HYDRATION SIZE must differ from the cluster SIZE

# Setting a non-MANUAL SCHEDULE on a cluster that already has a strategy is rejected.
! ALTER CLUSTER cc_burst_alt SET (SCHEDULE = ON REFRESH)
contains:AUTO SCALING STRATEGY cannot be combined with a SCHEDULE other than MANUAL

> DROP CLUSTER cc_burst_alt

# The invariants also fire when an ALTER *introduces* a strategy that conflicts with
# the cluster's current config.
> CREATE CLUSTER cc_burst_set (SIZE 'scale=1,workers=1')

! ALTER CLUSTER cc_burst_set SET (AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=1')))
contains:HYDRATION SIZE must differ from the cluster SIZE

> DROP CLUSTER cc_burst_set

> CREATE CLUSTER cc_burst_onref (SIZE 'scale=1,workers=1', SCHEDULE = ON REFRESH)

! ALTER CLUSTER cc_burst_onref SET (AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2')))
contains:AUTO SCALING STRATEGY cannot be combined with a SCHEDULE other than MANUAL

> DROP CLUSTER cc_burst_onref

# With SQL acceptance gated off, the option is rejected at plan time, at CREATE
# and at ALTER SET. RESET stays available so a policy set while the flag was on
# can always be shed, and the stored policy itself survives the rollback.
> CREATE CLUSTER cc_burst_gated (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2')))

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_auto_scaling_strategy = false

! CREATE CLUSTER cc_burst_gated2 (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2')))
contains:`AUTO SCALING STRATEGY` cluster option is not available

! ALTER CLUSTER cc_burst_gated SET (AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=4,workers=4')))
contains:`AUTO SCALING STRATEGY` cluster option is not available

> SELECT create_sql LIKE '%AUTO SCALING STRATEGY%' FROM (SHOW CREATE CLUSTER cc_burst_gated)
true

> ALTER CLUSTER cc_burst_gated RESET (AUTO SCALING STRATEGY)

> SELECT create_sql LIKE '%AUTO SCALING STRATEGY%' FROM (SHOW CREATE CLUSTER cc_burst_gated)
false

> DROP CLUSTER cc_burst_gated

# ----- Hydration burst: no burst on a cluster with nothing to hydrate -----
#
# Burst arming is existential: a burst is warranted iff there exists an object on
# the cluster that no steady replica has hydrated. A strategy-carrying cluster with
# zero objects therefore never bursts, in particular not at creation, before its
# steady replica has even registered. With the burst strategy enabled this is
# deterministic exactly because of the object gate.

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_auto_scaling_strategy = true
ALTER SYSTEM SET enable_hydration_burst = true

> CREATE CLUSTER cc_burst_empty (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2', LINGER DURATION = '600s')))

# Liveness synchronizer: a reconfiguration elsewhere proves the controller ran many
# ticks with the burst strategy enabled. Every tick was a chance to arm a burst on
# cc_burst_empty. It never does, because the cluster has no object to hydrate. (A
# burst that did arm would stick around for its 600s linger and fail the checks below.)
> CREATE CLUSTER cc_burst_tick (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)
> ALTER CLUSTER cc_burst_tick SET (REPLICATION FACTOR 2)
> SELECT count(*) FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'cc_burst_tick'
2
> DROP CLUSTER cc_burst_tick

# No burst is in flight: the auto-scaling state is empty ...
> SELECT mz_clusters.name, scaling.state IS NULL FROM mz_internal.mz_cluster_auto_scaling_strategies scaling JOIN mz_clusters ON mz_clusters.id = scaling.cluster_id WHERE mz_clusters.name = 'cc_burst_empty'
cc_burst_empty true

# ... SHOW CLUSTERS reports no activity ...
> SELECT name, activity FROM (SHOW CLUSTERS) WHERE name = 'cc_burst_empty'
cc_burst_empty <null>

# ... the replica set is exactly the steady baseline ...
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'cc_burst_empty'
cc_burst_empty r1 scale=1,workers=1

# ... and the audit log records neither a burst lifecycle event nor a
# burst-attributed replica.
> SELECT count(*) FROM mz_catalog.mz_audit_events WHERE (details->>'cluster_name' = 'cc_burst_empty' AND details->>'burst_size' IS NOT NULL) OR (object_type = 'cluster-replica' AND details->>'cluster_name' = 'cc_burst_empty' AND details->>'reason' = 'hydration-burst')
0

> DROP CLUSTER cc_burst_empty

# ----- Hydration burst: a burst end-to-end -----
#
# A materialized view that sleeps for an hour per input row keeps the steady
# replica from ever hydrating (the same sleep-in-dataflow pattern as the
# deadline sections above), so the burst arms deterministically and stays up.
# The controller writes the burst record, runs a burst replica at the
# HYDRATION SIZE, and surfaces the burst in the catalog relation, SHOW
# CLUSTERS, and the audit log. RESET then tears it down.

# The sleeping view references `mz_unsafe`, an unstable schema, which durable
# objects may only depend on under this flag.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unstable_dependencies = true

> CREATE CLUSTER cc_burst_e2e (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2', LINGER DURATION = '600s')))

> CREATE TABLE cc_burst_e2e_t (id int)

> INSERT INTO cc_burst_e2e_t VALUES (1)

> CREATE MATERIALIZED VIEW cc_burst_e2e_slow IN CLUSTER cc_burst_e2e AS
  SELECT mz_unsafe.mz_sleep(id * 3600) AS s FROM cc_burst_e2e_t

# The burst replica appears at the hydration size beside the steady replica.
> SELECT r.size, count(*) FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'cc_burst_e2e' GROUP BY r.size ORDER BY r.size
scale=1,workers=1 1
scale=2,workers=2 1

# The in-flight burst is surfaced in the catalog relation ...
> SELECT scaling.state->'burst'->>'burst_size' FROM mz_internal.mz_cluster_auto_scaling_strategies scaling JOIN mz_clusters ON mz_clusters.id = scaling.cluster_id WHERE mz_clusters.name = 'cc_burst_e2e'
scale=2,workers=2

# ... and in SHOW CLUSTERS.
> SELECT name, activity = 'hydration burst at scale=2,workers=2' FROM (SHOW CLUSTERS) WHERE name = 'cc_burst_e2e'
cc_burst_e2e true

# The lifecycle start was audited (this also pins the serialization the
# negative checks in the section above rely on) ...
> SELECT details->>'transition', details->>'burst_size' FROM mz_catalog.mz_audit_events WHERE object_type = 'cluster' AND details->>'cluster_name' = 'cc_burst_e2e' AND details->>'burst_size' IS NOT NULL
started scale=2,workers=2

# ... and the replica create is attributed to the burst.
> SELECT count(*) FROM mz_catalog.mz_audit_events WHERE object_type = 'cluster-replica' AND details->>'cluster_name' = 'cc_burst_e2e' AND details->>'reason' = 'hydration-burst'
1

# Converting to unmanaged mid-burst is refused: it would drop the burst record
# (and its audit trail) and strand the billed burst replica.
! ALTER CLUSTER cc_burst_e2e SET (MANAGED = false)
contains:cannot convert cluster to unmanaged while a hydration burst is in progress

# RESET tears the burst down: the record clears with a finished event, the
# burst replica is retired, and the relation row disappears with the policy.
> ALTER CLUSTER cc_burst_e2e RESET (AUTO SCALING STRATEGY)

> SELECT count(*) FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'cc_burst_e2e' AND r.size = 'scale=2,workers=2'
0

> SELECT count(*) FROM mz_internal.mz_cluster_auto_scaling_strategies scaling JOIN mz_clusters ON mz_clusters.id = scaling.cluster_id WHERE mz_clusters.name = 'cc_burst_e2e'
0

> SELECT details->>'finish_cause' FROM mz_catalog.mz_audit_events WHERE object_type = 'cluster' AND details->>'cluster_name' = 'cc_burst_e2e' AND details->>'transition' = 'finished'
no-longer-warranted

> DROP CLUSTER cc_burst_e2e CASCADE

> DROP TABLE cc_burst_e2e_t

# ----- Hydration burst: a single-replica source runs on the burst -----
#
# Keep both replicas unhydrated with a slow compute dataflow, then force a
# reshape. The burst replica is allocated before the replacement steady
# replica, so when the old steady replica is retired the singleton source moves
# to the burst replica, the lowest remaining ReplicaId. Once the slow dataflow
# is removed, the steady replica must count as hydrated even though the source
# is scheduled elsewhere. The burst can then retire and the source moves to the
# remaining steady replica.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP PUBLICATION IF EXISTS cc_burst_source;
DROP TABLE IF EXISTS public.cc_burst_source_t;
CREATE TABLE public.cc_burst_source_t (f1 INTEGER PRIMARY KEY);
ALTER TABLE public.cc_burst_source_t REPLICA IDENTITY FULL;
INSERT INTO public.cc_burst_source_t VALUES (1), (2), (3);
CREATE PUBLICATION cc_burst_source FOR TABLE public.cc_burst_source_t;

> CREATE CLUSTER cc_burst_source (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=2,workers=2', LINGER DURATION = '0s')))

> CREATE TABLE cc_burst_gate_t (id int)

> INSERT INTO cc_burst_gate_t VALUES (1)

> CREATE MATERIALIZED VIEW cc_burst_gate_mv IN CLUSTER cc_burst_source AS
  SELECT mz_unsafe.mz_sleep(id * 3600) AS s FROM cc_burst_gate_t

# The slow dataflow arms the burst and keeps it alive before the source is
# created, ensuring the source initially chooses the older steady replica.
> SELECT r.size, count(*)
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id
  WHERE c.name = 'cc_burst_source'
  GROUP BY r.size
  ORDER BY r.size
scale=1,workers=1 1
scale=2,workers=2 1

> CREATE SECRET cc_burst_pgpass AS 'postgres'

> CREATE CONNECTION cc_burst_pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET cc_burst_pgpass
  )

> CREATE SOURCE cc_burst_pg_source
  IN CLUSTER cc_burst_source
  FROM POSTGRES CONNECTION cc_burst_pg (PUBLICATION 'cc_burst_source')

> CREATE TABLE cc_burst_pg_table
  FROM SOURCE cc_burst_pg_source (REFERENCE cc_burst_source_t)

> SELECT * FROM cc_burst_pg_table
1
2
3

# Force cut-over because the slow dataflow cannot hydrate on the target. The
# source loses its old steady replica and is deterministically rescheduled onto
# the older burst replica.
> ALTER CLUSTER cc_burst_source SET (SIZE 'scale=4,workers=4') WITH (WAIT UNTIL READY (TIMEOUT '1s', ON TIMEOUT 'COMMIT'))

> SELECT r.size, count(*)
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id
  WHERE c.name = 'cc_burst_source'
  GROUP BY r.size
  ORDER BY r.size
scale=2,workers=2 1
scale=4,workers=4 1

> SELECT r.size
  FROM mz_internal.mz_source_statistics stats
  JOIN mz_sources s ON s.id = stats.id
  JOIN mz_cluster_replicas r ON r.id = stats.replica_id
  WHERE s.name = 'cc_burst_pg_source'
scale=2,workers=2

# Dropping a sleeping dataflow does not interrupt mz_sleep, so replace the
# blocked steady replica once more. The fresh steady replica never installs the
# dropped MV, while the source stays on the older burst replica.
> DROP MATERIALIZED VIEW cc_burst_gate_mv

# Ensure the controller has processed the drop before it creates the replacement
# replica, which must not replay the dropped sleeping dataflow.
> CREATE CLUSTER cc_burst_drop_barrier (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> ALTER CLUSTER cc_burst_drop_barrier SET (REPLICATION FACTOR 2)

> SELECT count(*)
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id
  WHERE c.name = 'cc_burst_drop_barrier'
2

> DROP CLUSTER cc_burst_drop_barrier

> ALTER CLUSTER cc_burst_source SET (SIZE 'scale=1,workers=2') WITH (WAIT UNTIL READY (TIMEOUT '1s', ON TIMEOUT 'COMMIT'))

# The fresh steady replica is hydrated even though the source is running
# off-target on the burst. The zero-duration linger then retires the burst.
> SELECT r.size
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id
  WHERE c.name = 'cc_burst_source'
scale=1,workers=2

# Dropping the burst reschedules the source onto the remaining steady replica.
> SELECT r.size
  FROM mz_internal.mz_source_statistics stats
  JOIN mz_sources s ON s.id = stats.id
  JOIN mz_cluster_replicas r ON r.id = stats.replica_id
  WHERE s.name = 'cc_burst_pg_source'
scale=1,workers=2

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO public.cc_burst_source_t VALUES (4);

> SELECT * FROM cc_burst_pg_table
1
2
3
4

> DROP SOURCE cc_burst_pg_source CASCADE
> DROP CLUSTER cc_burst_source
> DROP TABLE cc_burst_gate_t
> DROP CONNECTION cc_burst_pg
> DROP SECRET cc_burst_pgpass

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION cc_burst_source;
DROP TABLE public.cc_burst_source_t;

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unstable_dependencies = false

# Restore pristine server state (including the tick-interval override).
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM RESET enable_cluster_controller
ALTER SYSTEM RESET cluster_controller_tick_interval
ALTER SYSTEM RESET enable_zero_downtime_cluster_reconfiguration
ALTER SYSTEM RESET enable_background_alter_cluster
ALTER SYSTEM RESET enable_auto_scaling_strategy
ALTER SYSTEM RESET enable_hydration_burst
