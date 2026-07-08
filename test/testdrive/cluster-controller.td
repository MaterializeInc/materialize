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

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unstable_dependencies = false

# ----- Foreground wait-shim (background ALTER off) -----
#
# With enable_background_alter_cluster off, a config-shape ALTER blocks the session
# on the wait-shim instead of returning immediately. The shim keeps no timer of its
# own: it waits for the controller to resolve the durable record, then reports
# success once the realized config reached the requested shape. An empty cluster
# hydrates promptly, so the reshape cuts over and the blocking ALTER returns.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_background_alter_cluster = false
ALTER SYSTEM SET cluster_alter_check_ready_interval = '100ms'

> CREATE CLUSTER cc_fg (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

# Blocks until the controller cuts the realized size over, then returns success.
> ALTER CLUSTER cc_fg SET (SIZE 'scale=1,workers=2')

> SELECT size FROM mz_clusters WHERE name = 'cc_fg'
scale=1,workers=2

> DROP CLUSTER cc_fg

# Restore background mode after this section.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_background_alter_cluster = true
ALTER SYSTEM RESET cluster_alter_check_ready_interval

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
