# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the hydration lifecycle across the two relations that report it.
#
# `mz_introspection.mz_compute_hydration_times_per_worker` carries `time_ns` and
# `hydrated_at`, the durability reading: the instant the reported output frontier
# passed the as-of. `mz_introspection.mz_compute_lifecycle_events_per_worker`
# carries the stages, including the `installed` and `started` instants that this
# relation used to duplicate. The interesting assertions are therefore the ones
# that join the two, since a reader gets the queueing interval from one and the
# duration from the other.
#
# These tests rely on testdrive's retry feature, as dataflows take an unknown
# (but hopefully small) time to be installed and to hydrate. The section that
# must not retry, so that a transient invariant violation is not retried away,
# comes last, since `set-max-tries` has no way to restore the default.

$ set-sql-timeout duration=60s

> CREATE CLUSTER test SIZE 'scale=1,workers=2'
> SET cluster = test

# Log collections are never suspended and never receive a `Schedule` command, so
# they must still reach every stage and report a duration.

> SELECT DISTINCT
      max(l.occurred_at) FILTER (WHERE l.event = 'installed') IS NOT NULL,
      max(l.occurred_at) FILTER (WHERE l.event = 'started') IS NOT NULL,
      max(l.occurred_at) FILTER (WHERE l.event = 'hydrated') IS NOT NULL
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  WHERE l.export_id LIKE 's%'
  GROUP BY l.export_id, l.worker_id
true true true

> SELECT DISTINCT hydrated_at IS NOT NULL, time_ns IS NOT NULL
  FROM mz_introspection.mz_compute_hydration_times_per_worker
  WHERE export_id LIKE 's%'
true true

# A hydrated object reaches all three stages, ordered, and reports a duration.

> CREATE TABLE t (a int)
> CREATE INDEX idx IN CLUSTER test ON t (a)

> SELECT DISTINCT
      max(l.occurred_at) FILTER (WHERE l.event = 'started')
          >= max(l.occurred_at) FILTER (WHERE l.event = 'installed'),
      max(l.occurred_at) FILTER (WHERE l.event = 'hydrated')
          >= max(l.occurred_at) FILTER (WHERE l.event = 'started')
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_indexes i ON (i.id = l.export_id)
  WHERE i.name = 'idx'
  GROUP BY l.export_id, l.worker_id
true true

> SELECT DISTINCT h.hydrated_at IS NOT NULL, h.time_ns IS NOT NULL
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx'
true true

# `hydrated_at` is a wallclock instant, not an offset from some arbitrary origin,
# so it must land in the recent past.

> SELECT DISTINCT h.hydrated_at BETWEEN now() - '1 hour'::interval AND now()
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx'
true

# `time_ns` and the two relations' instants measure the same interval by
# different means: `time_ns` from an `Instant` taken when the export state is
# created, the instants from logging event times rounded to microseconds. They
# must agree, which is what makes the duration in one relation and the stages in
# the other tell one story. Both relations are per worker, so the join carries
# `worker_id`; joining on `export_id` alone would multiply each row by the number
# of workers. The tolerance is generous because the point is to catch a wrong
# interval or a units error, not to pin down the rounding.

> SELECT DISTINCT
      abs(extract(epoch FROM (h.hydrated_at - l.installed_at)) - h.time_ns / 1e9) < 1
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN (
      SELECT
          export_id,
          worker_id,
          max(occurred_at) FILTER (WHERE event = 'installed') AS installed_at
      FROM mz_introspection.mz_compute_lifecycle_events_per_worker
      GROUP BY export_id, worker_id
  ) l ON (l.export_id = h.export_id AND l.worker_id = h.worker_id)
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx'
true

# An object whose inputs are unavailable is never scheduled, so it reports
# `installed` but no `started`. A cluster with no replicas never advances the
# frontiers of the collections it maintains, which is what makes the input
# unavailable here.

> CREATE CLUSTER source_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 0
> CREATE MATERIALIZED VIEW mv_unavailable IN CLUSTER source_cluster AS SELECT * FROM t

> CREATE INDEX idx_waiting IN CLUSTER test ON mv_unavailable (a)

> SELECT DISTINCT
      count(*) FILTER (WHERE l.event = 'installed') = 1,
      count(*) FILTER (WHERE l.event = 'started') = 0,
      count(*) FILTER (WHERE l.event = 'hydrated') = 0
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_indexes i ON (i.id = l.export_id)
  WHERE i.name = 'idx_waiting'
  GROUP BY l.export_id, l.worker_id
true true true

> SELECT DISTINCT h.hydrated_at IS NULL, h.time_ns IS NULL
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx_waiting'
true true

# Once the input becomes available the object is scheduled and hydrates, and the
# queueing interval it spent waiting is visible as `started - installed`.

> ALTER CLUSTER source_cluster SET (REPLICATION FACTOR 1)

> SELECT DISTINCT
      max(l.occurred_at) FILTER (WHERE l.event = 'started')
          > max(l.occurred_at) FILTER (WHERE l.event = 'installed'),
      max(l.occurred_at) FILTER (WHERE l.event = 'hydrated')
          >= max(l.occurred_at) FILTER (WHERE l.event = 'started')
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_indexes i ON (i.id = l.export_id)
  WHERE i.name = 'idx_waiting'
  GROUP BY l.export_id, l.worker_id
true true

> DROP INDEX idx_waiting
> DROP MATERIALIZED VIEW mv_unavailable
> DROP CLUSTER source_cluster

# An object queued behind `compute_hydration_concurrency` reports `installed` but
# no `started`, exactly as one waiting on its inputs does. `mv_stuck` oscillates
# between one row and no rows, so it never converges and holds the single
# hydration slot indefinitely, in constant memory.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_hydration_concurrency = 1;

> CREATE CLUSTER queued SIZE 'scale=1,workers=1'
> SET cluster = queued

> CREATE MATERIALIZED VIEW mv_stuck IN CLUSTER queued AS
  WITH MUTUALLY RECURSIVE
      x (a int) AS (
          SELECT 1
          EXCEPT ALL
          SELECT a FROM x
      )
  SELECT * FROM x

# An in-flight object is distinguishable from one that never started: it has
# `started` but no `hydrated`.
> SELECT DISTINCT
      count(*) FILTER (WHERE l.event = 'started') = 1,
      count(*) FILTER (WHERE l.event = 'hydrated') = 0
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_materialized_views mv ON (mv.id = l.export_id)
  WHERE mv.name = 'mv_stuck'
  GROUP BY l.export_id, l.worker_id
true true

> CREATE INDEX idx_queued IN CLUSTER queued ON t (a)

> SELECT DISTINCT
      count(*) FILTER (WHERE l.event = 'installed') = 1,
      count(*) FILTER (WHERE l.event = 'started') = 0,
      count(*) FILTER (WHERE l.event = 'hydrated') = 0
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_indexes i ON (i.id = l.export_id)
  WHERE i.name = 'idx_queued'
  GROUP BY l.export_id, l.worker_id
true true true

> DROP INDEX idx_queued
> DROP MATERIALIZED VIEW mv_stuck
> SET cluster = test
> DROP CLUSTER queued

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET compute_hydration_concurrency;

# An import-free dataflow has nothing to suspend, so it reports `started` from
# creation rather than acquiring it when its `Schedule` lands. That keeps the log
# truthful while it hydrates: it reads as hydrating rather than as queued behind
# something that is not queueing it.
#
# An index is used rather than a materialized view because a constant
# materialized view runs to the empty frontier and its dataflow shuts down,
# taking the rows with it. The two stamps land in the same turn but are separate
# log events, so they are microseconds apart rather than equal.

> CREATE VIEW v_const AS SELECT 1 AS a
> CREATE INDEX idx_const IN CLUSTER test ON v_const (a)

> SELECT DISTINCT
      max(l.occurred_at) FILTER (WHERE l.event = 'started')
          >= max(l.occurred_at) FILTER (WHERE l.event = 'installed'),
      max(l.occurred_at) FILTER (WHERE l.event = 'started')
          - max(l.occurred_at) FILTER (WHERE l.event = 'installed')
          < '1 second'::interval,
      max(l.occurred_at) FILTER (WHERE l.event = 'hydrated')
          >= max(l.occurred_at) FILTER (WHERE l.event = 'started')
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_indexes i ON (i.id = l.export_id)
  WHERE i.name = 'idx_const'
  GROUP BY l.export_id, l.worker_id
true true true

> DROP INDEX idx_const
> DROP VIEW v_const

# The two relations must not contradict each other. A row reporting a duration
# must have a `hydrated` stage behind it, and its `hydrated_at` must not precede
# the export's `installed`. Assert it repeatedly while import-free dataflows are
# created and torn down, since those are the paths that stamp outside
# `handle_schedule`. Retries are disabled from here on, so that a violation
# cannot be retried away.

$ set-max-tries max-tries=1

> CREATE MATERIALIZED VIEW mv_const1 IN CLUSTER test AS SELECT 1
> SELECT count(*)
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN (
      SELECT
          export_id,
          worker_id,
          max(occurred_at) FILTER (WHERE event = 'installed') AS installed_at,
          count(*) FILTER (WHERE event = 'hydrated') AS hydrated_events
      FROM mz_introspection.mz_compute_lifecycle_events_per_worker
      GROUP BY export_id, worker_id
  ) l ON (l.export_id = h.export_id AND l.worker_id = h.worker_id)
  WHERE (h.time_ns IS NOT NULL AND h.hydrated_at IS NULL)
     OR (h.hydrated_at IS NOT NULL AND l.hydrated_events = 0)
     OR h.hydrated_at < l.installed_at
     OR l.installed_at IS NULL
0

> CREATE MATERIALIZED VIEW mv_const2 IN CLUSTER test AS SELECT 2
> SELECT count(*)
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN (
      SELECT
          export_id,
          worker_id,
          max(occurred_at) FILTER (WHERE event = 'installed') AS installed_at,
          count(*) FILTER (WHERE event = 'hydrated') AS hydrated_events
      FROM mz_introspection.mz_compute_lifecycle_events_per_worker
      GROUP BY export_id, worker_id
  ) l ON (l.export_id = h.export_id AND l.worker_id = h.worker_id)
  WHERE (h.time_ns IS NOT NULL AND h.hydrated_at IS NULL)
     OR (h.hydrated_at IS NOT NULL AND l.hydrated_events = 0)
     OR h.hydrated_at < l.installed_at
     OR l.installed_at IS NULL
0
