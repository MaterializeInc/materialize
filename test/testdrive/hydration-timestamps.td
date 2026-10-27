# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the hydration lifecycle timestamps reported by
# `mz_introspection.mz_compute_hydration_times_per_worker`, alongside the
# `time_ns` duration the relation has always carried.
#
# These tests rely on testdrive's retry feature, as dataflows take an unknown
# (but hopefully small) time to be installed and to hydrate. The one section
# that must not retry, so that a transient invariant violation is not retried
# away, comes last, since `set-max-tries` has no way to restore the default.

$ set-sql-timeout duration=60s

> CREATE CLUSTER test SIZE 'scale=1,workers=2'
> SET cluster = test

# Log collections are never suspended and never receive a `Schedule` command,
# so they must still report a complete set of timestamps.

> SELECT DISTINCT
      installed_at IS NOT NULL,
      started_at IS NOT NULL,
      hydrated_at IS NOT NULL
  FROM mz_introspection.mz_compute_hydration_times_per_worker
  WHERE export_id LIKE 's%'
true true true

# A hydrated object reports all three timestamps, ordered.

> CREATE TABLE t (a int)
> CREATE INDEX idx IN CLUSTER test ON t (a)

> SELECT DISTINCT
      h.installed_at IS NOT NULL,
      h.started_at >= h.installed_at,
      h.hydrated_at >= h.started_at,
      h.time_ns IS NOT NULL
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx'
true true true true

# The timestamps are wallclock instants, not offsets from some arbitrary
# origin, so they must land in the recent past.

> SELECT DISTINCT h.installed_at BETWEEN now() - '1 hour'::interval AND now()
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx'
true

# `time_ns` and the timestamps measure the same interval by different means:
# `time_ns` from an `Instant` taken when the export state is created, the
# timestamps from logging event times rounded to microseconds. They must agree,
# which is what makes the retained `time_ns` column and the new columns tell one
# consistent story. The tolerance is generous because the point is to catch a
# wrong interval or a units error, not to pin down the rounding.

> SELECT DISTINCT
      abs(extract(epoch FROM (h.hydrated_at - h.installed_at)) - h.time_ns / 1e9) < 1
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx'
true

# An object whose inputs are unavailable is never scheduled, so it reports an
# `installed_at` but no `started_at`. A cluster with no replicas never advances
# the frontiers of the collections it maintains, which is what makes the input
# unavailable here.

> CREATE CLUSTER source_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 0
> CREATE MATERIALIZED VIEW mv_unavailable IN CLUSTER source_cluster AS SELECT * FROM t

> CREATE INDEX idx_waiting IN CLUSTER test ON mv_unavailable (a)

> SELECT DISTINCT
      h.installed_at IS NOT NULL,
      h.started_at IS NULL,
      h.hydrated_at IS NULL,
      h.time_ns IS NULL
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx_waiting'
true true true true

# Once the input becomes available the object is scheduled and hydrates, and
# the queueing interval it spent waiting is visible as
# `started_at - installed_at`.

> ALTER CLUSTER source_cluster SET (REPLICATION FACTOR 1)

> SELECT DISTINCT
      h.started_at > h.installed_at,
      h.hydrated_at >= h.started_at
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx_waiting'
true true

> DROP INDEX idx_waiting
> DROP MATERIALIZED VIEW mv_unavailable
> DROP CLUSTER source_cluster

# An object queued behind `compute_hydration_concurrency` reports an
# `installed_at` but no `started_at`, exactly as one waiting on its inputs
# does. `mv_stuck` oscillates between one row and no rows, so it never
# converges and holds the single hydration slot indefinitely, in constant
# memory.

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

# An in-flight object is distinguishable from one that never started: it has a
# `started_at` but no `hydrated_at`.
> SELECT DISTINCT h.started_at IS NOT NULL, h.hydrated_at IS NULL
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_materialized_views mv ON (mv.id = h.export_id)
  WHERE mv.name = 'mv_stuck'
true true

> CREATE INDEX idx_queued IN CLUSTER queued ON t (a)

> SELECT DISTINCT
      h.installed_at IS NOT NULL,
      h.started_at IS NULL,
      h.hydrated_at IS NULL
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx_queued'
true true true

> DROP INDEX idx_queued
> DROP MATERIALIZED VIEW mv_stuck
> SET cluster = test
> DROP CLUSTER queued

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET compute_hydration_concurrency;

# An import-free dataflow has nothing to suspend, so it carries `started_at`
# from creation rather than acquiring it when its `Schedule` lands. That keeps the
# row truthful while it hydrates: it reads as hydrating rather than as queued
# behind something that is not queueing it.
#
# An index is used rather than a materialized view because a constant
# materialized view runs to the empty frontier and its dataflow shuts down,
# taking the row with it. The two stamps land in the same turn but are separate
# log events, so they are microseconds apart rather than equal.

> CREATE VIEW v_const AS SELECT 1 AS a
> CREATE INDEX idx_const IN CLUSTER test ON v_const (a)

> SELECT DISTINCT
      h.installed_at IS NOT NULL,
      h.started_at >= h.installed_at,
      h.started_at - h.installed_at < '1 second'::interval,
      h.hydrated_at >= h.started_at
  FROM mz_introspection.mz_compute_hydration_times_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx_const'
true true true true

> DROP INDEX idx_const
> DROP VIEW v_const

# The ordering invariant `installed_at <= started_at <= hydrated_at` must hold
# for every row, and no row may report a later stage without its predecessors.
# Assert it repeatedly while import-free dataflows are created and torn down,
# since those are the paths that stamp outside `handle_schedule`. Retries are
# disabled from here on, so that a violation cannot be retried away.

$ set-max-tries max-tries=1

> CREATE MATERIALIZED VIEW mv_const1 IN CLUSTER test AS SELECT 1
> SELECT count(*) FROM mz_introspection.mz_compute_hydration_times_per_worker
  WHERE installed_at IS NULL
     OR (started_at IS NULL AND hydrated_at IS NOT NULL)
     OR started_at < installed_at
     OR hydrated_at < started_at
0

> CREATE MATERIALIZED VIEW mv_const2 IN CLUSTER test AS SELECT 2
> SELECT count(*) FROM mz_introspection.mz_compute_hydration_times_per_worker
  WHERE installed_at IS NULL
     OR (started_at IS NULL AND hydrated_at IS NOT NULL)
     OR started_at < installed_at
     OR hydrated_at < started_at
0

> CREATE MATERIALIZED VIEW mv_const3 IN CLUSTER test AS SELECT 3
> SELECT count(*) FROM mz_introspection.mz_compute_hydration_times_per_worker
  WHERE installed_at IS NULL
     OR (started_at IS NULL AND hydrated_at IS NOT NULL)
     OR started_at < installed_at
     OR hydrated_at < started_at
0
