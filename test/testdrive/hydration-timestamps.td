# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the hydration lifecycle timestamps reported by
# `mz_introspection.mz_compute_hydration_timestamps_per_worker`.
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
  FROM mz_introspection.mz_compute_hydration_timestamps_per_worker
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
  FROM mz_introspection.mz_compute_hydration_timestamps_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx'
true true true true

# The timestamps are wallclock instants, not offsets from some arbitrary
# origin, so they must land in the recent past.

> SELECT DISTINCT h.installed_at BETWEEN now() - '1 hour'::interval AND now()
  FROM mz_introspection.mz_compute_hydration_timestamps_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx'
true

# `mz_compute_hydration_times_per_worker` is an exact projection of the new
# relation, so the two must never disagree.

> SELECT count(*)
  FROM mz_introspection.mz_compute_hydration_times_per_worker t
  FULL OUTER JOIN mz_introspection.mz_compute_hydration_timestamps_per_worker h
      USING (export_id, worker_id)
  WHERE t.export_id IS NULL
     OR h.export_id IS NULL
     OR t.time_ns IS DISTINCT FROM h.time_ns
0

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
  FROM mz_introspection.mz_compute_hydration_timestamps_per_worker h
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
  FROM mz_introspection.mz_compute_hydration_timestamps_per_worker h
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
  FROM mz_introspection.mz_compute_hydration_timestamps_per_worker h
  JOIN mz_materialized_views mv ON (mv.id = h.export_id)
  WHERE mv.name = 'mv_stuck'
true true

> CREATE INDEX idx_queued IN CLUSTER queued ON t (a)

> SELECT DISTINCT
      h.installed_at IS NOT NULL,
      h.started_at IS NULL,
      h.hydrated_at IS NULL
  FROM mz_introspection.mz_compute_hydration_timestamps_per_worker h
  JOIN mz_indexes i ON (i.id = h.export_id)
  WHERE i.name = 'idx_queued'
true true true

> DROP INDEX idx_queued
> DROP MATERIALIZED VIEW mv_stuck
> SET cluster = test
> DROP CLUSTER queued

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET compute_hydration_concurrency;

# The ordering invariant `installed_at <= started_at <= hydrated_at` must hold
# for every row, and no row may report a later stage without its predecessors.
# Import-free dataflows are the interesting case: their operators are not gated
# on the suspension token, so they can hydrate before their `Schedule` arrives.
#
# Such dataflows advance to the empty frontier and have their replica state
# cleaned up promptly, so we cannot reliably observe an individual one. Assert
# the invariant repeatedly instead, while they are being created and torn down.
# Retries are disabled from here on, so that a violation cannot be retried away.

$ set-max-tries max-tries=1

> CREATE MATERIALIZED VIEW mv_const1 IN CLUSTER test AS SELECT 1
> SELECT count(*) FROM mz_introspection.mz_compute_hydration_timestamps_per_worker
  WHERE installed_at IS NULL
     OR (started_at IS NULL AND hydrated_at IS NOT NULL)
     OR started_at < installed_at
     OR hydrated_at < started_at
0

> CREATE MATERIALIZED VIEW mv_const2 IN CLUSTER test AS SELECT 2
> SELECT count(*) FROM mz_introspection.mz_compute_hydration_timestamps_per_worker
  WHERE installed_at IS NULL
     OR (started_at IS NULL AND hydrated_at IS NOT NULL)
     OR started_at < installed_at
     OR hydrated_at < started_at
0

> CREATE MATERIALIZED VIEW mv_const3 IN CLUSTER test AS SELECT 3
> SELECT count(*) FROM mz_introspection.mz_compute_hydration_timestamps_per_worker
  WHERE installed_at IS NULL
     OR (started_at IS NULL AND hydrated_at IS NOT NULL)
     OR started_at < installed_at
     OR hydrated_at < started_at
0
