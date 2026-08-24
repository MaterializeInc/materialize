# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the lifecycle event log reported by
# `mz_introspection.mz_compute_lifecycle_events_per_worker`.
#
# These tests rely on testdrive's retry feature, as dataflows take an unknown
# (but hopefully small) time to be installed, to hydrate, and to write. The
# sections that must not retry, so that a transient invariant violation is not
# retried away, come last, since `set-max-tries` has no way to restore the
# default.

$ set-sql-timeout duration=60s

> CREATE CLUSTER test SIZE 'scale=1,workers=2'
> SET cluster = test

> CREATE TABLE t (a int)

# An index has no persist sink, so its lifecycle stops at `snapshot_complete`. Every
# worker hydrates its own fragment of the dataflow, so each of the three stages
# is reported once per worker.

> CREATE INDEX idx IN CLUSTER test ON t (a)

> SELECT l.event, count(*)
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_indexes i ON (i.id = l.export_id)
  WHERE i.name = 'idx'
  GROUP BY l.event
installed 2
started 2
snapshot_complete 2

# The stages are ordered within each worker, and `occurred_at` is a wallclock
# instant rather than an offset from some arbitrary origin.

> SELECT DISTINCT
      max(l.occurred_at) FILTER (WHERE l.event = 'started')
          >= max(l.occurred_at) FILTER (WHERE l.event = 'installed'),
      max(l.occurred_at) FILTER (WHERE l.event = 'snapshot_complete')
          >= max(l.occurred_at) FILTER (WHERE l.event = 'started'),
      max(l.occurred_at) BETWEEN now() - '1 hour'::interval AND now()
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_indexes i ON (i.id = l.export_id)
  WHERE i.name = 'idx'
  GROUP BY l.worker_id
true true true

# Every event carries the dataflow's as-of, without which the interval between
# two stages says nothing about how much work was done.

> SELECT DISTINCT l.details->>'as_of' IS NOT NULL
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_indexes i ON (i.id = l.export_id)
  WHERE i.name = 'idx'
true

# Every event carries the id of the dataflow maintaining the export, which has to be
# the dataflow `mz_compute_exports_per_worker` reports for that same export and worker.

> SELECT DISTINCT l.dataflow_id = e.dataflow_id
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_introspection.mz_compute_exports_per_worker e
    ON (e.export_id = l.export_id AND e.worker_id = l.worker_id)
  JOIN mz_indexes i ON (i.id = l.export_id)
  WHERE i.name = 'idx'
true

# A materialized view writes, so it reaches `written`. The write stages are
# observed by the single worker that maintains the sink frontier, so they are
# reported once per object rather than once per worker.

> CREATE MATERIALIZED VIEW mv IN CLUSTER test AS SELECT a + 1 AS a FROM t

> SELECT l.event, count(*)
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_materialized_views mv ON (mv.id = l.export_id)
  WHERE mv.name = 'mv' AND l.event = 'written'
  GROUP BY l.event
written 1

> SELECT l.event, count(*)
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_materialized_views mv ON (mv.id = l.export_id)
  WHERE mv.name = 'mv' AND l.event = 'snapshot_complete'
  GROUP BY l.event
snapshot_complete 2

# `written` never precedes `snapshot_complete`, which is what makes the interval
# between them the time taken to make the computed snapshot durable.
#
# The comparison has to be per worker. `written` is reported only by the worker
# that maintains the sink frontier, and it is ordered after that worker's own
# `snapshot_complete`. Another worker may finish later still, so comparing `written`
# against the maximum `snapshot_complete` across all workers proves nothing.

> SELECT DISTINCT
      max(l.occurred_at) FILTER (WHERE l.event = 'written')
          >= max(l.occurred_at) FILTER (WHERE l.event = 'snapshot_complete')
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  JOIN mz_materialized_views mv ON (mv.id = l.export_id)
  WHERE mv.name = 'mv'
  GROUP BY l.worker_id
  HAVING count(*) FILTER (WHERE l.event = 'written') > 0
true

# `write_blocked` is only reported for an object whose snapshot is complete and is still
# not permitted to write, which is a replica in read-only mode awaiting a
# cutover. Testdrive cannot reach that state, so there is nothing to assert
# positively here. The invariant block at the end of this file requires the
# blocked/unblocked pair to be absent or complete, never that it is present.

# Dropping an object retracts its rows, so the log does not accumulate lifecycles
# of objects that no longer exist. This converges rather than holding at every
# instant: `DROP` returns once the catalog row is gone, while the retraction still
# has to reach the replica, be logged, and travel through the introspection
# subscribe, so the retry is doing real work here and a consumer has to tolerate
# the gap. Transient dataflows are excluded: a peek installs one with a `t`
# prefixed export id that is never in `mz_objects`, so it would read as an orphan
# for as long as it lives.

> DROP MATERIALIZED VIEW mv
> DROP INDEX idx

> SELECT count(*)
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker l
  LEFT JOIN mz_objects o ON (o.id = l.export_id)
  WHERE o.id IS NULL AND l.export_id NOT LIKE 't%'
0

# Invariants that must hold at all times. Retries are disabled from here on, so
# that a violation cannot be retried away.

$ set-max-tries max-tries=1

# `reason` is drawn from a closed vocabulary, and only the events that have a
# cause to report carry one.

> SELECT count(*)
  FROM mz_introspection.mz_compute_lifecycle_events_per_worker
  WHERE event NOT IN (
            'installed', 'started', 'snapshot_complete',
            'write_blocked', 'write_unblocked', 'written'
        )
     OR reason NOT IN ('read_only')
     OR (reason IS NOT NULL AND event <> 'write_blocked')
0

# No worker reports a stage without its predecessors.

> SELECT count(*)
  FROM (
      SELECT
          export_id,
          worker_id,
          array_agg(event) AS events
      FROM mz_introspection.mz_compute_lifecycle_events_per_worker
      GROUP BY export_id, worker_id
  )
  WHERE NOT ('installed' = ANY(events))
     OR ('snapshot_complete' = ANY(events) AND NOT 'started' = ANY(events))
     OR ('write_unblocked' = ANY(events) AND NOT 'write_blocked' = ANY(events))
     OR ('written' = ANY(events) AND NOT 'snapshot_complete' = ANY(events))
0

# A stage is reported at most once per export and worker, which is what lets a
# reader take an event's `occurred_at` without aggregating first.

> SELECT count(*)
  FROM (
      SELECT export_id, worker_id, event, count(*) AS n
      FROM mz_introspection.mz_compute_lifecycle_events_per_worker
      GROUP BY export_id, worker_id, event
  )
  WHERE n > 1
0
