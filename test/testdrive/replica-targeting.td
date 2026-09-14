# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for replica-targeted queries (SELECT and SUBSCRIBE).
#
# These tests rely on testdrive's retry feature, as they query introspection
# sources whose data might not be immediately available.

$ set-sql-timeout duration=60s
$ set-regex match=\d{13} replacement=<TIMESTAMP>

# Test that replica-targeted queries return results from the target replica.
#
# This test works by querying introspection sources, which are the only compute
# collections that can have different contents between replicas. Specifically,
# we look at the distinct `worker_id`s present in `mz_compute_frontiers_per_worker`
# for replicas with different worker counts. We know that
# `mz_compute_frontiers_per_worker` always contains entries, because it tracks the
# frontiers of introspection sources as well.

> CREATE CLUSTER test REPLICAS (
      r1 (SIZE 'scale=1,workers=1'),
      r2 (SIZE 'scale=1,workers=2'),
      r4 (SIZE 'scale=1,workers=4')
  )

> SET cluster = test

> SET cluster_replica = r1

> SELECT DISTINCT worker_id FROM mz_introspection.mz_compute_frontiers_per_worker ORDER BY worker_id
0

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (
    SELECT count(DISTINCT worker_id) FROM mz_introspection.mz_compute_frontiers_per_worker
  )
> FETCH c
<TIMESTAMP> 1 1
> COMMIT

> SET cluster_replica = r2

> SELECT DISTINCT worker_id FROM mz_introspection.mz_compute_frontiers_per_worker ORDER BY worker_id
0
1

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (
    SELECT count(DISTINCT worker_id) FROM mz_introspection.mz_compute_frontiers_per_worker
  )
> FETCH c
<TIMESTAMP> 1 2
> COMMIT

> SET cluster_replica = r4

> SELECT DISTINCT worker_id FROM mz_introspection.mz_compute_frontiers_per_worker ORDER BY worker_id
0
1
2
3

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (
    SELECT count(DISTINCT worker_id) FROM mz_introspection.mz_compute_frontiers_per_worker
  )
> FETCH c
<TIMESTAMP> 1 4
> COMMIT

# Test that replica-targeted subscribes work when the subscribed collection
# advances to the empty frontier. Regression test for database-issues#7457.

> DROP CLUSTER test CASCADE
> CREATE CLUSTER test SIZE 'scale=4,workers=4', REPLICATION FACTOR 4
> SET cluster_replica = r1

> CREATE MATERIALIZED VIEW mv AS SELECT 1

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv
> FETCH c
<TIMESTAMP> 1 1
> COMMIT

# We want to provoke the case where a non-targeted replica returns a response
# first, so try multiple times to be sure.

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv
> FETCH c
<TIMESTAMP> 1 1
> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv
> FETCH c
<TIMESTAMP> 1 1
> COMMIT

# Test that replica-targeted peeks are only sent to the target replica.
#
# `mz_peek_durations_histogram_raw` records every peek a replica has completed.
# Replica r2 never receives a query until the final check, which is a fast-path
# peek against the raw log itself and is therefore not yet recorded when it is
# served. Any peek issued on r1 that also reached r2 would show up here.

> DROP CLUSTER test CASCADE
> CREATE CLUSTER test REPLICAS (
      r1 (SIZE 'scale=1,workers=1'),
      r2 (SIZE 'scale=1,workers=1')
  )
> SET cluster = test
> SET cluster_replica = r1

> SELECT count(*) >= 0 FROM mz_introspection.mz_compute_frontiers_per_worker
true
> SELECT count(*) >= 0 FROM mz_introspection.mz_compute_frontiers_per_worker
true
> SELECT count(*) >= 0 FROM mz_introspection.mz_compute_frontiers_per_worker
true

# Slow-path query: its join needs a dataflow with a transient index, unlike the
# fast-path queries above. That index is replica-targeted at r1, the same
# replica as the peek that reads it. A broadcast peek against an index that
# exists only on r1 would panic r2 in `handle_peek` on the missing trace. That
# panic aborts this test's `materialized` container, because the harness runs
# with `propagate_crashes`. The regression protection remains the
# `mz_peek_durations_histogram_raw` check on r2 at the end of this block: it
# only reads 0 because r2 never received this peek.
> SELECT count(*) >= 0
  FROM mz_introspection.mz_compute_frontiers_per_worker f
  JOIN mz_introspection.mz_compute_exports_per_worker e USING (export_id, worker_id)
true

$ set-from-sql var=ts
SELECT mz_now()::text

# With `cluster_replica = r1` set, this query is replica-targeted at r1: it
# never reaches r2, so it cannot add to the peek count checked below (unlike a
# query against a raw log source that let auto-routing pick a replica).
# Waiting for r2's own reported frontier to advance past `ts` (captured right
# after the r1 queries above, including the slow-path query) proves r2's
# replica has communicated with the controller at least once since those
# queries ran, without guessing at a wall-clock duration. This assumes the
# controller's frontier reports and a replica's own introspection-log
# publication are not arbitrarily far apart in time. That has held in
# practice, but if r2 is ever observed to still see stale data at this point,
# this barrier is why.
> SELECT bool_and(f.write_frontier > ${ts})
  FROM mz_cluster_replicas r, mz_cluster_replica_frontiers f
  WHERE r.id = f.replica_id AND r.name = 'r2'
    AND r.cluster_id = (SELECT id FROM mz_clusters WHERE name = 'test')
true

> SET cluster_replica = r2

> SELECT count(*) FROM mz_introspection.mz_peek_durations_histogram_raw
0
