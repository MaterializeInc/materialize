# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that materialized views created with IN CLUSTER ... REPLICA ... syntax
# are only installed on the target replica, and that lifecycle events (dropping
# replicas) work correctly.
#
# Dropping a replica implicitly drops all materialized views that target it.
#
# Replica-targeted MVs require the feature flag to be enabled.

$ set-sql-timeout duration=60s

# Enable the feature flag.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_replica_targeted_materialized_views = true

> CREATE TABLE t (a int)
> INSERT INTO t VALUES (1), (2), (3)

> CREATE CLUSTER test REPLICAS (
    r1 (SIZE 'scale=1,workers=1'),
    r2 (SIZE 'scale=1,workers=1')
  )
> SET cluster = test

# Create a regular (untargeted) materialized view for comparison.
> CREATE MATERIALIZED VIEW mv_all AS SELECT sum(a) AS s FROM t

# Create a materialized view targeted at r1.
> CREATE MATERIALIZED VIEW mv_r1 IN CLUSTER test REPLICA r1 AS SELECT sum(a) AS s FROM t

# Both materialized views should return correct data.
> SELECT * FROM mv_all
6

> SELECT * FROM mv_r1
6

# ---------------------------------------------------------------------------
# Verify that the targeted MV only has hydration status on the target replica.
# ---------------------------------------------------------------------------

# The untargeted MV should be hydrated on both replicas.
> SELECT r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_materialized_views mv ON (mv.id = h.object_id)
  WHERE mv.name = 'mv_all'
  ORDER BY r.name
r1 true
r2 true

# The targeted MV should only be hydrated on r1.
> SELECT r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_materialized_views mv ON (mv.id = h.object_id)
  WHERE mv.name = 'mv_r1'
  ORDER BY r.name
r1 true

# ---------------------------------------------------------------------------
# Verify per-replica frontiers: the targeted MV should only have a write
# frontier on the target replica.
# ---------------------------------------------------------------------------

# The untargeted MV should have write frontiers on both replicas.
> SELECT r.name
  FROM mz_cluster_replica_frontiers f
  JOIN mz_materialized_views mv ON (f.object_id = mv.id)
  JOIN mz_cluster_replicas r ON (f.replica_id = r.id)
  WHERE mv.name = 'mv_all' AND f.write_frontier > 0
  ORDER BY r.name
r1
r2

# The targeted MV should only have a write frontier on r1.
> SELECT r.name
  FROM mz_cluster_replica_frontiers f
  JOIN mz_materialized_views mv ON (f.object_id = mv.id)
  JOIN mz_cluster_replicas r ON (f.replica_id = r.id)
  WHERE mv.name = 'mv_r1' AND f.write_frontier > 0
  ORDER BY r.name
r1

# ---------------------------------------------------------------------------
# Verify that data updates propagate correctly to a targeted MV.
# ---------------------------------------------------------------------------

> INSERT INTO t VALUES (4)

> SELECT * FROM mv_r1
10

# ---------------------------------------------------------------------------
# Test: dropping the non-target replica does not affect the targeted MV.
# ---------------------------------------------------------------------------

> DROP CLUSTER REPLICA test.r2

> SELECT * FROM mv_r1
10

# Hydration status still reports on r1.
> SELECT r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_materialized_views mv ON (mv.id = h.object_id)
  WHERE mv.name = 'mv_r1'
  ORDER BY r.name
r1 true

# Re-create r2 and verify the targeted MV still only runs on r1.
> CREATE CLUSTER REPLICA test.r2 SIZE 'scale=1,workers=1'

> SELECT r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_materialized_views mv ON (mv.id = h.object_id)
  WHERE mv.name = 'mv_r1'
  ORDER BY r.name
r1 true

# The untargeted MV should reappear on the new r2 as well.
> SELECT r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_materialized_views mv ON (mv.id = h.object_id)
  WHERE mv.name = 'mv_all'
  ORDER BY r.name
r1 true
r2 true

# ---------------------------------------------------------------------------
# Test: dropping the target replica implicitly drops the targeted MV.
# ---------------------------------------------------------------------------

> DROP CLUSTER REPLICA test.r1

# The targeted MV should no longer exist.
! SELECT * FROM mv_r1
contains:unknown catalog item 'mv_r1'

# The untargeted MV should still work on the remaining replica.
> SELECT * FROM mv_all
10

# ---------------------------------------------------------------------------
# Test: creating a new targeted MV and then dropping its target replica also
# drops it.
# ---------------------------------------------------------------------------

> CREATE CLUSTER REPLICA test.r1 SIZE 'scale=1,workers=1'

> CREATE MATERIALIZED VIEW mv_r1 IN CLUSTER test REPLICA r1 AS SELECT sum(a) AS s FROM t

> SELECT r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_materialized_views mv ON (mv.id = h.object_id)
  WHERE mv.name = 'mv_r1'
  ORDER BY r.name
r1 true

> SELECT * FROM mv_r1
10

> DROP CLUSTER REPLICA test.r1

# The targeted MV should be gone again.
! SELECT * FROM mv_r1
contains:unknown catalog item 'mv_r1'

# The untargeted MV should be unaffected.
> SELECT * FROM mv_all
10

# The untargeted MV should be on both replicas (r2 still exists, r1 was
# dropped so only r2).
> SELECT r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_materialized_views mv ON (mv.id = h.object_id)
  WHERE mv.name = 'mv_all'
  ORDER BY r.name
r2 true

# ---------------------------------------------------------------------------
# Regression test for CLU-118: resizing a managed cluster that hosts a
# replica-targeted MV with a COMMENT must not leave a dangling comment behind
# (which previously tripped the coordinator consistency check and panicked).
# ---------------------------------------------------------------------------

> CREATE CLUSTER managed (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> CREATE MATERIALIZED VIEW mv_managed IN CLUSTER managed REPLICA r1 AS SELECT sum(a) AS s FROM t

> COMMENT ON MATERIALIZED VIEW mv_managed IS 'pinned to r1'

> SELECT * FROM mv_managed
10

# Resizing tears down and recreates the replica, which implicitly drops the
# targeted MV and must also drop its comment.
> ALTER CLUSTER managed SET (SIZE 'scale=1,workers=2')

# The targeted MV is gone.
! SELECT * FROM mv_managed
contains:unknown catalog item 'mv_managed'

# No dangling comment is left behind. Check mz_comments directly: joining to
# mz_materialized_views would vacuously return 0 once the MV is dropped, even if
# an orphaned comment row survived.
> SELECT count(*) FROM mz_internal.mz_comments WHERE comment = 'pinned to r1'
0

# The cluster is still usable after the resize.
> CREATE MATERIALIZED VIEW mv_after IN CLUSTER managed AS SELECT sum(a) AS s FROM t
> SELECT * FROM mv_after
10

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

> DROP MATERIALIZED VIEW mv_all
> DROP TABLE t CASCADE
> DROP CLUSTER test CASCADE
> DROP CLUSTER managed CASCADE
