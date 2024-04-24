# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_refresh_every_mvs = true

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_cluster_schedule_refresh = true

> CREATE DATABASE materialized_view_refresh_options;
> SET DATABASE = materialized_view_refresh_options;

> CREATE TABLE t1(x int);

> INSERT INTO t1 VALUES (1);

# This refresh interval needs to be not too small and not too big. See the constraints in comments below.
> CREATE MATERIALIZED VIEW mv1
  WITH (REFRESH EVERY '8sec')
  AS SELECT x+x as x2 FROM t1;

> INSERT INTO t1 VALUES (3);

# The following will not immediately return the recently inserted values, but Testdrive will wait.
# Warning: This test assumes that Testdrive's timeout is greater than the above refresh interval.
> SELECT * FROM mv1;
2
6

> INSERT INTO t1 VALUES (4);

# What we just inserted shouldn't be in the mv yet, because we are just after a refresh (because the previous SELECT
# returned correct results only after a refresh).
# Warning: this test assumes that the above INSERT completes within the above refresh interval. If we have some
# transient infrastructure problem that makes the INSERT really slow, then this test will fail.
> SELECT * FROM mv1;
2
6

> SELECT * FROM mv1;
2
6
8

# Check that I can query it together with other objects, even between refreshes, and that data added later than the last
# refresh in other objects is reflected in the result.
> CREATE MATERIALIZED VIEW mv2
  WITH (REFRESH = EVERY '10000sec')
  AS SELECT x+x as x2 FROM t1;

> CREATE TABLE t2(y int);

> INSERT INTO t2 VALUES (100);

> (SELECT * from mv2) UNION (SELECT * FROM t2);
2
6
8
100

# The following DELETE shouldn't affect mv2, because mv2 has a very large refresh interval.
> DELETE FROM t1;

> (SELECT * from mv2) UNION (SELECT * FROM t2);
2
6
8
100

# Check that there is an implicit refresh immediately at the creation of the MV, even if it's REFRESH EVERY.
> CREATE MATERIALIZED VIEW mv3
  WITH (REFRESH EVERY '10000sec')
  AS SELECT y+y as y2 FROM t2;

> SELECT * FROM mv3;
200

# Check that mz_now() occurring in the original statement works. This tests that after we purify away `mz_now()`, we
# also remove it from `resolved_ids`. Importantly, this has to be a Testdrive test, and not an slt test, because slt
# doesn't do the "the in-memory state of the catalog does not match its on-disk state" check.
#
# Also tests that planning uses `cast_to` with `CastContext::Implicit` (instead of `type_as`) when planning the
# REFRESH AT.
> CREATE MATERIALIZED VIEW mv4
  WITH (REFRESH AT mz_now()::string::int8 + 2000)
  AS SELECT y*y as y2 FROM t2;

> SELECT * FROM mv4;
10000

## Test turning replicas off and on, part 1:
## First, we check the following two things:
## - 1a. Turn the replica off just after a refresh, and then turn the replica back on immediately.
## - 1b. Turn the replica off just after a refresh, and then turn the replica back on only after the next refresh time
##   has passed.

> CREATE CLUSTER refresh_cluster SIZE = '1', REPLICATION FACTOR = 1;
> SET cluster = refresh_cluster;
> CREATE MATERIALIZED VIEW mv5
  WITH (REFRESH EVERY '8 sec' ALIGNED TO mz_now()::text::int8 + 5000)
  AS SELECT 3*y as y2 FROM t2;
> SET cluster = default;

> SELECT * FROM mv5;
300

> INSERT INTO t2 VALUES (102);

# Wait until the insert is reflected, so that we are just after a refresh. This is important, because otherwise the
# below `SET (REPLICATION FACTOR 0)` and the `SELECT` after that might straddle a refresh, in which case the `SELECT`
# would hang forever.
> SELECT * FROM mv5;
300
306

> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 0);

# We should be able to query the MV even if there is no replica.
> SELECT * FROM mv5;
300
306

> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 1);

> SELECT * FROM mv5;
300
306

> INSERT INTO t2 VALUES (110);

# Wait until the insert is reflected, so we are just after a refresh.
> SELECT * FROM mv5;
300
306
330

# Turn off the cluster, and insert something, and then sleep through a scheduled refresh. (1b.)
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 0);

> INSERT INTO t2 VALUES (120);

# (See the explanation for the `+1` in materialized_views.slt at a similar `mz_sleep`.)
> SELECT mz_unsafe.mz_sleep(8+1);
<null>

# Turn it back on, and check that we recover. Data that were added while we slept should be visible now.
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 1);

> SELECT * FROM mv5;
300
306
330
360

## Test turning replicas off and on, part 2:
## No replica during MV creation.
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 0);
> CREATE MATERIALIZED VIEW mv_no_rep
  IN CLUSTER refresh_cluster
  WITH (REFRESH EVERY '8 sec')
  AS SELECT 10*y as y2 FROM t2;
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 1);
> SELECT * FROM mv_no_rep;
1000
1020
1100
1200

## Test turning replicas off and on, part 3:
## Turn the replica off _during_ the first refresh, and then turn the replica back on immediately.

> CREATE TABLE tg (a int);
> INSERT INTO tg VALUES (1000000);

# Refreshing this will take a non-trivial amount of time.
# The `*2/2` stuff is to make it take more CPU time without increasing memory consumption.
> CREATE MATERIALIZED VIEW mv_gs
  IN CLUSTER refresh_cluster
  WITH (REFRESH AT CREATION) AS
  SELECT sum(x*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2)
  FROM (SELECT generate_series(1,a) as x FROM tg);

> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 0);
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 1);

> SELECT * FROM mv_gs;
500000500000

# `mv_gs` consumes a non-trivial amount of memory because of the big `generate_series`, so let's drop it.
> DROP MATERIALIZED VIEW mv_gs;
> SELECT mz_unsafe.mz_sleep(1);
<null>

## Test turning replicas off and on, part 4:
## - Turn the replica off while it's in a non-caught-up state after an input change.
## - Turn the replica off while it's still rehydrating after turning a replica on.
## - More than 1 replicas.
## - MV that reads from an index.
> DELETE FROM tg;
> INSERT INTO tg VALUES (100);

> CREATE MATERIALIZED VIEW mv_gs2
  IN CLUSTER refresh_cluster
  WITH (REFRESH EVERY '4 sec') AS
  SELECT sum(x*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2)
  FROM (SELECT generate_series(1,a) as x FROM tg);

> CREATE DEFAULT INDEX
  IN CLUSTER refresh_cluster
  ON tg;
> CREATE MATERIALIZED VIEW mv_gs2_index
  IN CLUSTER refresh_cluster
  WITH (REFRESH EVERY '4 sec') AS
  SELECT sum(x*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2*2/2)
  FROM (SELECT generate_series(1,a) as x FROM tg);

# Wait for the first refresh.
> SELECT * FROM mv_gs2;
5050
> SELECT * FROM mv_gs2_index;
5050

> INSERT INTO tg VALUES (1000000);

# Mess with the replicas before the MV could catch up with the above insert.
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 0);
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 2);
> SELECT mz_unsafe.mz_sleep(0.5);
<null>

> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 0);
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 2);

# Check that we recover.
> SELECT * FROM mv_gs2;
500000505050
> SELECT * FROM mv_gs2_index;
500000505050

> DROP MATERIALIZED VIEW mv_gs2;
> DROP MATERIALIZED VIEW mv_gs2_index;

## REFRESH AT + REFRESH EVERY

> CREATE TABLE t3(x int);
> INSERT INTO t3 VALUES (1);

> CREATE MATERIALIZED VIEW mv6 WITH (REFRESH AT mz_now()::text::int8 + 6000, REFRESH EVERY '8 seconds') AS SELECT * FROM t3;
> SELECT * FROM mv6
1

> INSERT INTO t3 VALUES (2);
> SELECT * FROM mv6
1
2

> SELECT mz_unsafe.mz_sleep(8+1);
<null>

> INSERT INTO t3 VALUES (3);
> SELECT * FROM mv6
1
2
3

# Test that MVs that advance to the empty frontier do not retain read holds on
# their inputs. Regression test for #24469.

> CREATE TABLE t4 (x int)
> CREATE MATERIALIZED VIEW mv7 WITH (REFRESH AT CREATION) AS SELECT * FROM t4
> SELECT * FROM mv7

> SELECT f.write_frontier
  FROM mz_internal.mz_frontiers f
  JOIN mz_materialized_views m ON (m.id = f.object_id)
  WHERE m.name = 'mv7'
<null>

# Verify that `t4`'s read frontier advances past the read frontier of `mv7`.
> SELECT ft.read_frontier > fm.read_frontier
  FROM mz_internal.mz_frontiers fm
  JOIN mz_materialized_views m ON (m.id = fm.object_id)
  JOIN mz_internal.mz_frontiers ft ON (true)
  JOIN mz_tables t ON (t.id = ft.object_id)
  WHERE m.name = 'mv7' AND t.name = 't4'
true

# Test the same thing with multiple replicas.

> DROP MATERIALIZED VIEW mv7
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 2);

> CREATE MATERIALIZED VIEW mv7
  IN CLUSTER refresh_cluster
  WITH (REFRESH AT CREATION)
  AS SELECT * FROM t4
> SELECT * FROM mv7

> SELECT f.write_frontier
  FROM mz_internal.mz_frontiers f
  JOIN mz_materialized_views m ON (m.id = f.object_id)
  WHERE m.name = 'mv7'
<null>

> SELECT ft.read_frontier > fm.read_frontier
  FROM mz_internal.mz_frontiers fm
  JOIN mz_materialized_views m ON (m.id = fm.object_id)
  JOIN mz_internal.mz_frontiers ft ON (true)
  JOIN mz_tables t ON (t.id = ft.object_id)
  WHERE m.name = 'mv7' AND t.name = 't4'
true

# Test the same thing with initially zero replicas.

> DROP MATERIALIZED VIEW mv7
> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 0);

> CREATE MATERIALIZED VIEW mv7
  IN CLUSTER refresh_cluster
  WITH (REFRESH AT CREATION)
  AS SELECT * FROM t4

> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 1);
> SELECT * FROM mv7

> SELECT f.write_frontier
  FROM mz_internal.mz_frontiers f
  JOIN mz_materialized_views m ON (m.id = f.object_id)
  WHERE m.name = 'mv7'
<null>

> SELECT ft.read_frontier > fm.read_frontier
  FROM mz_internal.mz_frontiers fm
  JOIN mz_materialized_views m ON (m.id = fm.object_id)
  JOIN mz_internal.mz_frontiers ft ON (true)
  JOIN mz_tables t ON (t.id = ft.object_id)
  WHERE m.name = 'mv7' AND t.name = 't4'
true

# Test that the warmup optimization works, i.e. a REFRESH MV can hydrate prior
# to the next refresh time.

> CREATE TABLE t5 (x int)
> CREATE MATERIALIZED VIEW mv8
  IN CLUSTER refresh_cluster
  WITH (REFRESH AT CREATION, REFRESH AT mz_now()::string::int8 + 1000000)
  AS SELECT * FROM t5
> SELECT * FROM mv8

> ALTER CLUSTER refresh_cluster SET (REPLICATION FACTOR 2)

> SELECT r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_materialized_views m ON (m.id = h.object_id)
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  WHERE m.name = 'mv8'
r1 true
r2 true

## Warmup before the first refresh

# MV with a first refresh in the far future
> CREATE MATERIALIZED VIEW mv9 WITH (REFRESH AT mz_now()::text::int8 + 1000000) AS
  SELECT x*x FROM t3;

# Check that all operators have hydrated. Ideally, we'd want to check `mz_hydration_statuses`, but that is
# currently wrong for REFRESH MVs, see https://github.com/MaterializeInc/materialize/issues/25518
> SELECT DISTINCT hydrated
  FROM mz_internal.mz_compute_operator_hydration_statuses h JOIN mz_objects o ON (h.object_id = o.id)
  WHERE name = 'mv9';
true

# Try the same with a less trivial MV. (In particular, hydration behaves differently when we have a pipeline breaker,
# such as an arrangement.)
> CREATE MATERIALIZED VIEW mv10 WITH (REFRESH AT mz_now()::text::int8 + 1000000) AS
  SELECT DISTINCT x, y FROM t2, t3;

> SELECT DISTINCT hydrated
  FROM mz_internal.mz_compute_operator_hydration_statuses h JOIN mz_objects o ON (h.object_id = o.id)
  WHERE name = 'mv10';
true

# ----------------------------------------
# Automated cluster scheduling for REFRESH
# ----------------------------------------

> CREATE CLUSTER scheduled_cluster (SIZE = '1', SCHEDULE = ON REFRESH);

# No MV yet, so the cluster should be turned off.
> SELECT replication_factor FROM mz_catalog.mz_clusters WHERE name = 'scheduled_cluster';
0

> CREATE MATERIALIZED VIEW mv11
  IN CLUSTER scheduled_cluster
  WITH (REFRESH = EVERY '8 sec')
  AS SELECT count(*) FROM t2;

# The cluster should be turned on at some point.
> SELECT replication_factor FROM mz_catalog.mz_clusters WHERE name = 'scheduled_cluster';
1

# And then the cluster should compute MV results.
> SELECT * FROM mv11;
4

# The cluster should be turned off at some point.
> SELECT replication_factor FROM mz_catalog.mz_clusters WHERE name = 'scheduled_cluster';
0

# We should have a "drop" in `mz_audit_events`
> SELECT count(*) > 0
  FROM mz_audit_events
  WHERE
    event_type = 'drop' AND
    object_type = 'cluster-replica' AND
    (details->'cluster_name')::text = '"scheduled_cluster"' AND
    user IS NULL;
true

> DELETE FROM t2;

# The cluster should be turned on at some point again.
> SELECT replication_factor FROM mz_catalog.mz_clusters WHERE name = 'scheduled_cluster';
1

# And then the cluster should compute MV results again.
> SELECT * FROM mv11;
0

# We should have a "create" in `mz_audit_events`
> SELECT count(*) > 0
  FROM mz_audit_events
  WHERE
    event_type = 'create' AND
    object_type = 'cluster-replica' AND
    (details->'cluster_name')::text = '"scheduled_cluster"' AND
    user IS NULL;
true

# Things should keep working if we switch from managed to unmanaged cluster and then back.
> ALTER CLUSTER scheduled_cluster SET (MANAGED = false, SCHEDULE = MANUAL);
> ALTER CLUSTER scheduled_cluster SET (MANAGED = true, SIZE = '1', SCHEDULE = ON REFRESH);

> INSERT INTO t2 VALUES (64);

> SELECT * FROM mv11;
1

## When we create an MV whose first refresh is in the far future, we should immediately turn on the cluster briefly, so
## that the Persist shard's write frontier moves from 0 to the first refresh time.

> CREATE CLUSTER scheduled_cluster_2 (SIZE = '1', SCHEDULE = ON REFRESH);
> SELECT replication_factor FROM mz_catalog.mz_clusters WHERE name = 'scheduled_cluster_2';
0

> CREATE MATERIALIZED VIEW mv12
  IN CLUSTER scheduled_cluster_2
  WITH (REFRESH AT '3000-01-01 23:59')
  AS SELECT sum(y) FROM t2;

# The MV's write frontier should move away from 0.
> SELECT f.write_frontier > 0
  FROM mz_internal.mz_frontiers f
  JOIN mz_materialized_views m ON (m.id = f.object_id)
  WHERE m.name = 'mv12'
true

# But then the cluster should turn off.
> SELECT replication_factor FROM mz_catalog.mz_clusters WHERE name = 'scheduled_cluster_2';
0

## Unbilled replicas.
## - The auto scheduling won't create or drop unbilled replicas.
## - But unbilled replicas can affect the auto scheduling: If a cluster has an unbilled replica at the moment when a
##   refresh should happen, the unbilled replica might complete the refresh before the auto scheduling has a chance to
##   create a replica. (However, it's not guaranteed that an unbilled replica will prevent the auto scheduling from
##   creating a replica.)

# Creating an unbilled replica on a `SCHEDULE = ON REFRESH` cluster shouldn't blow things up.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
$ postgres-execute connection=mz_system
CREATE CLUSTER REPLICA scheduled_cluster.unbilled SIZE '2-2', BILLED AS 'free', INTERNAL;

> INSERT INTO t2 VALUES (200);

> SELECT * FROM mv11;
2

# Unbilled is not auto-dropped.

> SELECT r.name
  FROM mz_catalog.mz_cluster_replicas r, mz_catalog.mz_clusters c
  WHERE c.id = r.cluster_id AND c.name = 'scheduled_cluster';
unbilled

> SELECT mz_unsafe.mz_sleep(3);
<null>

> SELECT r.name
  FROM mz_catalog.mz_cluster_replicas r, mz_catalog.mz_clusters c
  WHERE c.id = r.cluster_id AND c.name = 'scheduled_cluster';
unbilled

# Things continue normally after the unbilled is (manually) dropped.

$ postgres-execute connection=mz_system
DROP CLUSTER REPLICA scheduled_cluster.unbilled;

> INSERT INTO t2 VALUES (99);

> SELECT * FROM mv11;
3

## REHYDRATION TIME ESTIMATE

> CREATE CLUSTER c_schedule_6 (SIZE = '1', SCHEDULE = ON REFRESH (REHYDRATION TIME ESTIMATE = '995 seconds'));
> CREATE CLUSTER c_schedule_7 (SIZE = '1');
> ALTER CLUSTER c_schedule_7 SET (SCHEDULE = ON REFRESH (REHYDRATION TIME ESTIMATE = '995 seconds'));

# Create MVs whose first refresh is 1000 seconds from now.
> CREATE MATERIALIZED VIEW mv13
  IN CLUSTER c_schedule_6
  WITH (REFRESH AT mz_now()::string::int8 + 1000 * 1000)
  AS SELECT count(*) FROM t2;
> CREATE MATERIALIZED VIEW mv14
  IN CLUSTER c_schedule_7
  WITH (REFRESH AT mz_now()::string::int8 + 1000 * 1000)
  AS SELECT count(*) FROM t2;

# Should be turned on soon due to the REHYDRATION TIME ESTIMATE.
> SELECT replication_factor FROM mz_catalog.mz_clusters WHERE name = 'c_schedule_6';
1
> SELECT replication_factor FROM mz_catalog.mz_clusters WHERE name = 'c_schedule_7';
1

# ----------------------------------------
# Introspection
# ----------------------------------------

> SET cluster = refresh_cluster;

## Create MVs for checking `mz_materialized_view_refreshes`.

# Const MVs, only REFRESH ATs
> CREATE MATERIALIZED VIEW mv14_at_const
  WITH (REFRESH AT '3000-01-01 23:59')
  AS SELECT 1;

> CREATE MATERIALIZED VIEW mv15_ats_const
  WITH (REFRESH AT '2500-01-01 23:59', REFRESH AT '3000-01-01 23:59')
  AS SELECT 1;

> CREATE MATERIALIZED VIEW mv_at_creation
  WITH (REFRESH AT CREATION)
  AS SELECT 1;

# Const MV, with REFRESH EVERY
> CREATE MATERIALIZED VIEW mv16_every_const
  WITH (REFRESH AT '2500-01-01 23:59', REFRESH AT '3000-01-01 23:59', REFRESH EVERY '1 minutes')
  AS SELECT 1;

# MV is before the first refresh
> CREATE MATERIALIZED VIEW mv17_before_first
  WITH (REFRESH AT mz_now()::string::int8 + 1000 * 1000)
  AS SELECT count(*) FROM t2;

# Cluster doesn't have a replica at the moment of creating an MV.
> CREATE CLUSTER cluster_no_replica SIZE = '2', REPLICATION FACTOR = 0;
> CREATE MATERIALIZED VIEW mv18_no_replica
  IN CLUSTER cluster_no_replica
  WITH (REFRESH EVERY '2 seconds')
  AS SELECT DISTINCT * FROM t2;

# MV between two refreshes
> CREATE MATERIALIZED VIEW mv19
  WITH (REFRESH EVERY '2 seconds')
  AS SELECT DISTINCT * FROM t2;

## Check the above MVs in `mz_materialized_view_refreshes`.

> SELECT last_completed_refresh, next_refresh
  FROM mz_internal.mz_materialized_view_refreshes mvr, mz_catalog.mz_materialized_views mv
  WHERE mv.id = mvr.materialized_view_id AND (name = 'mv14_at_const' OR name = 'mv15_ats_const');
32503766340000 <null>
32503766340000 <null>

> SELECT last_completed_refresh < mz_now(), next_refresh
  FROM mz_internal.mz_materialized_view_refreshes mvr, mz_catalog.mz_materialized_views mv
  WHERE mv.id = mvr.materialized_view_id AND name = 'mv_at_creation';
true <null>

> SELECT last_completed_refresh, next_refresh
  FROM mz_internal.mz_materialized_view_refreshes mvr, mz_catalog.mz_materialized_views mv
  WHERE mv.id = mvr.materialized_view_id AND name = 'mv16_every_const';
18446744073709551615 <null>

> SELECT last_completed_refresh, next_refresh > mz_now()
  FROM mz_internal.mz_materialized_view_refreshes mvr, mz_catalog.mz_materialized_views mv
  WHERE mv.id = mvr.materialized_view_id AND name = 'mv17_before_first';
<null> true

> SELECT last_completed_refresh, next_refresh < mz_now()
  FROM mz_internal.mz_materialized_view_refreshes mvr, mz_catalog.mz_materialized_views mv
  WHERE mv.id = mvr.materialized_view_id AND name = 'mv18_no_replica';
<null> true

> SELECT next_refresh::string::int8 - last_completed_refresh::string::int8
  FROM mz_internal.mz_materialized_view_refreshes mvr, mz_catalog.mz_materialized_views mv
  WHERE mv.id = mvr.materialized_view_id AND name = 'mv19';
2000

# Also check an old MV.
> SELECT next_refresh::string::int8 - last_completed_refresh::string::int8
  FROM mz_internal.mz_materialized_view_refreshes mvr, mz_catalog.mz_materialized_views mv
  WHERE mv.id = mvr.materialized_view_id AND name = 'mv1';
8000

# ----------------------------------------
# Cleanup
# ----------------------------------------

> DROP DATABASE materialized_view_refresh_options;
> DROP CLUSTER refresh_cluster;
> DROP CLUSTER scheduled_cluster;
> DROP CLUSTER scheduled_cluster_2;
> DROP CLUSTER c_schedule_6;
> DROP CLUSTER c_schedule_7;
> DROP CLUSTER cluster_no_replica;
