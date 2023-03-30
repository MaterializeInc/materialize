# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test for a subset of the information returned by introspection sources.
# The test focuses on computing answers that are independent of particular
# timing measurement data, even if these stable answers may take some time
# to appear in the results of queries to introspection sources.

# Note that we count on the retry behavior of testdrive in this test
# since introspection sources may take some time to catch up.

# The contents of the introspection tables depend on the replica size
$ skip-if
SELECT '${arg.default-replica-size}' != '4-4'

# In case the environment has other replicas
> SET cluster_replica = r1

> CREATE TABLE t (a int)

> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t

> SELECT count_all > 0
  FROM (SELECT SUM(count) as count_all
        FROM
            mz_materialized_views AS views,
            mz_internal.mz_compute_exports as compute_exports,
            mz_internal.mz_compute_delays_histogram_per_worker AS delays
        WHERE
            views.name = 'mv' AND
            views.id = compute_exports.export_id AND
            compute_exports.export_id = delays.export_id)
true

> SELECT COUNT(*)
  FROM
    mz_materialized_views AS views,
    mz_internal.mz_compute_exports AS compute_exports,
    mz_internal.mz_compute_import_frontiers_per_worker AS import_frontiers
  WHERE
    views.name = 'mv' AND
    views.id = compute_exports.export_id AND
    compute_exports.export_id = import_frontiers.export_id AND
    time > 0
16

> CREATE VIEW vv AS SELECT * FROM t

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.export_id, delays.import_id
        FROM
            mz_internal.mz_compute_delays_histogram AS delays)
1

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_internal.mz_compute_import_frontiers AS import_frontiers)
1

> CREATE DEFAULT INDEX ON vv

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.export_id, delays.import_id
        FROM
            mz_internal.mz_compute_delays_histogram AS delays)
2

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_internal.mz_compute_import_frontiers AS import_frontiers)
2

> SELECT count_all > 0
  FROM (SELECT SUM(count) as count_all
        FROM
            mz_views AS views,
            mz_indexes AS indexes,
            mz_internal.mz_compute_exports AS compute_exports,
            mz_internal.mz_compute_delays_histogram_per_worker AS delays
        WHERE
            views.name = 'vv' AND
            views.id = indexes.on_id AND
            indexes.id = compute_exports.export_id AND
            compute_exports.export_id = delays.export_id)
true

> SELECT COUNT(*)
  FROM
    mz_views AS views,
    mz_indexes AS indexes,
    mz_internal.mz_compute_exports compute_exports,
    mz_internal.mz_compute_import_frontiers_per_worker AS import_frontiers
  WHERE
    views.name = 'vv' AND
    views.id = indexes.on_id AND
    indexes.id = compute_exports.export_id AND
    compute_exports.export_id = import_frontiers.export_id AND
    time > 0
16

> CREATE MATERIALIZED VIEW mvv AS SELECT * FROM vv

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.export_id, delays.import_id
        FROM
            mz_internal.mz_compute_delays_histogram AS delays)
3

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_internal.mz_compute_import_frontiers AS import_frontiers)
3

> DROP MATERIALIZED VIEW mvv

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.export_id, delays.import_id
        FROM
            mz_internal.mz_compute_delays_histogram AS delays)
2

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_internal.mz_compute_import_frontiers AS import_frontiers)
2

> DROP INDEX vv_primary_idx

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.export_id, delays.import_id
        FROM
            mz_internal.mz_compute_delays_histogram AS delays)
1

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_internal.mz_compute_import_frontiers AS import_frontiers)
1

> DROP MATERIALIZED VIEW mv

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.export_id, delays.import_id
        FROM
            mz_internal.mz_compute_delays_histogram AS delays)
0

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_internal.mz_compute_import_frontiers AS import_frontiers)
0

# Test that frontiers of introspection sources advance at all.

! SELECT * FROM mz_internal.mz_active_peeks AS OF 0
contains: Timestamp (0) is not valid for all inputs

# Test that logged subscribe frontiers advance beyond 0.

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (
  SELECT true
  FROM mz_internal.mz_compute_frontiers f, mz_internal.mz_subscriptions s
  WHERE f.export_id = s.id AND time > 0)
> FETCH 1 c WITH (timeout='5s');
<TIMESTAMP> 1 true
> COMMIT

# Test that the contents of mz_compute_dependencies are correct.

> CREATE TABLE t1 (a int)
> CREATE TABLE t2 (b int)
> CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t1, t2
> CREATE DEFAULT INDEX ON mv1
> CREATE MATERIALIZED VIEW mv2 AS SELECT 1;

> SELECT export.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects export ON dep.export_id = export.id
  LEFT JOIN mz_objects import ON dep.import_id = import.id
  ORDER BY export.name, import.name
mv1             t1
mv1             t2
mv1_primary_idx mv1

# Test that mz_compute_exports contains correct dataflow IDs.

> CREATE MATERIALIZED VIEW my_unique_mv_name AS SELECT * FROM t1;

> SELECT count(*)
  FROM
    mz_materialized_views mv,
    mz_internal.mz_compute_exports exp,
    mz_internal.mz_dataflows df
  WHERE
    mv.name = 'my_unique_mv_name' AND
    mv.id = exp.export_id AND
    exp.dataflow_id = df.local_id AND
    df.name LIKE '%my_unique_mv_name%';
1

# Test that each operator has at most one parent

> SELECT max(count) FROM (
    SELECT count(*)
    FROM mz_internal.mz_dataflow_operator_parents
    GROUP BY id) counts
1

# Test that certain `*_per_worker` relations include the same data for each
# worker. The corresponding global relations rely on this to justify showing
# only the data from worker 0.

> SELECT DISTINCT count(*)
  FROM mz_internal.mz_dataflows_per_worker
  GROUP BY id, local_id, name
16

> SELECT DISTINCT count(*)
  FROM mz_internal.mz_dataflow_addresses_per_worker
  GROUP BY id, address
16

> SELECT DISTINCT count(*)
  FROM mz_internal.mz_dataflow_channels_per_worker
  GROUP BY id, from_index, from_port, to_index, to_port
16

> SELECT DISTINCT count(*)
  FROM mz_internal.mz_dataflow_operators_per_worker
  GROUP BY id, name
16

> SELECT DISTINCT count(*)
  FROM mz_internal.mz_dataflow_operator_dataflows_per_worker
  GROUP BY id, name, dataflow_id, dataflow_name
16

> SELECT DISTINCT count(*)
  FROM mz_internal.mz_dataflow_channel_operators_per_worker
  GROUP BY id, from_operator_id, to_operator_id
16

> SELECT DISTINCT count(*)
  FROM mz_internal.mz_compute_exports_per_worker
  GROUP BY export_id, dataflow_id
16

> SELECT DISTINCT count(*)
  FROM mz_internal.mz_compute_dependencies_per_worker
  GROUP BY export_id, import_id
16

> SELECT DISTINCT count(*)
  FROM mz_internal.mz_arrangement_sharing_per_worker
  GROUP BY operator_id, count
16

# Test that the roll-up of arrangement sizes is correct

> CREATE TABLE t2 (a int)

> INSERT INTO t SELECT * FROM generate_series(1, 100);

> INSERT INTO t2 SELECT * FROM generate_series(1, 200);

> CREATE VIEW vv2 AS SELECT sum(a) FROM t JOIN t2 USING (a);

> CREATE DEFAULT INDEX ii2 ON vv2

> SELECT records FROM mz_internal.mz_dataflow_arrangement_records WHERE name='ii2'
303
