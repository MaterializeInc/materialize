# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-replica-size=1

# Test for a subset of the information returned by introspection sources.
# The test focuses on computing answers that are independent of particular
# timing measurement data, even if these stable answers may take some time
# to appear in the results of queries to introspection sources.

# Note that we count on the retry behavior of testdrive in this test
# since introspection sources may take some time to catch up.

# Introspection subscribes add noise to the introspection sources, so disable them.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_introspection_subscribes = false

# In case the environment has other replicas
> CREATE CLUSTER test SIZE '4-4'
> SET cluster = test

> CREATE TABLE t (a int)

> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t

> SELECT COUNT(*)
  FROM
    mz_materialized_views AS views,
    mz_introspection.mz_compute_exports AS compute_exports,
    mz_introspection.mz_compute_import_frontiers_per_worker AS import_frontiers
  WHERE
    views.name = 'mv' AND
    views.id = compute_exports.export_id AND
    compute_exports.export_id = import_frontiers.export_id AND
    time > 0
16

> CREATE VIEW vv AS SELECT * FROM t

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_introspection.mz_compute_import_frontiers AS import_frontiers)
1

> CREATE DEFAULT INDEX ON vv

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_introspection.mz_compute_import_frontiers AS import_frontiers)
2

> SELECT COUNT(*)
  FROM
    mz_views AS views,
    mz_indexes AS indexes,
    mz_introspection.mz_compute_exports compute_exports,
    mz_introspection.mz_compute_import_frontiers_per_worker AS import_frontiers
  WHERE
    views.name = 'vv' AND
    views.id = indexes.on_id AND
    indexes.id = compute_exports.export_id AND
    compute_exports.export_id = import_frontiers.export_id AND
    time > 0
16

> CREATE MATERIALIZED VIEW mvv AS SELECT * FROM vv

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_introspection.mz_compute_import_frontiers AS import_frontiers)
3

> DROP MATERIALIZED VIEW mvv

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_introspection.mz_compute_import_frontiers AS import_frontiers)
2

> DROP INDEX vv_primary_idx

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_introspection.mz_compute_import_frontiers AS import_frontiers)
1

> DROP MATERIALIZED VIEW mv

> SELECT COUNT(*)
  FROM (SELECT import_frontiers.export_id, import_frontiers.import_id
        FROM
            mz_introspection.mz_compute_import_frontiers AS import_frontiers)
0

# Test that frontiers of introspection sources advance at all.

! SELECT * FROM mz_introspection.mz_active_peeks AS OF 0
contains: Timestamp (0) is not valid for all inputs

# Test that logged subscribe frontiers advance beyond 0.

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (
  SELECT true
  FROM mz_introspection.mz_compute_frontiers f, mz_internal.mz_subscriptions s
  WHERE f.export_id = s.id AND time > 0)
> FETCH 1 c WITH (timeout='20s')
<TIMESTAMP> 1 true
> COMMIT

# Test that mz_compute_exports contains correct dataflow IDs.

> CREATE MATERIALIZED VIEW my_unique_mv_name AS SELECT * FROM t

> SELECT count(*)
  FROM
    mz_materialized_views mv,
    mz_introspection.mz_compute_exports exp,
    mz_introspection.mz_dataflows df
  WHERE
    mv.name = 'my_unique_mv_name' AND
    mv.id = exp.export_id AND
    exp.dataflow_id = df.id AND
    df.name LIKE '%my_unique_mv_name%'
1

# Test that each operator has at most one parent

> SELECT max(count) FROM (
    SELECT count(*)
    FROM mz_introspection.mz_dataflow_operator_parents
    GROUP BY id) counts
1

# Test that certain `*_per_worker` relations include the same data for each
# worker. The corresponding global relations rely on this to justify showing
# only the data from worker 0.

> SELECT DISTINCT count(*)
  FROM mz_introspection.mz_dataflows_per_worker
  GROUP BY id, name
16

> SELECT DISTINCT count(*)
  FROM mz_introspection.mz_dataflow_addresses_per_worker
  GROUP BY id, address
16

> SELECT DISTINCT count(*)
  FROM mz_introspection.mz_dataflow_channels_per_worker
  GROUP BY id, from_index, from_port, to_index, to_port
16

> SELECT DISTINCT count(*)
  FROM mz_introspection.mz_dataflow_operators_per_worker
  GROUP BY id, name
16

> SELECT DISTINCT count(*)
  FROM mz_introspection.mz_dataflow_operator_dataflows_per_worker
  GROUP BY id, name, dataflow_id, dataflow_name
16

> SELECT DISTINCT count(*)
  FROM mz_introspection.mz_dataflow_channel_operators_per_worker
  GROUP BY id, from_operator_id, to_operator_id
16

> SELECT DISTINCT count(*)
  FROM mz_introspection.mz_compute_exports_per_worker
  GROUP BY export_id, dataflow_id
16

> CREATE DEFAULT INDEX ON t
> SELECT DISTINCT count(*)
  FROM mz_introspection.mz_arrangement_sharing_per_worker
  GROUP BY operator_id, count
16

> DROP INDEX t_primary_idx

# Test that the roll-up of arrangement sizes is correct

> INSERT INTO t SELECT * FROM generate_series(1, 100)

> CREATE TABLE t2 (b int)
> INSERT INTO t2 SELECT * FROM generate_series(1, 200)

> CREATE VIEW vv_arr AS SELECT sum(a) FROM t JOIN t2 ON t.a = t2.b

> CREATE MATERIALIZED VIEW mv_arr AS SELECT * FROM vv_arr

> CREATE DEFAULT INDEX ii_arr ON vv_arr

# It's hard to come up with precise bounds because we might de-duplicate some data in arrangements.
> SELECT records >= 300, size > 0 FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_arr' OR name LIKE '%mv_arr'
true true
true true

# Test that non-arranging dataflows show up in `mz_dataflow_arrangement_sizes`

> CREATE TABLE t3 (c int)

> CREATE DEFAULT INDEX ii_empty ON t3

> SELECT records, size < 16 * 1024, allocations < 512 FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_empty'
0 true true

# Tests that arrangement sizes are approximate

> CREATE TABLE t4 (c int8)

> CREATE INDEX ii_t4 ON t4(c)

# We have 16 workers, and only want to ensure that the sizes are not egregious.

> SELECT records, size < 16 * 1024, allocations < 512 FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t4'
0 true true

> INSERT INTO t4 SELECT 1

> SELECT records, size < 16 * 1024, allocations > 0 FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t4'
1 true true

> INSERT INTO t4 SELECT generate_series(1, 1000)

# Determining exact sizes is difficult because of deduplication in arrangements, so we just use safe values.
> SELECT records >= 1000 AND records <= 1001, batches > 0, size > 0 AND size < 4*30000, capacity > 0, allocations > 0 FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t4'
true true true true true

> DROP INDEX ii_t4

> SELECT records, batches, size, capacity, allocations FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t4'

# Test arrangement size logging for error dataflows

> CREATE TABLE t5(a int)

> INSERT INTO t5 SELECT -a FROM generate_series(1, 10000) g(a)

> CREATE VIEW vt5 AS SELECT a::uint2 FROM t5

> CREATE INDEX vt5_idx ON vt5(a)

> SELECT records, (size/1024/1024)::int FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE '%vt5_idx'
10000 1

> DROP TABLE t5 CASCADE

# Test that `mz_dataflow_arrangement_sizes` shows dataflows not contained in the catalog.

> CREATE TABLE t6 (a int)
> INSERT INTO t6 SELECT generate_series(1, 1000)
> CREATE INDEX ii_t6 ON t6 (a)
> CREATE VIEW t6_plus_1 AS SELECT a + 1 AS b FROM t6
> CREATE INDEX ii_t6_plus_1 ON t6_plus_1 (b)
> DROP INDEX ii_t6

> SELECT name, records > 0 FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t6%'
"Dataflow: materialize.public.ii_t6"        true
"Dataflow: materialize.public.ii_t6_plus_1" true

# Test that `mz_dataflow_arrangement_sizes` produces sensible results with accumulable reductions.

> CREATE TABLE ten (f1 integer);

> INSERT INTO ten VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);

> CREATE MATERIALIZED VIEW c1 AS
  SELECT COUNT(DISTINCT 100 * a1.f1 + 10 * a2.f1 + a3.f1)
  FROM ten AS a1, ten AS a2, ten AS a3;

> CREATE MATERIALIZED VIEW c2 AS
  SELECT 100 * a1.f1 + 10 * a2.f1 + a3.f1, COUNT(*)
  FROM ten AS a1, ten AS a2, ten AS a3
  GROUP BY 1
  HAVING COUNT(*) > 1;

> SELECT
    records > 2 * 1000,
    records < 2 * 2 * 1000,
    size > 0,
    size < 4 * 130 * 1000,
    allocations < 2 * 2 * 1000
  FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%c1%';
true true true true true

> SELECT
    records > 1000,
    records < 2 * 1000,
    size > 0,
    size < 4 * 100 * 1024,
    allocations < 2 * 1000
  FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%c2%';
true true true true true

# For coverage, we also include a recursive materialized view to account for dynamic timestamps.
> CREATE MATERIALIZED VIEW rec AS
  WITH MUTUALLY RECURSIVE (ERROR AT RECURSION LIMIT 3) term (key int, iter int) AS (
    SELECT 100 * a1.f1 + 10 * a2.f1 + a3.f1 AS key, 0 AS iter
    FROM ten AS a1, ten AS a2, ten AS a3
    UNION
    SELECT key, iter
    FROM term
    UNION
    SELECT key, MAX(iter) + 1 AS iter
    FROM term
    GROUP BY key
    HAVING MAX(iter) < 1
    OPTIONS (AGGREGATE INPUT GROUP SIZE = 1)
  )
  SELECT * FROM term;

> SELECT
    records > 0,
    records < 2 * 12 * 1000,
    size > 0,
    size < 4 * 1000 * 1000,
    allocations < 2 * 12 * 1000
  FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%rec%';
true true true true true

> DROP TABLE ten CASCADE;

# Test mz_dataflow_arrangement_sizes with hierarchical reductions and top-k over
# monotonic inputs. The latter is when there is a possibility for memory misestimation
# since we include monoids in the diff field.
> CREATE CLUSTER counter_cluster SIZE = '1';
> CREATE SOURCE counter IN CLUSTER counter_cluster FROM LOAD GENERATOR COUNTER (TICK INTERVAL '2ms');

> CREATE VIEW input AS
  SELECT counter % 1000 + 1 AS f1, counter % 10 + 1 AS f2
  FROM counter;
> CREATE DEFAULT INDEX ON input;

> CREATE VIEW m_minmax AS
  SELECT f1, MIN(f2), MAX(f2)
  FROM input
  GROUP BY f1;
> CREATE DEFAULT INDEX ON m_minmax;

> CREATE VIEW m_top1 AS
  SELECT DISTINCT ON (f1) f1, f2
  FROM input
  ORDER BY f1, f2 DESC;
> CREATE DEFAULT INDEX ON m_top1;

> SELECT
    records >= 2 * 1000,
    records < 1.1 * 2 * 1000,
    size > 0,
    size < 4 * 200 * 1000,
    allocations < 2 * 2 * 1000
  FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%m_minmax%';
true true true true true

> SELECT
    records >= 2 * 1000,
    records < 2 * 2 * 1000,
    size > 0,
    size < 4 * 172 * 1000,
    allocations < 2 * 2 * 1000
  FROM mz_introspection.mz_dataflow_arrangement_sizes
  WHERE name LIKE '%m_top1%';
true true true true true

> DROP SOURCE counter CASCADE;

# Test dataflow error introspection.

> CREATE TABLE zeros (a int)
> CREATE VIEW v_div_by_zero AS SELECT 1 / a AS x FROM zeros
> CREATE INDEX idx1_div_by_zero ON v_div_by_zero (x)
> CREATE MATERIALIZED VIEW mv_div_by_zero AS SELECT 1 / a AS x FROM zeros
> CREATE INDEX idx2_div_by_zero ON mv_div_by_zero (x)

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name

> INSERT INTO zeros VALUES (0)

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 1
idx2_div_by_zero 1
mv_div_by_zero   1

> INSERT INTO zeros VALUES (0), (0)

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 3
idx2_div_by_zero 3
mv_div_by_zero   3

> DELETE FROM zeros

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name

# Test that error logging is retracted when objects are dropped.

> INSERT INTO zeros VALUES (0), (0)

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 2
idx2_div_by_zero 2
mv_div_by_zero   2

> DROP INDEX idx1_div_by_zero

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx2_div_by_zero 2
mv_div_by_zero   2

> DROP MATERIALIZED VIEW mv_div_by_zero

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name

# Test logging of errors in indexes that advance to the empty frontier.
#
# Note that the same is not expected to work for MVs that advance to the empty
# frontier. There is no reason to keep completed MV dataflows around, so we
# drop them immediately, removing all their associated logging in the process.

> CREATE MATERIALIZED VIEW mv_zero AS SELECT 0 AS x
> CREATE VIEW div_by_zero AS SELECT 1 / x FROM mv_zero
> CREATE INDEX idx_div_by_zero ON div_by_zero ()

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx_div_by_zero 1

> DROP MATERIALIZED VIEW mv_zero CASCADE

# Test logging of errors in reused indexes.

> CREATE INDEX idx1_div_by_zero ON v_div_by_zero (x)
> CREATE INDEX idx2_div_by_zero ON v_div_by_zero (x)
> CREATE INDEX idx3_div_by_zero ON v_div_by_zero (x)

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 2
idx2_div_by_zero 2
idx3_div_by_zero 2

> INSERT INTO zeros VALUES (0)

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 3
idx2_div_by_zero 3
idx3_div_by_zero 3

> DROP TABLE zeros CASCADE

> SELECT name, count
  FROM mz_introspection.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name

# Cleanup.
> DROP CLUSTER test CASCADE
