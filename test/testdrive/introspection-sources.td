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
> FETCH 1 c WITH (timeout='5s')
<TIMESTAMP> 1 true
> COMMIT

# Test that mz_compute_exports contains correct dataflow IDs.

> CREATE MATERIALIZED VIEW my_unique_mv_name AS SELECT * FROM t

> SELECT count(*)
  FROM
    mz_materialized_views mv,
    mz_internal.mz_compute_exports exp,
    mz_internal.mz_dataflows df
  WHERE
    mv.name = 'my_unique_mv_name' AND
    mv.id = exp.export_id AND
    exp.dataflow_id = df.id AND
    df.name LIKE '%my_unique_mv_name%'
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
  GROUP BY id, name
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

> CREATE DEFAULT INDEX ON t
> SELECT DISTINCT count(*)
  FROM mz_internal.mz_arrangement_sharing_per_worker
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

# Under arrangement specialization of only empty values, a single-column relation
# should demand 56 bytes per tuple instead of 88.
> SELECT records >= 300, size >= 300*56, capacity >= 300*56 FROM mz_internal.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_arr' OR name LIKE '%mv_arr'
true true true
true true true

# Test that non-arranging dataflows show up in `mz_dataflow_arrangement_sizes`

> CREATE TABLE t3 (c int)

> CREATE DEFAULT INDEX ii_empty ON t3

> SELECT records, batches, size < 1024, capacity < 1024, allocations < 512 FROM mz_internal.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_empty'
0 32 true true true

# Tests that arrangement sizes are approximate

> CREATE TABLE t4 (c int8)

> CREATE INDEX ii_t4 ON t4(c)

> SELECT records, batches, size < 1024, capacity < 1024, allocations < 512 FROM mz_internal.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t4'
0 32 true true true

> INSERT INTO t4 SELECT 1

> SELECT records, batches, size < 1024, capacity > 96, allocations > 0 FROM mz_internal.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t4'
1 32 true true true

> INSERT INTO t4 SELECT generate_series(1, 1000)

> SELECT records >= 1000 AND records <= 1001, batches > 0, size > 56*1000 AND size < 2*56*1000, capacity > 56*1000, allocations > 0 FROM mz_internal.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t4'
true true true true true

> DROP INDEX ii_t4

> SELECT records, batches, size, capacity, allocations FROM mz_internal.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t4'

# Test arrangement size logging for error dataflows

> CREATE TABLE t5(a int)

> INSERT INTO t5 SELECT -a FROM generate_series(1, 10000) g(a)

> CREATE VIEW vt5 AS SELECT a::uint2 FROM t5

> CREATE INDEX vt5_idx ON vt5(a)

> SELECT records, (size/1024/1024)::int FROM mz_internal.mz_dataflow_arrangement_sizes WHERE name LIKE '%vt5_idx'
10000 1

> DROP TABLE t5 CASCADE

# Test that `mz_dataflow_arrangement_sizes` shows dataflows not contained in the catalog.

> CREATE TABLE t6 (a int)
> INSERT INTO t6 SELECT generate_series(1, 1000)
> CREATE INDEX ii_t6 ON t6 (a)
> CREATE VIEW t6_plus_1 AS SELECT a + 1 AS b FROM t6
> CREATE INDEX ii_t6_plus_1 ON t6_plus_1 (b)
> DROP INDEX ii_t6

> SELECT name, records > 0 FROM mz_internal.mz_dataflow_arrangement_sizes WHERE name LIKE '%ii_t6%'
"Dataflow: materialize.public.ii_t6"        true
"Dataflow: materialize.public.ii_t6_plus_1" true

# Test that `mz_dataflow_arrangement_sizes` produces sensible results with accumulable reductions.
# The test scenario is adapted from #7428. Note that for c1 we expect the two largest arrangements
# to contain ((24 + 24) + 4) + (8 + 8) = 68 bytes per element, while for c2 we will have one with
# (24 + 4) + (8 + (24 + 8 + [104|112])) = [172|180] bytes per element and the other one with
# (24 + 4) + (24 + 4) + (8 + 8) = 72 bytes per element. Note that the range for c2 comes from
# different alignments for the enum Accum in ARM vs. x86 (namely, 16 vs. 8). Otherwise, the numbers
# come from row keys and values taking 24 bytes, offsets 4 bytes, timestamps 8 bytes, regular diffs
# 8 bytes, and reduce accumulable diffs [136|144] bytes.

# Commenting these out just for the revert of the new Row impl

# > CREATE TABLE ten (f1 integer);
#
# > INSERT INTO ten VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);
#
# > CREATE MATERIALIZED VIEW c1 AS
#   SELECT COUNT(DISTINCT 100 * a1.f1 + 10 * a2.f1 + a3.f1)
#   FROM ten AS a1, ten AS a2, ten AS a3;
#
# > CREATE MATERIALIZED VIEW c2 AS
#   SELECT 100 * a1.f1 + 10 * a2.f1 + a3.f1, COUNT(*)
#   FROM ten AS a1, ten AS a2, ten AS a3
#   GROUP BY 1
#   HAVING COUNT(*) > 1;
#
# > SELECT
#     records > 2 * 1000 AND records < 1.1 * 2 * 1000,
#     size > (84 + 84) * 1000 AND size < 1.1 * (84 + 84) * 1000,
#     capacity > (84 + 84) * 1000 AND capacity < 1.1 * (84 + 84) * 1000,
#     allocations > 0
#   FROM mz_internal.mz_dataflow_arrangement_sizes
#   WHERE name LIKE '%c1%';
# true true true true
#
# > SELECT
#     records > 2 * 1000 AND records < 1.1 * 2 * 1000,
#     size > (188 + 88) * 1000 AND size < 1.1 * (196 + 88) * 1000,
#     capacity > (188 + 88) * 1000 AND capacity < 1.1 * (196 + 88) * 1000,
#     allocations > 1000
#   FROM mz_internal.mz_dataflow_arrangement_sizes
#   WHERE name LIKE '%c2%';
# true true true true

# For coverage, we also include a recursive materialized view to account for dynamic timestamps.
# Here, the timestamps take not 8 bytes, but (8 + (24 + [0|1 * 8])) = [32|40] bytes. There are also
# not two but six largest arrangements of interest (for one min/max aggregation and two distincts).
# The distinct arrangements approach (24 + 4) + (0 + 4) + ([32|40] + 8) = [72|80] bytes per row;
# However, one of them takes slightly less at 2/3* ((24 + 4) + (0 + 4)) + (40 + 8) = [62|70] bytes
# due to containing 3000 records but only 2000 distinct elements. The min/max reduction input
# arrangement takes 1/2 * (24 + 4) + (24 + 24 + 4) + ([32|40] + 8) = [106|114] as there are 2000
# records, but only 1000 keys and also the values are represented as Vec<Row>. The output
# arrangement takes by a similar argument 1/3 * (24 + 4) + 2/3*(24 + 4) + ([32|40] + 8) = [68|76]
# as there are only 2000 output values but 3000 records in the arrangement.

# Commenting these out just for the revert of the new Row impl

# > CREATE MATERIALIZED VIEW rec AS
#   WITH MUTUALLY RECURSIVE (ERROR AT RECURSION LIMIT 3) term (key int, iter int) AS (
#     SELECT 100 * a1.f1 + 10 * a2.f1 + a3.f1 AS key, 0 AS iter
#     FROM ten AS a1, ten AS a2, ten AS a3
#     UNION
#     SELECT key, iter
#     FROM term
#     UNION
#     SELECT key, MAX(iter) + 1 AS iter
#     FROM term
#     GROUP BY key
#     HAVING MAX(iter) < 1
#     OPTIONS (AGGREGATE INPUT GROUP SIZE = 1)
#   )
#   SELECT * FROM term;
#
# > SELECT
#     records > (4 * 2 + 2 * 3) * 1000 AND records < 1.1 * (4 * 2 + 2 * 3) * 1000,
#     size > (3 * 80 * 2 + 70 * 3 + 130 * 2 + 76 * 3) * 1000
#       AND size < 1.1 * (3 * 88 * 2 + 78 * 3 + 138 * 2 + 84 * 3) * 1000,
#     capacity > (3 * 80 * 2 + 70 * 3 + 130 * 2 + 76 * 3) * 1000
#       AND capacity < 1.1 * (3 * 88 * 2 + 78 * 3 + 138 * 2 + 84 * 3) * 1000,
#     allocations > (4 * 2 + 2 * 3) * 1000
#   FROM mz_internal.mz_dataflow_arrangement_sizes
#   WHERE name LIKE '%rec%';
# true true true true

# > DROP TABLE ten CASCADE;

# Test mz_dataflow_arrangement_sizes with hierarchical reductions and top-k over
# monotonic inputs. The latter is when there is a possibility for memory misestimation
# since we include monoids in the diff field. Note that in both materialized views below,
# the output arrangements end up taking 72 bytes per element due to a similar reasoning
# as above. The input arrangement of m_minmax takes (24 + 4) + (8 + (24 + (32 + 32))) = 124
# bytes per element, while for m_top1 we have (24 + 4) + (0 + 4) + (8 + (48 + 16)) = 104.
# A Vec<ReductionMonoid> takes 24 for the Vec plus 32 bytes per ReductionMonoid, while a
# Top1Monoid takes 48 bytes plus 16 bytes per ColumnOrder.

# Commenting these out just for the revert of the new Row impl

# > CREATE SOURCE counter FROM LOAD GENERATOR COUNTER (TICK INTERVAL '2ms') WITH (SIZE = '1');
#
# > CREATE MATERIALIZED VIEW input AS
#   SELECT counter % 1000 + 1 AS f1, counter % 10 + 1 AS f2
#   FROM counter;
#
# > CREATE MATERIALIZED VIEW m_minmax AS
#   SELECT f1, MIN(f2), MAX(f2)
#   FROM input
#   GROUP BY f1;
#
# > CREATE MATERIALIZED VIEW m_top1 AS
#   SELECT DISTINCT ON (f1) f1, f2
#   FROM input
#   ORDER BY f1, f2 DESC;
#
# > SELECT
#     records >= 2 * 1000 AND records < 1.1 * 2 * 1000,
#     size >= (140 + 72) * 1000 AND size < 1.1 * (140 + 72) * 1000,
#     capacity >= (140 + 72) * 1000 AND capacity < 1.1 * (140 + 72) * 1000,
#     allocations > 1000
#   FROM mz_internal.mz_dataflow_arrangement_sizes
#   WHERE name LIKE '%m_minmax%';
# true true true true
#
# > SELECT
#     records >= 2 * 1000 AND records < 1.1 * 2 * 1000,
#     size >= (112 + 80) * 1000 AND size < 1.1 * (112 + 80) * 1000,
#     capacity >= (112 + 80) * 1000 AND capacity < 1.1 * (112 + 80) * 1000,
#     allocations > 1000
#   FROM mz_internal.mz_dataflow_arrangement_sizes
#   WHERE name LIKE '%m_top1%';
# true true true true

# > DROP SOURCE counter CASCADE;

# Test dataflow error introspection.

> CREATE TABLE zeros (a int)
> CREATE VIEW v_div_by_zero AS SELECT 1 / a AS x FROM zeros
> CREATE INDEX idx1_div_by_zero ON v_div_by_zero (x)
> CREATE MATERIALIZED VIEW mv_div_by_zero AS SELECT 1 / a AS x FROM zeros
> CREATE INDEX idx2_div_by_zero ON mv_div_by_zero (x)

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name

> INSERT INTO zeros VALUES (0)

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 1
idx2_div_by_zero 1
mv_div_by_zero   1

> INSERT INTO zeros VALUES (0), (0)

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 3
idx2_div_by_zero 3
mv_div_by_zero   3

> DELETE FROM zeros

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name

# Test that error logging is retracted when objects are dropped.

> INSERT INTO zeros VALUES (0), (0)

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 2
idx2_div_by_zero 2
mv_div_by_zero   2

> DROP INDEX idx1_div_by_zero

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx2_div_by_zero 2
mv_div_by_zero   2

> DROP MATERIALIZED VIEW mv_div_by_zero

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name

# Test logging of errors in things that advance to the empty frontier.

> CREATE MATERIALIZED VIEW mv_zero AS SELECT 0 AS x
> CREATE MATERIALIZED VIEW mv2_div_by_zero AS SELECT 1 / x FROM mv_zero

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
mv2_div_by_zero 1

> DROP MATERIALIZED VIEW mv_zero CASCADE

# Test logging of errors in reused indexes.

> CREATE INDEX idx1_div_by_zero ON v_div_by_zero (x)
> CREATE INDEX idx2_div_by_zero ON v_div_by_zero (x)
> CREATE INDEX idx3_div_by_zero ON v_div_by_zero (x)

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 2
idx2_div_by_zero 2
idx3_div_by_zero 2

> INSERT INTO zeros VALUES (0)

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
idx1_div_by_zero 3
idx2_div_by_zero 3
idx3_div_by_zero 3

> DROP TABLE zeros CASCADE

> SELECT name, count
  FROM mz_internal.mz_compute_error_counts c
  JOIN mz_objects o ON (c.export_id = o.id)
  ORDER BY name
