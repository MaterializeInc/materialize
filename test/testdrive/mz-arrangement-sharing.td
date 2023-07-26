# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_with_mutually_recursive = true

> SET cluster_replica = r1

# from attributes/mir_unique_keys.slt
> CREATE TABLE u (c int, d int)
> CREATE VIEW v as SELECT c, d FROM u GROUP BY c, d
> CREATE DEFAULT INDEX on v

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
"Arrange bundle err"
"Arranged DistinctBy"
DistinctBy
DistinctByErrorCheck

> DROP TABLE u CASCADE

# from cte.slt
> CREATE TABLE squares (x int, y int)
> CREATE TABLE roots (x int, y int);
> CREATE MATERIALIZED VIEW v AS
  SELECT * FROM squares
  WHERE x IN (
      WITH squares_y AS (
          SELECT squares.y
      )
      SELECT y FROM roots
      WHERE y IN (
          SELECT y FROM squares_y
      )
  );

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
"ArrangeBy[[Column(0), Column(1)]]"
ArrangeBy[[Column(0)]]
"Arranged DistinctBy"
"Arranged DistinctBy"
DistinctBy
DistinctBy
DistinctByErrorCheck
DistinctByErrorCheck
JoinStage
JoinStage

> DROP TABLE squares CASCADE
> DROP TABLE roots CASCADE

# from explain/decorrelated_plan_as_json.slt
> CREATE TABLE t (
    a int,
    b int
  )

> CREATE VIEW v AS SELECT * FROM t ORDER BY b asc, a desc LIMIT 5

> CREATE DEFAULT INDEX ON v

> CREATE MATERIALIZED VIEW mv AS
  SELECT * FROM t WHERE a IS NOT NULL

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
"ArrangeBy[[Column(0), Column(1)]]"
"ArrangeBy[[Column(0), Column(1)]]-errors"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"

> DROP TABLE t CASCADE

# from list.slt
> CREATE TABLE t3(f1 int, f2 int, f3 int, f4 int, f5 int, f6 int, f7 int, f8 int, n int, m int, l int list)

> CREATE VIEW m3 AS SELECT * FROM t3

> CREATE DEFAULT INDEX ON m3

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
"ArrangeBy[[Column(0), Column(1), Column(2), Column(3), Column(4), Column(5), Column(6), Column(7), Column(8), Column(9), Column(10)]]"
"ArrangeBy[[Column(0), Column(1), Column(2), Column(3), Column(4), Column(5), Column(6), Column(7), Column(8), Column(9), Column(10)]]-errors"

> DROP TABLE t3 CASCADE

# from materialized_views.slt
# Disabled because of https://github.com/MaterializeInc/materialize/issues/20188
#> CREATE MATERIALIZED VIEW mat_clusters AS SELECT name FROM (SHOW CLUSTERS);
#> CREATE MATERIALIZED VIEW mat_connections AS SELECT name, type FROM (SHOW CONNECTIONS);
#> CREATE MATERIALIZED VIEW mat_databases AS SELECT name FROM (SHOW DATABASES);
#> CREATE MATERIALIZED VIEW mat_objects AS SELECT name FROM (SHOW OBJECTS);
#> CREATE MATERIALIZED VIEW mat_schemas AS SELECT name FROM (SHOW SCHEMAS);
#> CREATE MATERIALIZED VIEW mat_secrets AS SELECT name FROM (SHOW SECRETS);
#> CREATE MATERIALIZED VIEW mat_sinks AS SELECT name, type, size FROM (SHOW SINKS);
#> CREATE MATERIALIZED VIEW mat_sources AS SELECT name, type, size FROM (SHOW SOURCES);
#> CREATE MATERIALIZED VIEW mat_tables AS SELECT name FROM (SHOW TABLES);
#> CREATE MATERIALIZED VIEW mat_views AS SELECT name FROM (SHOW VIEWS);
#
#> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
#ArrangeBy[[Column(0)]]
#ArrangeBy[[Column(1)]]
#
#> DROP MATERIALIZED VIEW mat_clusters
#> DROP MATERIALIZED VIEW mat_connections
#> DROP MATERIALIZED VIEW mat_databases
#> DROP MATERIALIZED VIEW mat_objects
#> DROP MATERIALIZED VIEW mat_schemas
#> DROP MATERIALIZED VIEW mat_secrets
#> DROP MATERIALIZED VIEW mat_sinks
#> DROP MATERIALIZED VIEW mat_sources
#> DROP MATERIALIZED VIEW mat_tables
#> DROP MATERIALIZED VIEW mat_views

# from mztimestamp.slt
> CREATE VIEW intervals (a, b) AS VALUES (1, 10), (1, 2), (2, 13), (3, 1), (-3, 10), (5, 18446744073709551616)

> CREATE MATERIALIZED VIEW valid AS
  SELECT *
  FROM intervals
  WHERE mz_now() BETWEEN a AND b;

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;

> DROP VIEW intervals CASCADE

# from outer_join_simpliciation.slt
> create table foo_raw (a int4, b int8, u text)
> create table bar_raw (a int4, v text)
> create materialized view foo as select * from foo_raw where a is not null and b is not null;
> create materialized view bar as select distinct on (a) a, v from bar_raw
> create materialized view ban_nn as select * from bar where a is not null
> create table baz_raw (b int8, c int2, w text)
> create materialized view baz as select distinct on (b) b, c, w from baz_raw where b is not null
> create table quux_raw (c int2, x text)
> create materialized view quux as select distinct on (c) c, x from quux_raw where c is not null

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"

> DROP TABLE foo_raw CASCADE
> DROP TABLE bar_raw CASCADE
> DROP TABLE baz_raw CASCADE
> DROP TABLE quux_raw CASCADE

# from session-window-wmr.slt
> CREATE TABLE events (
      id int,
      event_time timestamp,
      user_id int,
      worth decimal
  );
> CREATE MATERIALIZED VIEW event_session AS
      WITH MUTUALLY RECURSIVE
          make_session (user_id int4, session tsrange)
              AS (
                  SELECT
                      user_id, tsrange(event_time, event_time + '5 m'::INTERVAL) AS session
                  FROM
                      events
              ),
          merge_session (user_id int4, session tsrange)
              AS (
                  SELECT
                      DISTINCT user_id, l_session + r_session
                  FROM
                      (
                          SELECT
                              l.user_id AS user_id, l.session AS l_session, r.session AS r_session
                          FROM
                              make_session AS l, make_session AS r
                          WHERE
                              l.user_id = r.user_id
                              AND (l.session && r.session OR l.session -|- r.session)
                          UNION ALL
                              SELECT
                                  make_session.user_id, make_session.session, merge_session.session
                              FROM
                                  make_session, merge_session
                              WHERE
                                  make_session.user_id = merge_session.user_id
                                  AND (
                                          make_session.session && merge_session.session
                                          OR make_session.session -|- merge_session.session
                                      )
                      )
              ),
          reduce_session (user_id int4, session tsrange)
              AS (
                  SELECT
                      user_id, tsrange(lower, upper)
                  FROM
                      (
                          SELECT
                              user_id, min(lower) AS lower, upper
                          FROM
                              (
                                  SELECT
                                      user_id, lower(session), max(upper(session)) AS upper
                                  FROM
                                      merge_session
                                  GROUP BY
                                      user_id, lower(session)
                              )
                          GROUP BY
                              user_id, upper
                      )
              )
      SELECT
          *
      FROM
          reduce_session;
> CREATE MATERIALIZED VIEW user_session_worth AS
      SELECT
          user_id, id, count, upper(session) - lower(session) AS session_len, sum AS worth
      FROM
          (
              SELECT
                  events.user_id, session, min(id) AS id, count(id), sum(worth)
              FROM
                  events
                  JOIN event_session ON
                          events.user_id = event_session.user_id
                          AND event_session.session @> events.event_time
              GROUP BY
                  events.user_id, session
          );

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
AccumulableErrorCheck
"Arrange ReduceCollation"
"Arrange ReduceMinsMaxes"
"Arrange ReduceMinsMaxes"
"Arrange ReduceMinsMaxes"
"Arrange recursive err"
"Arrange recursive err"
ArrangeAccumulable
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(2)]]
"Arranged DistinctBy"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Distinct recursive err"
"Distinct recursive err"
DistinctBy
DistinctByErrorCheck
ReduceAccumulable
ReduceCollation
"ReduceCollation Errors"
ReduceMinsMaxes
ReduceMinsMaxes
ReduceMinsMaxes
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"

> DROP TABLE events CASCADE

# from transactions.slt
> CREATE TABLE t (a int)
> CREATE MATERIALIZED VIEW v AS SELECT COUNT(*) FROM T

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
AccumulableErrorCheck
ArrangeAccumulable
ReduceAccumulable

> DROP TABLE t CASCADE

# from with mutually_recursive.slt
> CREATE TABLE t1 (f1 INTEGER);
> CREATE MATERIALIZED VIEW v1 AS
  WITH MUTUALLY RECURSIVE (RETURN AT RECURSION LIMIT 2)
    cnt (f1 INTEGER) AS (
      SELECT f1 FROM t1 UNION ALL SELECT f1+1 AS f1 FROM cnt
    )
  SELECT * FROM cnt;

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
"Arrange recursive err"
"Distinct recursive err"

> DROP TABLE t1 CASCADE

> CREATE TABLE t1 (f1 INTEGER);
> CREATE VIEW v1 AS
  WITH MUTUALLY RECURSIVE (RETURN AT RECURSION LIMIT 2)
    cnt (f1 INTEGER) AS (
      SELECT f1 FROM t1 UNION ALL SELECT f1+1 AS f1 FROM cnt
    )
  SELECT * FROM cnt;
> CREATE DEFAULT INDEX ON v1;

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
"Arrange export iterative"
"Arrange export iterative err"
"Arrange recursive err"
"Distinct recursive err"

> DROP TABLE t1 CASCADE

# from fetch-tail-as-of.td
> CREATE TABLE t1 (f1 INTEGER)
> CREATE DEFAULT INDEX ON t1

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]-errors

> DROP TABLE t1 CASCADE

# from fetch-tail-query.td
> CREATE TABLE t1 (f1 INTEGER)
> CREATE MATERIALIZED VIEW v1 AS SELECT count(*) FROM t1

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
AccumulableErrorCheck
ArrangeAccumulable
ReduceAccumulable

> DROP TABLE t1 CASCADE

# from fetch-tail-retraction.td
> CREATE TABLE inserts (f1 INTEGER)
> CREATE TABLE deletes (f1 INTEGER)
> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM inserts EXCEPT ALL SELECT * FROM deletes

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
ArrangeBy[[Column(0)]]
"Threshold local"

> DROP TABLE inserts CASCADE
> DROP TABLE deletes CASCADE

# from introspection-sources.td
> CREATE TABLE t (a int)
> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t
> CREATE VIEW vv AS SELECT * FROM t
> CREATE DEFAULT INDEX ON vv
> CREATE MATERIALIZED VIEW mvv AS SELECT * FROM vv
> CREATE TABLE t1 (a int)
> CREATE TABLE t2 (b int)
> CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t1, t2
> CREATE DEFAULT INDEX ON mv1
> CREATE MATERIALIZED VIEW mv2 AS SELECT 1
> CREATE MATERIALIZED VIEW my_unique_mv_name AS SELECT * FROM t1
> CREATE VIEW vv_arr AS SELECT sum(a) FROM t JOIN t2 ON t.a = t2.b
> CREATE MATERIALIZED VIEW mv_arr AS SELECT * FROM vv_arr
> CREATE DEFAULT INDEX ii_arr ON vv_arr
> CREATE TABLE t3 (c int)
> CREATE DEFAULT INDEX ii_empty ON t3

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
AccumulableErrorCheck
AccumulableErrorCheck
ArrangeAccumulable
ArrangeAccumulable
"ArrangeBy[[Column(0), Column(1)]]"
"ArrangeBy[[Column(0), Column(1)]]-errors"
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[]]
ArrangeBy[[]]
ReduceAccumulable
ReduceAccumulable

> DROP TABLE t3 CASCADE
> DROP TABLE t2 CASCADE
> DROP TABLE t1 CASCADE
> DROP TABLE t CASCADE

# from joins.td
> CREATE TABLE names (num bigint, name text)
> CREATE TABLE mods (num bigint, mod text)
> CREATE MATERIALIZED VIEW test1 AS
  SELECT * FROM names JOIN mods USING (num)
> CREATE MATERIALIZED VIEW test2 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names JOIN mods ON names.num = mods.num
> CREATE MATERIALIZED VIEW test3 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names, mods WHERE names.num = mods.num
> CREATE MATERIALIZED VIEW test4 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even'
> CREATE MATERIALIZED VIEW test5 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names LEFT JOIN mods ON names.num = mods.num
> CREATE MATERIALIZED VIEW test6 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names RIGHT JOIN mods ON names.num = mods.num
> CREATE MATERIALIZED VIEW test7 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even'
> CREATE MATERIALIZED VIEW test8 AS
  SELECT mods.* FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even'
> CREATE MATERIALIZED VIEW test9 AS
  SELECT foo.mod, foo.num, bar.name FROM names as bar, mods as foo
  WHERE bar.num = foo.num AND foo.mod = 'even'
> CREATE MATERIALIZED VIEW test10 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names, mods
> CREATE MATERIALIZED VIEW test11 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names CROSS JOIN mods
> CREATE MATERIALIZED VIEW test12 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names LEFT JOIN mods ON 1 = 0
> CREATE MATERIALIZED VIEW test13 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names RIGHT JOIN mods ON 1 = 0
> CREATE MATERIALIZED VIEW test14 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names FULL OUTER JOIN mods ON 1 = 0

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[]]
ArrangeBy[[]]
ArrangeBy[[]]
ArrangeBy[[]]
"Arranged DistinctBy"
"Arranged DistinctBy"
DistinctBy
DistinctBy
DistinctByErrorCheck
DistinctByErrorCheck

> DROP TABLE names CASCADE
> DROP TABLE mods CASCADE

# from linear-join-fuel.td
> CREATE CLUSTER linear_join REPLICAS (r1 (SIZE '1'))
> SET cluster=linear_join
> CREATE TABLE t1 (a int)
> CREATE MATERIALIZED VIEW v1 IN CLUSTER linear_join AS
  SELECT SUM(a1.a + a2.a * 10000) FROM t1 AS a1, t1 AS a2
> CREATE DEFAULT INDEX ON v1

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
AccumulableErrorCheck
ArrangeAccumulable
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[]]
ReduceAccumulable

> DROP TABLE t1 CASCADE
> SET cluster=default
> DROP CLUSTER linear_join CASCADE

# from negative-multiplicities.td
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_repeat_row  = true

> CREATE TABLE base (data bigint, diff bigint)
> CREATE MATERIALIZED VIEW data AS SELECT data FROM base, repeat_row(diff)
> CREATE VIEW topk AS
  SELECT grp.id, count(t.data) AS top_2_count,
           (SELECT COUNT(d.data) FROM data d WHERE d.data % 2 = grp.id) AS total_count
    FROM (SELECT generate_series(0,1) id) grp,
           LATERAL (SELECT data FROM data WHERE data % 2 = grp.id ORDER BY data LIMIT 2) t
    GROUP BY grp.id
> CREATE DEFAULT INDEX ON topk
> CREATE VIEW max_data AS
  SELECT MAX(data) FROM data
> CREATE DEFAULT INDEX ON max_data
> CREATE VIEW collation AS
  SELECT
      data,
      COUNT(DISTINCT data),
      STRING_AGG(data::text || '1',  ',') AS data_1,
      MIN(data),
      MAX(DISTINCT data),
      SUM(data),
      STRING_AGG(data::text || '2',  ',') AS data_2
    FROM data
    GROUP BY data
> CREATE DEFAULT INDEX ON collation

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
AccumulableErrorCheck
AccumulableErrorCheck
AccumulableErrorCheck
"Arrange ReduceCollation"
"Arrange ReduceMinsMaxes"
"Arrange ReduceMinsMaxes"
"Arrange bundle err"
ArrangeAccumulable
ArrangeAccumulable
ArrangeAccumulable
"ArrangeBy[[CallBinary { func: ModInt64, expr1: Column(0), expr2: Literal(Ok(Row{[Int64(2)]}), ColumnType { scalar_type: Int64, nullable: false }) }]]"
"ArrangeBy[[CallUnary { func: CastInt32ToInt64(CastInt32ToInt64), expr: Column(0) }]]"
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
"Arranged Accumulable"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged MinsMaxesHierarchical input"
"Arranged ReduceFuseBasic input"
"Arranged ReduceInaccumulable"
"Arranged ReduceInaccumulable"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
ReduceAccumulable
ReduceAccumulable
ReduceAccumulable
ReduceCollation
"ReduceCollation Errors"
ReduceFuseBasic
ReduceInaccumulable
ReduceInaccumulable
"ReduceInaccumulable Error Check"
ReduceMinsMaxes
ReduceMinsMaxes
"Reduced Accumulable"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced Fallibly MinsMaxesHierarchical"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"

> DROP TABLE base CASCADE

# from orms.td
> CREATE TABLE t (i bigint, t text)
> CREATE DEFAULT INDEX ON t
> CREATE INDEX complex_index ON t (t::varchar, i::string)

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
"ArrangeBy[[CallUnary { func: CastStringToVarChar(CastStringToVarChar { length: None, fail_on_len: false }), expr: Column(1) }, CallUnary { func: CastInt64ToString(CastInt64ToString), expr: Column(0) }]]"
"ArrangeBy[[CallUnary { func: CastStringToVarChar(CastStringToVarChar { length: None, fail_on_len: false }), expr: Column(1) }, CallUnary { func: CastInt64ToString(CastInt64ToString), expr: Column(0) }]]-errors"
"ArrangeBy[[Column(0), Column(1)]]"
"ArrangeBy[[Column(0), Column(1)]]-errors"

> DROP TABLE t CASCADE

# Check mz_introspection
> SET CLUSTER TO mz_introspection

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
AccumulableErrorCheck
ArrangeAccumulable
"ArrangeBy[[Column(0), Column(1), Column(2), Column(3)]]"
"ArrangeBy[[Column(0), Column(1), Column(2), Column(3)]]-errors"
"ArrangeBy[[Column(0), Column(2)]]"
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
ArrangeBy[[Column(0)]]-errors
"ArrangeBy[[Column(1), Column(0)]]"
"ArrangeBy[[Column(1), Column(3)]]"
"ArrangeBy[[Column(1), Column(3)]]"
ArrangeBy[[Column(1)]]
ArrangeBy[[Column(1)]]
ArrangeBy[[Column(1)]]
ArrangeBy[[Column(1)]]-errors
ArrangeBy[[Column(1)]]-errors
"ArrangeBy[[Column(2), Column(3)]]"
"ArrangeBy[[Column(2), Column(3)]]-errors"
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(2)]]-errors
ArrangeBy[[Column(3)]]
"ArrangeBy[[Column(4), Column(5), Column(6)]]"
"ArrangeBy[[Column(4), Column(5), Column(6)]]-errors"
"Arranged DistinctBy"
"Arranged ReduceInaccumulable"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
"Arranged TopK input"
DistinctBy
DistinctByErrorCheck
JoinStage
JoinStage
ReduceAccumulable
ReduceInaccumulable
"ReduceInaccumulable Error Check"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"
"Reduced TopK input"

> SET CLUSTER TO default

# Check dataflows of our logging infrastructure with log_logging
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET log_filter = 'debug'

> CREATE CLUSTER REPLICA default.replica SIZE = '2', INTROSPECTION DEBUGGING = true;

> SET cluster_replica = replica;

> SELECT mdo.name FROM mz_internal.mz_arrangement_sharing mash JOIN mz_internal.mz_dataflow_operators mdo ON mash.operator_id = mdo.id ORDER BY mdo.name;
"Arrange Timely(Reachability)"
"ArrangeByKey Compute(DataflowCurrent)"
"ArrangeByKey Compute(DataflowDependency)"
"ArrangeByKey Compute(FrontierCurrent)"
"ArrangeByKey Compute(FrontierDelay)"
"ArrangeByKey Compute(ImportFrontierCurrent)"
"ArrangeByKey Compute(PeekCurrent)"
"ArrangeByKey Compute(PeekDuration)"
"ArrangeByKey Differential(ArrangementBatches)"
"ArrangeByKey Differential(ArrangementRecords)"
"ArrangeByKey Differential(Sharing)"
"ArrangeByKey Timely(Addresses)"
"ArrangeByKey Timely(Channels)"
"ArrangeByKey Timely(Elapsed)"
"ArrangeByKey Timely(Histogram)"
"ArrangeByKey Timely(MessagesReceived)"
"ArrangeByKey Timely(MessagesSent)"
"ArrangeByKey Timely(Operates)"
"ArrangeByKey Timely(Parks)"

> DROP CLUSTER REPLICA default.replica
