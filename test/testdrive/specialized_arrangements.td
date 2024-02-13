# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests specialized arrangement functionality. Note that this test requires
# the retry behavior of testdrive as it accesses introspection sources.

> CREATE CLUSTER test SIZE '2', REPLICATION FACTOR 2;

> SET cluster = test;

> SET cluster_replica = r1;

# Test specialization to empty values.
> CREATE TABLE v_empty(a int, b int);

> INSERT INTO v_empty SELECT a, a FROM generate_series(1, 10) a;

> CREATE DEFAULT INDEX on v_empty;

> SELECT COUNT(*)
  FROM mz_internal.mz_dataflow_operator_dataflows
  WHERE name LIKE '%ArrangeBy%[val: empty]%';
1

# Fast-path peek over specialized index.
> SELECT * FROM v_empty WHERE a = 1 AND b = 1;
1 1

# Literal lookup over specialized index.
> SELECT COUNT(*) FROM v_empty WHERE a = 1 AND b = 1;
1

# Full scan over specialized index.
> SELECT b FROM v_empty WHERE a = 1;
1

# Uses of specialized index along with non-specialized collection.
> CREATE TABLE t (a int, b int);

> INSERT INTO t VALUES (42, 42), (NULL, NULL);

> SELECT b FROM v_empty WHERE a = 1
  UNION ALL
  SELECT a FROM t WHERE a IS NOT NULL
  ORDER BY 1;
1
42

> SELECT a FROM t WHERE a IS NOT NULL
  EXCEPT ALL
  SELECT b FROM v_empty WHERE a = 1
  ORDER BY 1;
42

> INSERT INTO v_empty VALUES (42, 42);

# Linear join: Trace RowUnit - Local RowUnit
? EXPLAIN PHYSICAL PLAN FOR
  CREATE MATERIALIZED VIEW empty_val_join AS
  SELECT v.b
  FROM v_empty AS v
      JOIN t ON v.a = t.a AND v.b = t.b;
materialize.public.empty_val_join:
  Join::Linear
    linear_stage[0]
      closure
        project=(#1)
        filter=((#0) IS NOT NULL AND (#1) IS NOT NULL)
      lookup={ relation=1, key=[#0, #1] }
      stream={ key=[#0, #1], thinning=() }
    source={ relation=0, key=[#0, #1] }
    Get::PassArrangements materialize.public.v_empty
      raw=false
      arrangements[0]={ key=[#0, #1], permutation=id, thinning=() }
      types=[integer?, integer?]
    ArrangeBy
      raw=true
      arrangements[0]={ key=[#0, #1], permutation=id, thinning=() }
      types=[integer, integer]
      Get::Collection materialize.public.t
        raw=true

Source materialize.public.t
  filter=((#0) IS NOT NULL AND (#1) IS NOT NULL)

Used Indexes:
  - materialize.public.v_empty_primary_idx (differential join)

> CREATE MATERIALIZED VIEW empty_val_join AS
  SELECT v.b
  FROM v_empty AS v
      JOIN t ON v.a = t.a AND v.b = t.b;

> SELECT * FROM empty_val_join;
42

> SELECT COUNT(*)
  FROM mz_internal.mz_dataflow_operator_dataflows
  WHERE name LIKE '%ArrangeBy%[val: empty]%';
2

# More linear joins based on coverage: Trace RowUnit - Local RowRow
> CREATE VIEW v_empty_a AS SELECT a FROM v_empty;

> CREATE DEFAULT INDEX ON v_empty_a;

? EXPLAIN PHYSICAL PLAN FOR
  SELECT v.a, t.b
  FROM v_empty_a AS v
      JOIN t ON v.a = t.a;
Explained Query:
  Join::Linear
    linear_stage[0]
      closure
        filter=((#0) IS NOT NULL)
      lookup={ relation=1, key=[#0] }
      stream={ key=[#0], thinning=() }
    source={ relation=0, key=[#0] }
    Get::PassArrangements materialize.public.v_empty_a
      raw=false
      arrangements[0]={ key=[#0], permutation=id, thinning=() }
      types=[integer?]
    ArrangeBy
      raw=true
      arrangements[0]={ key=[#0], permutation=id, thinning=(#1) }
      types=[integer, integer?]
      Get::Collection materialize.public.t
        raw=true

Source materialize.public.t
  filter=((#0) IS NOT NULL)

Used Indexes:
  - materialize.public.v_empty_a_primary_idx (differential join)

> SELECT v.a, t.b
  FROM v_empty_a AS v
      JOIN t ON v.a = t.a;
42 42

> DROP VIEW v_empty_a;

# More linear joins based on coverage: Trace RowRow - Trace RowUnit
> CREATE INDEX v_a ON v_empty (a);

> CREATE TABLE t2 (a int);

> INSERT INTO t2 VALUES (42);

> CREATE DEFAULT INDEX ON t2;

? EXPLAIN PHYSICAL PLAN FOR
  SELECT v.b
  FROM v_empty AS v
      JOIN t2 ON v.a = t2.a;
Explained Query:
  Join::Linear
    linear_stage[0]
      closure
        project=(#1)
        filter=((#0) IS NOT NULL)
      lookup={ relation=1, key=[#0] }
      stream={ key=[#0], thinning=(#1) }
    source={ relation=0, key=[#0] }
    Get::PassArrangements materialize.public.v_empty
      raw=false
      arrangements[0]={ key=[#0], permutation=id, thinning=(#1) }
      types=[integer?, integer?]
    Get::PassArrangements materialize.public.t2
      raw=false
      arrangements[0]={ key=[#0], permutation=id, thinning=() }
      types=[integer?]

Used Indexes:
  - materialize.public.v_a (differential join)
  - materialize.public.t2_primary_idx (differential join)

> SELECT v.b
  FROM v_empty AS v
      JOIN t2 ON v.a = t2.a;
42

# More linear joins based on coverage: Trace RowUnit - Trace RowUnit
> CREATE INDEX t_a_b ON t(a, b);

? EXPLAIN PHYSICAL PLAN FOR
  SELECT v.b
  FROM v_empty AS v
      JOIN t ON v.a = t.a AND v.b = t.b;
Explained Query:
  Join::Linear
    linear_stage[0]
      closure
        project=(#1)
        filter=((#0) IS NOT NULL AND (#1) IS NOT NULL)
      lookup={ relation=1, key=[#0, #1] }
      stream={ key=[#0, #1], thinning=() }
    source={ relation=0, key=[#0, #1] }
    Get::PassArrangements materialize.public.v_empty
      raw=false
      arrangements[0]={ key=[#0, #1], permutation=id, thinning=() }
      types=[integer?, integer?]
    Get::PassArrangements materialize.public.t
      raw=false
      arrangements[0]={ key=[#0, #1], permutation=id, thinning=() }
      types=[integer?, integer?]

Used Indexes:
  - materialize.public.v_empty_primary_idx (differential join)
  - materialize.public.t_a_b (differential join)

> SELECT v.b
  FROM v_empty AS v
      JOIN t ON v.a = t.a AND v.b = t.b;
42

# Cover a delta join as well.
? EXPLAIN PHYSICAL PLAN FOR
  SELECT (v.b + t.a + t2.a) / 3
  FROM v_empty AS v
      JOIN t ON v.a = t.a AND v.b = t.b
      JOIN t2 ON v.a = t2.a;
Explained Query:
  Join::Delta
    plan_path[0]
      delta_stage[1]
        closure
          project=(#1)
        lookup={ relation=2, key=[#0] }
        stream={ key=[#0], thinning=(#1) }
      delta_stage[0]
        closure
          project=(#0, #2)
        lookup={ relation=1, key=[#0, #1] }
        stream={ key=[#0, #1], thinning=(#2) }
      initial_closure
        project=(#0..=#2)
        filter=((#0) IS NOT NULL AND (#1) IS NOT NULL)
        map=((((#1 + #0) + #0) / 3))
      source={ relation=0, key=[#0] }
    plan_path[1]
      delta_stage[1]
        closure
          project=(#1)
        lookup={ relation=2, key=[#0] }
        stream={ key=[#0], thinning=(#1) }
      delta_stage[0]
        closure
          project=(#0, #2)
          filter=((#0) IS NOT NULL AND (#1) IS NOT NULL)
          map=((((#1 + #0) + #0) / 3))
        lookup={ relation=0, key=[#0, #1] }
        stream={ key=[#0, #1], thinning=() }
      source={ relation=1, key=[#0, #1] }
    plan_path[2]
      delta_stage[1]
        closure
          project=(#2)
        lookup={ relation=1, key=[#0, #1] }
        stream={ key=[#1, #0], thinning=(#2) }
      delta_stage[0]
        closure
          project=(#1, #0, #2)
          filter=((#0) IS NOT NULL AND (#1) IS NOT NULL)
          map=((((#1 + #0) + #0) / 3))
        lookup={ relation=0, key=[#0] }
        stream={ key=[#0], thinning=() }
      source={ relation=2, key=[#0] }
    Get::PassArrangements materialize.public.v_empty
      raw=false
      arrangements[0]={ key=[#0, #1], permutation=id, thinning=() }
      arrangements[1]={ key=[#0], permutation=id, thinning=(#1) }
      types=[integer?, integer?]
    Get::PassArrangements materialize.public.t
      raw=false
      arrangements[0]={ key=[#0, #1], permutation=id, thinning=() }
      types=[integer?, integer?]
    Get::PassArrangements materialize.public.t2
      raw=false
      arrangements[0]={ key=[#0], permutation=id, thinning=() }
      types=[integer?]

Used Indexes:
  - materialize.public.v_empty_primary_idx (delta join 1st input (full scan))
  - materialize.public.v_a (delta join 1st input (full scan))
  - materialize.public.t2_primary_idx (delta join lookup)
  - materialize.public.t_a_b (delta join lookup)

> SELECT (v.b + t.a + t2.a) / 3
  FROM v_empty AS v
      JOIN t ON v.a = t.a AND v.b = t.b
      JOIN t2 ON v.a = t2.a;
42

> DROP INDEX v_a;

> DROP INDEX t_a_b;

> DROP TABLE t2;

? EXPLAIN PHYSICAL PLAN FOR
  CREATE MATERIALIZED VIEW empty_val_distinct_join AS
  SELECT v.b
  FROM v_empty AS v
      JOIN (SELECT DISTINCT a, b FROM t) AS t ON v.a = t.a AND v.b = t.b;
materialize.public.empty_val_distinct_join:
  Join::Linear
    linear_stage[0]
      closure
        project=(#1)
        filter=((#0) IS NOT NULL AND (#1) IS NOT NULL)
      lookup={ relation=0, key=[#0, #1] }
      stream={ key=[#0, #1], thinning=() }
    source={ relation=1, key=[#0, #1] }
    Get::PassArrangements materialize.public.v_empty
      raw=false
      arrangements[0]={ key=[#0, #1], permutation=id, thinning=() }
      types=[integer?, integer?]
    Reduce::Distinct
      val_plan
        project=()
      key_plan=id
      Get::Collection materialize.public.t
        raw=true

Source materialize.public.t
  filter=((#0) IS NOT NULL AND (#1) IS NOT NULL)

Used Indexes:
  - materialize.public.v_empty_primary_idx (differential join)

> CREATE MATERIALIZED VIEW empty_val_distinct_join AS
  SELECT v.b
  FROM v_empty AS v
      JOIN (SELECT DISTINCT a, b FROM t) AS t ON v.a = t.a AND v.b = t.b;

> SELECT * FROM empty_val_distinct_join;
42

# Note DistinctBy introduces two arrangements, one input and one output one.
> SELECT COUNT(*)
  FROM mz_internal.mz_dataflow_operator_dataflows
  WHERE name LIKE '%ArrangeBy%[val: empty]%'
     OR name LIKE '%DistinctBy%[val: empty]%';
4

# Two or more basic distinct aggregates should generate only one validating
# reduction that cannot use an empty value arrangement for distinctness.
> CREATE MATERIALIZED VIEW basic_agg_distinct AS
  SELECT
    array_agg(DISTINCT a) AS array_a,
    array_agg(DISTINCT b) AS array_b,
    list_agg(DISTINCT a) AS list_a
  FROM v_empty
  WHERE a = 42;

# Note DistinctBy introduces two arrangements, one input and one output one.
> SELECT COUNT(*)
  FROM mz_internal.mz_dataflow_operator_dataflows
  WHERE name LIKE '%Arranged ReduceInaccumulable Distinct%[val: empty]%'
     OR name LIKE '%ReduceInaccumulable Distinct%[val: empty]%';
5

> RESET cluster;

> RESET cluster_replica;
