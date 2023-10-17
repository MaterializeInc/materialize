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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_specialized_arrangements = true

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

> RESET cluster;

> RESET cluster_replica;

> DROP CLUSTER test CASCADE;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_specialized_arrangements = false
