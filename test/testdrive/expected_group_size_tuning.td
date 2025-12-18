# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=120s

# This test uses introspection queries that need to be targeted to a replica
> SET cluster_replica = r1

# Start from a TPC-H load generator source with small data.
> CREATE CLUSTER lgtpch_cluster_tuning SIZE 'scale=1,workers=1';
> BEGIN
> CREATE SOURCE lgtpch IN CLUSTER lgtpch_cluster_tuning FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.0001, TICK INTERVAL 0.1, UP TO 1000);

> CREATE TABLE customer FROM SOURCE lgtpch (REFERENCE customer);
> CREATE TABLE lineitem FROM SOURCE lgtpch (REFERENCE lineitem);
> CREATE TABLE nation FROM SOURCE lgtpch (REFERENCE nation);
> CREATE TABLE orders FROM SOURCE lgtpch (REFERENCE orders);
> CREATE TABLE part FROM SOURCE lgtpch (REFERENCE part);
> CREATE TABLE partsupp FROM SOURCE lgtpch (REFERENCE partsupp);
> CREATE TABLE region FROM SOURCE lgtpch (REFERENCE region);
> CREATE TABLE supplier FROM SOURCE lgtpch (REFERENCE supplier);
> COMMIT

# Create a set of materialized views for testing based on the TPC-H schema.
> CREATE MATERIALIZED VIEW lineitem_by_orderkey AS
  SELECT l1.l_orderkey,
        MAX(l1.l_extendedprice),
        (SELECT l2.l_quantity FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
        ORDER BY l2.l_extendedprice DESC
        LIMIT 1)
  FROM lineitem l1
  GROUP BY l1.l_orderkey;

> CREATE MATERIALIZED VIEW lineitem_by_partsuppkey AS
  SELECT l1.l_partkey,
        l_suppkey,
        MAX(l1.l_extendedprice),
        (SELECT l2.l_quantity FROM lineitem l2
        WHERE l2.l_partkey = l1.l_partkey
          AND l2.l_suppkey = l1.l_suppkey
        ORDER BY l2.l_extendedprice DESC
        LIMIT 1)
  FROM lineitem l1
  GROUP BY l1.l_partkey, l_suppkey;

> CREATE MATERIALIZED VIEW lineitem_by_partkey AS
  SELECT l1.l_partkey,
        MAX(l1.l_extendedprice),
        (SELECT l2.l_quantity FROM lineitem l2
        WHERE l2.l_partkey = l1.l_partkey
        ORDER BY l2.l_extendedprice DESC
        LIMIT 1)
  FROM lineitem l1
  GROUP BY l1.l_partkey;

> CREATE MATERIALIZED VIEW lineitem_by_suppkey AS
  SELECT l1.l_suppkey,
        MAX(l1.l_extendedprice),
        (SELECT l2.l_quantity FROM lineitem l2
        WHERE l2.l_suppkey = l1.l_suppkey
        ORDER BY l2.l_extendedprice DESC
        LIMIT 1)
  FROM lineitem l1
  GROUP BY l1.l_suppkey;

# Check that we know the values of the perfect hints for TPC-H.
> SELECT pow(16, ceil(log(16, MAX(group_size)))) - 1 AS perfect_hint
  FROM (
        SELECT l_orderkey, COUNT(*) AS group_size
        FROM lineitem
        GROUP BY l_orderkey
  );
15

> SELECT pow(16, ceil(log(16, MAX(group_size)))) - 1 perfect_hint
  FROM (
        SELECT l_partkey, l_suppkey, COUNT(*) AS group_size
        FROM lineitem
        GROUP BY l_partkey, l_suppkey
  );
255

> SELECT pow(16, ceil(log(16, MAX(group_size)))) - 1 perfect_hint
  FROM (
        SELECT l_partkey, COUNT(*) AS group_size
        FROM lineitem
        GROUP BY l_partkey
  );
255

> SELECT pow(16, ceil(log(16, MAX(group_size)))) - 1 perfect_hint
  FROM (
        SELECT l_suppkey, COUNT(*) AS group_size
        FROM lineitem
        GROUP BY l_suppkey
  );
4095

> SELECT dataflow_name, region_name, levels, to_cut, hint
  FROM mz_introspection.mz_expected_group_size_advice
  ORDER BY dataflow_name, region_name;
"Dataflow: materialize.public.lineitem_by_orderkey" ReduceHierarchical 8 7 15
"Dataflow: materialize.public.lineitem_by_orderkey" TopK 8 7 15
"Dataflow: materialize.public.lineitem_by_partsuppkey" TopK 8 6 255
"Dataflow: materialize.public.lineitem_by_partsuppkey" ReduceHierarchical 8 6 255
"Dataflow: materialize.public.lineitem_by_partkey" ReduceHierarchical 8 6 255
"Dataflow: materialize.public.lineitem_by_partkey" TopK 8 6 255
"Dataflow: materialize.public.lineitem_by_suppkey" ReduceHierarchical 8 5 4095
"Dataflow: materialize.public.lineitem_by_suppkey" TopK 8 5 4095

# Validate that there are positive memory savings listed for the entries above.
> SELECT COUNT(savings > 0)
  FROM mz_introspection.mz_expected_group_size_advice;
8

# Create partly hinted versions of the views and check that the advice gets revised accordingly.
> DROP MATERIALIZED VIEW lineitem_by_suppkey;

> CREATE MATERIALIZED VIEW lineitem_by_suppkey AS
  SELECT l1.l_suppkey,
        MAX(l1.l_extendedprice),
        (SELECT l2.l_quantity FROM lineitem l2
        WHERE l2.l_suppkey = l1.l_suppkey
        ORDER BY l2.l_extendedprice DESC
        LIMIT 1)
  FROM lineitem l1
  GROUP BY l1.l_suppkey
  OPTIONS (AGGREGATE INPUT GROUP SIZE = 4095);

> DROP MATERIALIZED VIEW IF EXISTS lineitem_by_orderkey;

> CREATE MATERIALIZED VIEW lineitem_by_orderkey AS
  SELECT l1.l_orderkey,
        MAX(l1.l_extendedprice),
        (SELECT l2.l_quantity FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
        OPTIONS (LIMIT INPUT GROUP SIZE = 15)
        ORDER BY l2.l_extendedprice DESC
        LIMIT 1)
  FROM lineitem l1
  GROUP BY l1.l_orderkey;

> SELECT dataflow_name, region_name, levels, to_cut, hint
  FROM mz_introspection.mz_expected_group_size_advice
  ORDER BY dataflow_name, region_name;
"Dataflow: materialize.public.lineitem_by_orderkey" ReduceHierarchical 8 7 15
"Dataflow: materialize.public.lineitem_by_partsuppkey" TopK 8 6 255
"Dataflow: materialize.public.lineitem_by_partsuppkey" ReduceHierarchical 8 6 255
"Dataflow: materialize.public.lineitem_by_partkey" ReduceHierarchical 8 6 255
"Dataflow: materialize.public.lineitem_by_partkey" TopK 8 6 255
"Dataflow: materialize.public.lineitem_by_suppkey" TopK 8 5 4095

# Validate that there are positive memory savings listed for the entries above.
> SELECT COUNT(savings > 0)
  FROM mz_introspection.mz_expected_group_size_advice;
6

> DROP SOURCE lgtpch CASCADE
