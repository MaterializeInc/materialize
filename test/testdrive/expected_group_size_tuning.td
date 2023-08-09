# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test uses introspection queries that need to be targeted to a replica
> SET cluster_replica = r1

# Start from a TPC-H load generator source with small data.
> CREATE SOURCE lgtpch FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.0001, TICK INTERVAL 0.1) FOR ALL TABLES WITH (SIZE = '1');

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

> WITH operators AS (
  SELECT
      dod.dataflow_id,
      dor.id AS region_id,
      dod.id,
      ars.records
  FROM
      mz_internal.mz_dataflow_operator_dataflows dod
      JOIN mz_internal.mz_dataflow_addresses doa
          ON dod.id = doa.id
      JOIN mz_internal.mz_dataflow_addresses dra
          ON dra.address = doa.address[:list_length(doa.address) - 1]
      JOIN mz_internal.mz_dataflow_operators dor
          ON dor.id = dra.id
      JOIN mz_internal.mz_arrangement_sizes ars
          ON ars.operator_id = dod.id
  WHERE
      dod.name = 'Arranged TopK input'
      OR dod.name = 'Arranged MinsMaxesHierarchical input'
      OR dod.name = 'Arrange ReduceMinsMaxes'
  ),
  levels AS (
      SELECT o.dataflow_id, o.region_id, COUNT(*) AS levels
      FROM operators o
      GROUP BY o.dataflow_id, o.region_id
  ),
  pivot AS (
      SELECT
          o1.dataflow_id,
          o1.region_id,
          o1.id,
          o1.records
      FROM operators o1
      WHERE
          o1.id = (
              SELECT MIN(o2.id)
              FROM operators o2
              WHERE
                  o2.dataflow_id = o1.dataflow_id
                  AND o2.region_id = o1.region_id
              OPTIONS (EXPECTED GROUP SIZE = 8)
          )
  ),
  candidates AS (
      SELECT
          o.dataflow_id,
          o.region_id,
          o.id,
          o.records
      FROM
          operators o
          JOIN pivot p
              ON o.dataflow_id = p.dataflow_id
                  AND o.region_id = p.region_id
                  AND o.id <> p.id
      WHERE o.records >= p.records * (1 - 0.15)
  ),
  cuts AS (
      SELECT c.dataflow_id, c.region_id, COUNT(*) to_cut
      FROM candidates c
      GROUP BY c.dataflow_id, c.region_id
      HAVING COUNT(*) > 0
  )
  SELECT
      dod.dataflow_name,
      dod.name AS region_name,
      l.levels,
      c.to_cut,
      pow(16, l.levels - c.to_cut) - 1 AS hint
  FROM cuts c
      JOIN levels l
          ON c.dataflow_id = l.dataflow_id AND c.region_id = l.region_id
      JOIN mz_internal.mz_dataflow_operator_dataflows dod
          ON dod.dataflow_id = c.dataflow_id AND dod.id = c.region_id
  ORDER BY dod.dataflow_name, dod.name;
"Dataflow: materialize.public.lineitem_by_orderkey" ReduceHierarchical 8 7 15
"Dataflow: materialize.public.lineitem_by_orderkey" TopK 8 7 15
"Dataflow: materialize.public.lineitem_by_partsuppkey" TopK 8 6 255
"Dataflow: materialize.public.lineitem_by_partsuppkey" ReduceHierarchical 8 6 255
"Dataflow: materialize.public.lineitem_by_partkey" ReduceHierarchical 8 6 255
"Dataflow: materialize.public.lineitem_by_partkey" TopK 8 6 255
"Dataflow: materialize.public.lineitem_by_suppkey" ReduceHierarchical 8 5 4095
"Dataflow: materialize.public.lineitem_by_suppkey" TopK 8 5 4095

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
  OPTIONS (EXPECTED GROUP SIZE = 4095);

> DROP MATERIALIZED VIEW IF EXISTS lineitem_by_orderkey;

> CREATE MATERIALIZED VIEW lineitem_by_orderkey AS
  SELECT l1.l_orderkey,
        MAX(l1.l_extendedprice),
        (SELECT l2.l_quantity FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
        OPTIONS (EXPECTED GROUP SIZE = 15)
        ORDER BY l2.l_extendedprice DESC
        LIMIT 1)
  FROM lineitem l1
  GROUP BY l1.l_orderkey;

> WITH operators AS (
  SELECT
      dod.dataflow_id,
      dor.id AS region_id,
      dod.id,
      ars.records
  FROM
      mz_internal.mz_dataflow_operator_dataflows dod
      JOIN mz_internal.mz_dataflow_addresses doa
          ON dod.id = doa.id
      JOIN mz_internal.mz_dataflow_addresses dra
          ON dra.address = doa.address[:list_length(doa.address) - 1]
      JOIN mz_internal.mz_dataflow_operators dor
          ON dor.id = dra.id
      JOIN mz_internal.mz_arrangement_sizes ars
          ON ars.operator_id = dod.id
  WHERE
      dod.name = 'Arranged TopK input'
      OR dod.name = 'Arranged MinsMaxesHierarchical input'
      OR dod.name = 'Arrange ReduceMinsMaxes'
  ),
  levels AS (
      SELECT o.dataflow_id, o.region_id, COUNT(*) AS levels
      FROM operators o
      GROUP BY o.dataflow_id, o.region_id
  ),
  pivot AS (
      SELECT
          o1.dataflow_id,
          o1.region_id,
          o1.id,
          o1.records
      FROM operators o1
      WHERE
          o1.id = (
              SELECT MIN(o2.id)
              FROM operators o2
              WHERE
                  o2.dataflow_id = o1.dataflow_id
                  AND o2.region_id = o1.region_id
              OPTIONS (EXPECTED GROUP SIZE = 8)
          )
  ),
  candidates AS (
      SELECT
          o.dataflow_id,
          o.region_id,
          o.id,
          o.records
      FROM
          operators o
          JOIN pivot p
              ON o.dataflow_id = p.dataflow_id
                  AND o.region_id = p.region_id
                  AND o.id <> p.id
      WHERE o.records >= p.records * (1 - 0.15)
  ),
  cuts AS (
      SELECT c.dataflow_id, c.region_id, COUNT(*) to_cut
      FROM candidates c
      GROUP BY c.dataflow_id, c.region_id
      HAVING COUNT(*) > 0
  )
  SELECT
      dod.dataflow_name,
      dod.name AS region_name,
      l.levels,
      c.to_cut,
      pow(16, l.levels - c.to_cut) - 1 AS hint
  FROM cuts c
      JOIN levels l
          ON c.dataflow_id = l.dataflow_id AND c.region_id = l.region_id
      JOIN mz_internal.mz_dataflow_operator_dataflows dod
          ON dod.dataflow_id = c.dataflow_id AND dod.id = c.region_id
  ORDER BY dod.dataflow_name, dod.name;
"Dataflow: materialize.public.lineitem_by_orderkey" ReduceHierarchical 8 7 15
"Dataflow: materialize.public.lineitem_by_partsuppkey" TopK 8 6 255
"Dataflow: materialize.public.lineitem_by_partsuppkey" ReduceHierarchical 8 6 255
"Dataflow: materialize.public.lineitem_by_partkey" ReduceHierarchical 8 6 255
"Dataflow: materialize.public.lineitem_by_partkey" TopK 8 6 255
"Dataflow: materialize.public.lineitem_by_suppkey" TopK 8 5 4095
