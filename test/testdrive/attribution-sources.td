# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# UIDs are not stable across testdrive runs
$ set-regex match=[ut]\d+ replacement=UID

# VIEW + INDEX

> CREATE TABLE t(x INT NOT NULL, y INT, z TEXT);

> CREATE VIEW v AS
  SELECT t1.x AS x, t1.z AS z1, t2.z AS z2
  FROM t AS t1, t AS t2
  WHERE t1.x = t2.y;

> CREATE INDEX v_idx_x ON v(x);

> SELECT id, global_id FROM mz_internal.mz_dataflow_global_ids ORDER BY id, global_id;
8  UID
8  UID

> SELECT global_id, lir_id FROM mz_internal.mz_lir_mapping ORDER BY global_id, lir_id DESC;
UID  5
UID  4
UID  3
UID  2
UID  1
UID  7
UID  6

## attribution queries

# omitting sum(duration_ns) as duration, sum(count) as count
> SELECT global_id, lir_id, parent_lir_id, REPEAT(' ', nesting * 2) || operator AS operator
    FROM           mz_internal.mz_lir_mapping mlm
         LEFT JOIN mz_introspection.mz_compute_operator_durations_histogram mcodh
         ON (mlm.operator_id_start <= mcodh.id AND mcodh.id < mlm.operator_id_end)
  GROUP BY global_id, lir_id, operator, parent_lir_id, nesting
  ORDER BY global_id, lir_id DESC;
UID 1 2 "    Get::Collection UID"
UID 2 5 "  Arrange 1"
UID 3 4 "    Get::Collection UID"
UID 4 5 "  Arrange 3"
UID 5 <null> "Join::Differential 2» 4"
UID 6 7 "  Get::PassArrangements UID"
UID 7 <null> "Arrange 6"

# omitting pg_size_pretty(sum(size)) as size
> SELECT global_id, lir_id, parent_lir_id, repeat(' ', nesting * 2) || operator AS operator
    FROM           mz_internal.mz_lir_mapping mlm
         LEFT JOIN mz_introspection.mz_arrangement_sizes mas
         ON (mlm.operator_id_start <= mas.operator_id AND mas.operator_id < mlm.operator_id_end)
  GROUP BY global_id, lir_id, operator, parent_lir_id, nesting
  ORDER BY global_id, lir_id DESC;
UID  5  <null>  "Join::Differential 2» 4"
UID  4  5  "  Arrange 3"
UID  3  4  "    Get::Collection UID"
UID  2  5  "  Arrange 1"
UID  1  2  "    Get::Collection UID"
UID  7  <null>  "Arrange 6"
UID  6  7  "  Get::PassArrangements UID"

> DROP TABLE t CASCADE;

> SELECT COUNT(*) FROM mz_internal.mz_dataflow_global_ids;
0

> SELECT COUNT(*) FROM mz_internal.mz_lir_mapping;
0

# MATERIALIZED VIEW

> CREATE TABLE u(x INT NOT NULL, y INT, z TEXT);

> CREATE MATERIALIZED VIEW w AS
  SELECT t1.x AS x, t1.z AS z1, t2.z AS z2
  FROM u AS t1, u AS t2
  WHERE t1.x = t2.y;

# Sometimes id 19, sometimes 21
# > SELECT id, global_id FROM mz_internal.mz_dataflow_global_ids ORDER BY id, global_id;
# 21  UID


> SELECT global_id, lir_id FROM mz_internal.mz_lir_mapping ORDER BY global_id, lir_id DESC;
UID  5
UID  4
UID  3
UID  2
UID  1

## attribution queries

# omitting sum(duration_ns) as duration, sum(count) as count
> SELECT global_id, lir_id, parent_lir_id, REPEAT(' ', nesting * 2) || operator AS operator
    FROM           mz_internal.mz_lir_mapping mlm
         LEFT JOIN mz_introspection.mz_compute_operator_durations_histogram mcodh
         ON (mlm.operator_id_start <= mcodh.id AND mcodh.id < mlm.operator_id_end)
  GROUP BY global_id, lir_id, operator, parent_lir_id, nesting
  ORDER BY global_id, lir_id DESC;
UID  5  <null>  "Join::Differential 2» 4"
UID  4  5  "  Arrange 3"
UID  3  4  "    Get::Collection UID"
UID  2  5  "  Arrange 1"
UID  1  2  "    Get::Collection UID"

# omitting pg_size_pretty(sum(size)) as size
> SELECT global_id, lir_id, parent_lir_id, REPEAT(' ', nesting * 2) || operator AS operator
    FROM           mz_internal.mz_lir_mapping mlm
         LEFT JOIN mz_introspection.mz_arrangement_sizes mas
         ON (mlm.operator_id_start <= mas.operator_id AND mas.operator_id < mlm.operator_id_end)
  GROUP BY global_id, lir_id, operator, parent_lir_id, nesting
  ORDER BY global_id, lir_id DESC;
UID  5  <null>  "Join::Differential 2» 4"
UID  4  5  "  Arrange 3"
UID  3  4  "    Get::Collection UID"
UID  2  5  "  Arrange 1"
UID  1  2  "    Get::Collection UID"

> DROP TABLE u CASCADE;

> SELECT COUNT(*) FROM mz_internal.mz_dataflow_global_ids;
0

> SELECT COUNT(*) FROM mz_internal.mz_lir_mapping;
0

# ATTRIBUTING TOP K HINTS

> CREATE TABLE t(x INT NOT NULL, y INT, z TEXT);

> CREATE VIEW v2 AS SELECT DISTINCT ON(x, y) * FROM t ORDER BY x, y;

> CREATE INDEX v2_idx_x ON v2(x);

> SELECT mlm.global_id AS global_id, lir_id, parent_lir_id, REPEAT(' ', nesting * 2) || operator AS operator, levels, to_cut, savings, hint
    FROM           mz_internal.mz_lir_mapping mlm
         JOIN      mz_introspection.mz_dataflow_global_ids mdgi
         ON (mlm.global_id = mdgi.global_id)
         LEFT JOIN mz_introspection.mz_expected_group_size_advice megsa
         ON (megsa.dataflow_id = mdgi.id AND
             mlm.operator_id_start <= megsa.region_id AND megsa.region_id < mlm.operator_id_end)
  ORDER BY mlm.global_id, lir_id DESC;
UID  2  <null>  "TopK::Basic 1"  8  7  1792  15
UID  1  2  "  Get::PassArrangements UID"  <null>  <null>  <null>  <null>
UID  4  <null>  "Arrange 3"  <null>  <null>  <null>  <null>
UID  3  4  "  Get::PassArrangements UID"  <null>  <null>  <null>  <null>
