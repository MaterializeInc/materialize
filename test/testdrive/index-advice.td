# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.



> CREATE CLUSTER c1 REPLICAS (r1 (SIZE '1'));
> CREATE CLUSTER c2 REPLICAS (r1 (SIZE '1'));

> CREATE MATERIALIZED VIEW v11 IN CLUSTER c1 AS (SELECT 1);

> CREATE VIEW v21 AS (SELECT * FROM v11);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v21;

> CREATE VIEW v31 AS (SELECT * FROM v21);
> CREATE VIEW v32 AS (SELECT * FROM v21);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v32;

> CREATE VIEW v41 AS (SELECT * FROM v31);
> CREATE DEFAULT INDEX IN CLUSTER c2 ON v41;
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v41;

> CREATE MATERIALIZED VIEW v42 IN CLUSTER c1 AS (SELECT * FROM v31 UNION ALL SELECT * FROM v32);
> CREATE DEFAULT INDEX IN CLUSTER c2 ON v42;

#                                   dependency graph of the above objects
#
#                                                                      ┌────────────────────────┐
#                                                                      │          v11           │
#                                                          ┌──────────►│ materialized view (c1) │
#                                                          │           └────────────────────────┘
#                                                          │
#                                                          │
#                                                          │
#                                                ┌─────────┴─────────┐
#                                                │        v21        │
#                                    ┌──────────►│     index (c1)    │◄──────────┐
#                                    │           └───────────────────┘           │
#                                    │                                           │
#                                    │                                           │
#                                    │                                           │
#                          ┌─────────┴─────────┐                       ┌─────────┴─────────┐
#                          │        v31        │                       │        v32        │
#             ┌───────────►│       view        │◄──────────┬──────────►│     index (c1)    │
#             │            └───────────────────┘           │           └───────────────────┘
#             │                                            │
#             │                                            │
#             │                                            │
#   ┌─────────┴─────────┐                      ┌───────────┴────────────┐
#   │        v41        │                      │          v42           │
#   │   index (c1, c2)  │                      │ materialized view (c1) │
#   └───────────────────┘                      │       index (c2)       │
#                                              └────────────────────────┘
#
# With these dependencies, the index on v32 and the materialized view on v11 should be removed
# because these objects only have a single maintained dependency. But an index should be added
# on v31 because the work can be reused by v41 and v42. Moreover, v41 should be turned into a
# materialized view because the result is currently recomputed by the index on the remote
# cluster c2.


> SELECT o.name, a.hint, a.details FROM mz_internal.mz_index_advice AS a JOIN mz_objects AS o ON (a.object_id = o.id) ORDER BY name;
name             hint                           details
--------------------------------------------------------------------------------------------------------------------------------
v11              "convert to a view"            "no dependencies from sinks nor from objects on different clusters"
v21_primary_idx  "keep"                         "multiple downstream dependencies: {v31,v42}"
v31              "add index"                    "multiple downstream dependencies: {v41,v42}"
v32_primary_idx  "drop unless queried directly" "fewer than two downstream dependencies: {v42}"
v41              "convert to materialized view" "dependencies on multiple clusters: {v41_primary_idx,v41_primary_idx1}"
v41_primary_idx  "drop unless queried directly" "associated object does not have any dependencies (maintained or not maintained)"
v41_primary_idx1 "drop unless queried directly" "associated object does not have any dependencies (maintained or not maintained)"
v42              "keep"                         "dependencies from sinks or objects on different clusters: {v42_primary_idx}"
v42_primary_idx  "drop unless queried directly" "associated object does not have any dependencies (maintained or not maintained)"

# verify that all indexes and materialized views have a recommendation
> SELECT id FROM mz_objects WHERE id LIKE 'u%' AND type IN ('index', 'materialized-view')
  EXCEPT ALL
  SELECT o.id FROM mz_internal.mz_index_advice AS a INNER JOIN mz_objects AS o ON (a.object_id = o.id) WHERE o.type IN ('index', 'materialized-view');

# cleanup test
> DROP MATERIALIZED VIEW v11 CASCADE;





> CREATE MATERIALIZED VIEW v11 IN CLUSTER c1 AS (SELECT 1);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v11;

> SELECT o.name, a.hint, a.details FROM mz_internal.mz_index_advice AS a JOIN mz_objects AS o ON (a.object_id = o.id) ORDER BY name;
name              hint                               details
---------------------------------------------------------------------------------------------------------------------------------
 v11              "convert to a view with an index"  "no dependencies from sinks nor from objects on different clusters, but maintained dependencies on the same cluster: {v11_primary_idx}"
 v11_primary_idx  "drop unless queried directly"     "associated object does not have any dependencies (maintained or not maintained)"

# verify that all indexes and materialized views have a recommendation
> SELECT id FROM mz_objects WHERE id LIKE 'u%' AND type IN ('index', 'materialized-view')
  EXCEPT ALL
  SELECT o.id FROM mz_internal.mz_index_advice AS a INNER JOIN mz_objects AS o ON (a.object_id = o.id) WHERE o.type IN ('index', 'materialized-view');

# cleanup  test
> DROP MATERIALIZED VIEW v11 CASCADE;





> CREATE VIEW v11 AS (SELECT 1);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v11;

> SELECT o.name, a.hint, a.details FROM mz_internal.mz_index_advice AS a JOIN mz_objects AS o ON (a.object_id = o.id) ORDER BY name;
name              hint                            details
---------------------------------------------------------------------------------------------------------------------------------
 v11_primary_idx  "drop unless queried directly"  "associated object does not have any dependencies (maintained or not maintained)"

# verify that all indexes and materialized views have a recommendation
> SELECT id FROM mz_objects WHERE id LIKE 'u%' AND type IN ('index', 'materialized-view')
  EXCEPT ALL
  SELECT o.id FROM mz_internal.mz_index_advice AS a INNER JOIN mz_objects AS o ON (a.object_id = o.id) WHERE o.type IN ('index', 'materialized-view');

# cleanup test
> DROP VIEW v11 CASCADE;





> CREATE SOURCE s1
  IN CLUSTER c1
  FROM LOAD GENERATOR COUNTER (UP TO 100);

> CREATE DEFAULT INDEX IN CLUSTER c1 ON s1;

> SELECT o.name, a.hint, a.details FROM mz_internal.mz_index_advice AS a JOIN mz_objects AS o ON (a.object_id = o.id) ORDER BY name;
name                  hint                            details
---------------------------------------------------------------------------------------------------------------------------------
 s1_primary_idx  "drop unless queried directly"  "sources do not transform data and can expose data directly"

# verify that all indexes and materialized views have a recommendation
> SELECT id FROM mz_objects WHERE id LIKE 'u%' AND type IN ('index', 'materialized-view')
  EXCEPT ALL
  SELECT o.id FROM mz_internal.mz_index_advice AS a INNER JOIN mz_objects AS o ON (a.object_id = o.id) WHERE o.type IN ('index', 'materialized-view');

> DROP SOURCE s1 CASCADE;





> CREATE SOURCE s1
  IN CLUSTER c1
  FROM LOAD GENERATOR COUNTER (UP TO 100);

> CREATE VIEW v21 AS (SELECT * FROM s1);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v21;

> CREATE VIEW v22 AS (SELECT * FROM s1);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v22;

> SELECT o.name, a.hint, a.details FROM mz_internal.mz_index_advice AS a JOIN mz_objects AS o ON (a.object_id = o.id) ORDER BY name;
name                  hint                            details
---------------------------------------------------------------------------------------------------------------------------------
 v21_primary_idx      "drop unless queried directly"  "associated object does not have any dependencies (maintained or not maintained)"
 v22_primary_idx      "drop unless queried directly"  "associated object does not have any dependencies (maintained or not maintained)"

# verify that all indexes and materialized views have a recommendation
> SELECT id FROM mz_objects WHERE id LIKE 'u%' AND type IN ('index', 'materialized-view')
  EXCEPT ALL
  SELECT o.id FROM mz_internal.mz_index_advice AS a INNER JOIN mz_objects AS o ON (a.object_id = o.id) WHERE o.type IN ('index', 'materialized-view');

> DROP SOURCE s1 CASCADE;



> CREATE SOURCE s1
  IN CLUSTER c1
  FROM LOAD GENERATOR COUNTER (UP TO 100);

> CREATE DEFAULT INDEX IN CLUSTER c1 ON s1;

> CREATE VIEW v21 AS (SELECT * FROM s1);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v21;

> CREATE VIEW v22 AS (SELECT * FROM s1);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v22;

> SELECT o.name, a.hint, a.details FROM mz_internal.mz_index_advice AS a JOIN mz_objects AS o ON (a.object_id = o.id) ORDER BY name;
name                  hint                            details
---------------------------------------------------------------------------------------------------------------------------------
 s1_primary_idx       "drop unless queried directly"  "sources do not transform data and can expose data directly"
 v21_primary_idx      "drop unless queried directly"  "associated object does not have any dependencies (maintained or not maintained)"
 v22_primary_idx      "drop unless queried directly"  "associated object does not have any dependencies (maintained or not maintained)"

# verify that all indexes and materialized views have a recommendation
> SELECT id FROM mz_objects WHERE id LIKE 'u%' AND type IN ('index', 'materialized-view')
  EXCEPT ALL
  SELECT o.id FROM mz_internal.mz_index_advice AS a INNER JOIN mz_objects AS o ON (a.object_id = o.id) WHERE o.type IN ('index', 'materialized-view');

> DROP SOURCE s1 CASCADE;
