# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.



> CREATE CLUSTER c4 REPLICAS (r1 (SIZE '1'));
> CREATE VIEW v11 AS (SELECT 1);
> CREATE DEFAULT INDEX IN CLUSTER c4 ON v11;

> SELECT o.name, a.recommendation, a.details FROM mz_internal.mz_index_advice AS a JOIN mz_objects AS o ON (a.object_id = o.id) ORDER BY name;
name              recommendation                  details
---------------------------------------------------------------------------------------------------------------------------------
 v11_primary_idx  "drop unless queried directly"  "associated view does not have any dependencies (maintained or not maintained)"

# cleanup previous test
> DROP VIEW v11;
> DROP CLUSTER c4 CASCADE;




> CREATE CLUSTER c3 REPLICAS (r1 (SIZE '1'));
> CREATE MATERIALIZED VIEW v11 IN CLUSTER c3 AS (SELECT 1);
> CREATE DEFAULT INDEX IN CLUSTER c3 ON v11;

> SELECT o.name, a.recommendation, a.details FROM mz_internal.mz_index_advice AS a JOIN mz_objects AS o ON (a.object_id = o.id) ORDER BY name;
name              recommendation                     details
---------------------------------------------------------------------------------------------------------------------------------
 v11              "convert to a view with an index"  "no dependencies from sinks or objects on different clusters, but maintained dependencies on the same cluster: {v11_primary_idx}"
 v11_primary_idx  "drop unless queried directly"     "associated view does not have any dependencies (maintained or not maintained)"


# verify that all indexes and materialized views have a recommendation
> SELECT name FROM mz_objects WHERE id LIKE 'u%' AND type IN ('index', 'materialized-view')
  EXCEPT ALL
  SELECT o.name FROM mz_internal.mz_index_advice AS a INNER JOIN mz_objects AS o ON (a.object_id = o.id) WHERE o.type IN ('index', 'materialized-view');

# cleanup previous test
> DROP CLUSTER c3 CASCADE;




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


#                                   dependency tree of the above objects
#
#                                                                      ┌───────────────────┐
#                                                                      │        v11        │
#                                                          ┌──────────►│ materialized view │
#                                                          │           └───────────────────┘
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
#   ┌─────────┴─────────┐                        ┌─────────┴─────────┐
#   │        v41        │                        │        v42        │
#   │   index (c1, c2)  │                        │ materialized view │
#   └───────────────────┘                        │     index (c2)    │
#                                                └───────────────────┘
#
# With these dependencies, the index on v32 and the materialized view on v11 should be removed
# because these objects only have a single maintained dependency. But an index should be added
# on v31 because the work can be reused by v41 and v42. Moreover, v41 should be turned into a
# materialized view because the result is currently recomputed by the index on the remote
# cluster c2.


> SELECT o.name, a.recommendation, a.details FROM mz_internal.mz_index_advice AS a JOIN mz_objects AS o ON (a.object_id = o.id) ORDER BY name;
name             recommendation                 details
--------------------------------------------------------------------------------------------------------------------------------
v11              "convert to a view"            "no dependencies from sinks or objects on different clusters"
v21_primary_idx  "keep"                         "dependant objects: {v31,v42}"
v31              "add index"                    "multiple maintained dependencies: {v41,v42}"
v32_primary_idx  "drop unless queried directly" "fewer than two maintained dependencies: {v42}"
v41              "convert to materialized view" "indexes on multiple different clusters: {v41_primary_idx,v41_primary_idx1}"
v41_primary_idx  "drop unless queried directly" "associated view does not have any dependencies (maintained or not maintained)"
v41_primary_idx1 "drop unless queried directly" "associated view does not have any dependencies (maintained or not maintained)"
v42              "keep"                         "dependencies from sinks or objects on different clusters: {c2,v42_primary_idx}"
v42_primary_idx  "drop unless queried directly" "associated view does not have any dependencies (maintained or not maintained)"

# verify that all indexes and materialized views have a recommendation
> SELECT name FROM mz_objects WHERE id LIKE 'u%' AND type IN ('index', 'materialized-view')
  EXCEPT ALL
  SELECT o.name FROM mz_internal.mz_index_advice AS a INNER JOIN mz_objects AS o ON (a.object_id = o.id) WHERE o.type IN ('index', 'materialized-view');

# cleanup previous test
> DROP CLUSTER c1 CASCADE;
> DROP CLUSTER c2 CASCADE;
