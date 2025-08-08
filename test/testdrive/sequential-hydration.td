# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for sequential dataflow hydration in Compute.

# Test that dataflows that are unable to hydrate because their source data is
# not yet available don't block hydration of other dataflows.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_hydration_concurrency = 1;

> CREATE CLUSTER source1 SIZE 'scale=1,workers=1', REPLICATION FACTOR 0
> CREATE CLUSTER source2 SIZE 'scale=1,workers=1', REPLICATION FACTOR 0
> CREATE CLUSTER source3 SIZE 'scale=1,workers=1', REPLICATION FACTOR 1

> CREATE TABLE t (a int);
> CREATE MATERIALIZED VIEW mv1 IN CLUSTER source1 AS SELECT * FROM t
> CREATE MATERIALIZED VIEW mv2 IN CLUSTER source2 AS SELECT * FROM t
> CREATE MATERIALIZED VIEW mv3 IN CLUSTER source3 AS SELECT * FROM t

> CREATE INDEX idx1 ON mv1 (a)
> SELECT DISTINCT name, hydrated
  FROM mz_internal.mz_hydration_statuses
  JOIN mz_indexes ON (id = object_id)
  WHERE id LIKE 'u%'
idx1  false

> CREATE VIEW v12 AS SELECT * FROM mv1 JOIN mv2 USING (a)
> CREATE INDEX idx12 ON v12 (a)
> SELECT DISTINCT name, hydrated
  FROM mz_internal.mz_hydration_statuses
  JOIN mz_indexes ON (id = object_id)
  WHERE id LIKE 'u%'
idx1  false
idx12 false

> CREATE INDEX idx3 ON mv3 (a)
> SELECT DISTINCT name, hydrated
  FROM mz_internal.mz_hydration_statuses
  JOIN mz_indexes ON (id = object_id)
  WHERE id LIKE 'u%'
idx1  false
idx12 false
idx3  true

> ALTER CLUSTER source1 SET (REPLICATION FACTOR 1)
> SELECT DISTINCT name, hydrated
  FROM mz_internal.mz_hydration_statuses
  JOIN mz_indexes ON (id = object_id)
  WHERE id LIKE 'u%'
idx1  true
idx12 false
idx3  true

> ALTER CLUSTER source2 SET (REPLICATION FACTOR 1)
> SELECT DISTINCT name, hydrated
  FROM mz_internal.mz_hydration_statuses
  JOIN mz_indexes ON (id = object_id)
  WHERE id LIKE 'u%'
idx1  true
idx12 true
idx3  true
