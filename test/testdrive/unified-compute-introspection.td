# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test for a subset of the information returned by unified compute
# introspection relations.
#
# Note that we count on the retry behavior of testdrive in this test
# since introspection sources may take some time to catch up.

$ set-sql-timeout duration=60s
$ set-arg-default default-replica-size=scale=1,workers=1

# Test dataflow error introspection.

> CREATE CLUSTER test SIZE '${arg.default-replica-size}'
> SET cluster = test

> CREATE TABLE zeros (a int)
> CREATE VIEW v_div_by_zero AS SELECT 1 / a AS x FROM zeros
> CREATE INDEX idx1_div_by_zero ON v_div_by_zero (x)
> CREATE MATERIALIZED VIEW mv_div_by_zero AS SELECT 1 / a AS x FROM zeros
> CREATE INDEX idx2_div_by_zero ON mv_div_by_zero (x)

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0

> INSERT INTO zeros VALUES (0)

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0
idx1_div_by_zero r1 1
idx2_div_by_zero r1 1
mv_div_by_zero   r1 1

> INSERT INTO zeros VALUES (0), (0)

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0
idx1_div_by_zero r1 3
idx2_div_by_zero r1 3
mv_div_by_zero   r1 3

> DELETE FROM zeros

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0

# Test that errors are inserted for new replicas.

> INSERT INTO zeros VALUES (0), (0)

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0
idx1_div_by_zero r1 2
idx2_div_by_zero r1 2
mv_div_by_zero   r1 2

> ALTER CLUSTER test SET (REPLICATION FACTOR 2)

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0
idx1_div_by_zero r1 2
idx1_div_by_zero r2 2
idx2_div_by_zero r1 2
idx2_div_by_zero r2 2
mv_div_by_zero   r1 2
mv_div_by_zero   r2 2

# Test that error logging is retracted when replicas are dropped.

> ALTER CLUSTER test SET (REPLICATION FACTOR 1)

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0
idx1_div_by_zero r1 2
idx2_div_by_zero r1 2
mv_div_by_zero   r1 2

# Test that error logging is retracted when objects are dropped.

> DROP INDEX idx1_div_by_zero

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0
idx2_div_by_zero r1 2
mv_div_by_zero   r1 2

> DROP MATERIALIZED VIEW mv_div_by_zero

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0

# Test logging of errors in indexes that advance to the empty frontier.
#
# Note that the same is not expected to work for MVs that advance to the empty
# frontier. There is no reason to keep completed MV dataflows around, so we
# drop them immediately, removing all their associated logging in the process.

> CREATE MATERIALIZED VIEW mv_zero AS SELECT 0 AS x
> CREATE VIEW div_by_zero AS SELECT 1 / x FROM mv_zero
> CREATE INDEX idx_div_by_zero ON div_by_zero ()

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0
idx_div_by_zero r1 1

> DROP MATERIALIZED VIEW mv_zero CASCADE
> DROP TABLE zeros CASCADE

> SELECT o.name, r.name, c.count
  FROM mz_internal.mz_compute_error_counts_raw_unified c
  JOIN mz_objects o ON c.object_id = o.id
  JOIN mz_cluster_replicas r ON c.replica_id = r.id
  WHERE c.count != 0

# Clean up.
> DROP CLUSTER test
