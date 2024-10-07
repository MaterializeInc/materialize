# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for replica-targeted queries (SELECT and SUBSCRIBE).
#
# These tests rely on testdrive's retry feature, as they query introspection
# sources whose data might not be immediately available.

$ set-regex match=\d{13} replacement=<TIMESTAMP>

# Test that replica-targeted queries return results from the target replica.
#
# This test works by querying introspection sources, which are the only compute
# collections that can have different contents between replicas. Specifically,
# we look at the distinct `worker_id`s present in `mz_compute_frontiers_per_worker`
# for replicas with different worker counts. We know that
# `mz_compute_frontiers_per_worker` always contains entries, because it tracks the
# frontiers of introspection sources as well.

> CREATE CLUSTER test REPLICAS (
      r1 (SIZE '1'),
      r2 (SIZE '2'),
      r4 (SIZE '4')
  )

> SET cluster = test

> SET cluster_replica = r1

> SELECT DISTINCT worker_id FROM mz_introspection.mz_compute_frontiers_per_worker ORDER BY worker_id
0

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (
    SELECT count(DISTINCT worker_id) FROM mz_introspection.mz_compute_frontiers_per_worker
  )
> FETCH c
<TIMESTAMP> 1 1
> COMMIT

> SET cluster_replica = r2

> SELECT DISTINCT worker_id FROM mz_introspection.mz_compute_frontiers_per_worker ORDER BY worker_id
0
1

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (
    SELECT count(DISTINCT worker_id) FROM mz_introspection.mz_compute_frontiers_per_worker
  )
> FETCH c
<TIMESTAMP> 1 2
> COMMIT

> SET cluster_replica = r4

> SELECT DISTINCT worker_id FROM mz_introspection.mz_compute_frontiers_per_worker ORDER BY worker_id
0
1
2
3

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (
    SELECT count(DISTINCT worker_id) FROM mz_introspection.mz_compute_frontiers_per_worker
  )
> FETCH c
<TIMESTAMP> 1 4
> COMMIT

# Test that replica-targeted subscribes work when the subscribed collection
# advances to the empty frontier. Regression test for database-issues#7457.

> DROP CLUSTER test CASCADE
> CREATE CLUSTER test SIZE '4-4', REPLICATION FACTOR 4
> SET cluster_replica = r1

> CREATE MATERIALIZED VIEW mv AS SELECT 1

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv
> FETCH c
<TIMESTAMP> 1 1
> COMMIT

# We want to provoke the case where a non-targeted replica returns a response
# first, so try multiple times to be sure.

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv
> FETCH c
<TIMESTAMP> 1 1
> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv
> FETCH c
<TIMESTAMP> 1 1
> COMMIT
