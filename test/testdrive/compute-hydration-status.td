# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test reporting of compute dataflow hydration status through
# `mz_internal.mz_compute_hydration_status`.
#
# Note that all of the below tests only assert that the `hydrated` flag
# eventually becomes `true`, not that it starts off as `false`. That's because
# we have no control about the hydration timing of dataflows or the update
# cadence of `mz_compute_hydration_status`, so we have no reliable way of
# ensuring that a query arrives before a dataflow has hydrated.
#
# These tests rely on testdrive's retry feature, as the
# `mz_compute_hydration_status` table is asynchronously updates, so DDL
# commands are not immediately reflected there.

> CREATE CLUSTER test REPLICAS (hydrated_test_1 (SIZE '1'))
> SET cluster = test

# Test that on an empty cluster only the introspection dataflows show up.
> SELECT DISTINCT left(h.object_id, 1), h.hydrated
  FROM mz_internal.mz_compute_hydration_status h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  WHERE r.name LIKE 'hydrated_test%';
s true

# Test adding new dataflows.

> CREATE TABLE t (a int)
> CREATE INDEX idx ON t (a)
> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_status h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_1 true
mv  hydrated_test_1 true

# Test adding new replicas.

> CREATE CLUSTER REPLICA test.hydrated_test_2 SIZE '1'

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_status h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_1 true
idx hydrated_test_2 true
mv  hydrated_test_1 true
mv  hydrated_test_2 true

# Test dropping replicas.

> DROP CLUSTER REPLICA test.hydrated_test_1

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_status h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_2 true
mv  hydrated_test_2 true

> DROP CLUSTER REPLICA test.hydrated_test_2

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_status h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

# Test dropping dataflows.

> CREATE CLUSTER REPLICA test.hydrated_test_3 SIZE '1'

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_status h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_3 true
mv  hydrated_test_3 true

> DROP MATERIALIZED VIEW mv;

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_status h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_3 true

> DROP INDEX idx;

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_status h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

# Cleanup
> DROP CLUSTER test CASCADE;
