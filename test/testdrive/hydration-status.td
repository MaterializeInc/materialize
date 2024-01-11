# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test reporting of dataflow hydration status through
# `mz_internal.mz_compute_hydration_statuses` and
# `mz_internal.mz_hydration_statuses`.
#
# Note that all of the below tests only assert that the `hydrated` flag
# eventually becomes `true`, not that it starts off as `false`. That's because
# we have no control about the hydration timing of dataflows or the update
# cadence of the hydration introspection relations, so we have no reliable way
# of ensuring that a query arrives before a dataflow has hydrated.
#
# These tests rely on testdrive's retry feature, as dataflows can take an
# unknown (but hopefully small) time to hydrate. Furthermore, the queried
# introspection relations are asynchronously updated, so DDL commands are not
# immediately reflected there.

> CREATE CLUSTER test REPLICAS (hydrated_test_1 (SIZE '1'))
> SET cluster = test

# Test that on an empty cluster only the introspection dataflows show up.

> SELECT DISTINCT left(h.object_id, 1), h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  WHERE r.name LIKE 'hydrated_test%';
s true

> SELECT DISTINCT left(h.object_id, 1), h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  WHERE r.name LIKE 'hydrated_test%';
s true

# Test adding new compute dataflows.

> CREATE TABLE t (a int)
> CREATE INDEX idx ON t (a)
> CREATE MATERIALIZED VIEW mv WITH (RETAIN HISTORY FOR '30s') AS SELECT * FROM t

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_1 true
mv  hydrated_test_1 true

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
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
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_1 true
idx hydrated_test_2 true
mv  hydrated_test_1 true
mv  hydrated_test_2 true

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
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
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_2 true
mv  hydrated_test_2 true

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_2 true
mv  hydrated_test_2 true

> DROP CLUSTER REPLICA test.hydrated_test_2

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

# Test dropping dataflows.

> CREATE CLUSTER REPLICA test.hydrated_test_3 SIZE '1'

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_3 true
mv  hydrated_test_3 true

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
idx hydrated_test_3 true
mv  hydrated_test_3 true

> DROP INDEX idx;

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_compute_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
mv  hydrated_test_3 true

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
mv  hydrated_test_3 true

# Test adding new storage dataflows.

> CREATE SOURCE src
  IN CLUSTER test
  FROM LOAD GENERATOR auction
  FOR ALL TABLES
> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
> CREATE CONNECTION csr_conn
  TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}')
> CREATE SINK snk
  IN CLUSTER test
  FROM mv
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# Webhook sources are not backed by dataflows, so they have no concept of
# hydration and shouldn't show up in mz_hydration_statuses.
> CREATE SOURCE web IN CLUSTER test FROM WEBHOOK BODY FORMAT JSON

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
accounts      hydrated_test_3 true
auctions      hydrated_test_3 true
bids          hydrated_test_3 true
mv            hydrated_test_3 true
organizations hydrated_test_3 true
src           hydrated_test_3 true
snk           hydrated_test_3 true
users         hydrated_test_3 true

# Test dropping replicas.

> DROP CLUSTER REPLICA test.hydrated_test_3

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

# Test adding new replicas.

> CREATE CLUSTER REPLICA test.hydrated_test_4 SIZE '1'

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';
accounts      hydrated_test_4 true
auctions      hydrated_test_4 true
bids          hydrated_test_4 true
mv            hydrated_test_4 true
organizations hydrated_test_4 true
src           hydrated_test_4 true
snk           hydrated_test_4 true
users         hydrated_test_4 true

# Test dropping dataflows.

> DROP SINK snk
> DROP SOURCE src
> DROP MATERIALIZED VIEW mv

> SELECT o.name, r.name, h.hydrated
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON (r.id = h.replica_id)
  JOIN mz_objects o ON (o.id = h.object_id)
  WHERE
      r.name LIKE 'hydrated_test%' AND
      o.id NOT LIKE 's%';

# Cleanup
> DROP CLUSTER test CASCADE;
