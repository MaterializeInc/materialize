# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test reporting of controller frontiers through `mz_internal.mz_frontiers` and
# `mz_internal.mz_cluster_replica_frontiers`.
#
# These tests rely on testdrive's retry feature, as they query introspection
# sources whose data might not be immediately available.

> DROP CLUSTER IF EXISTS test
> DROP CLUSTER IF EXISTS test_source

> CREATE CLUSTER test REPLICAS (
    r1 (SIZE '1'),
    r2 (SIZE '1')
  )
> CREATE CLUSTER test_source REPLICAS (
    s1 (SIZE '1')
  )

> SET cluster = test;

> CREATE TABLE t1 (a int)
> INSERT INTO t1 VALUES (1)

# Test that frontiers of materialized views are reported.

> CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t1

> SELECT
    mvs.name,
    replicas.name
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  JOIN mz_materialized_views mvs
    ON frontiers.object_id = mvs.id
  LEFT JOIN mz_cluster_replicas replicas
    ON frontiers.replica_id = replicas.id
  WHERE
    frontiers.object_id LIKE 'u%' AND
    frontiers.write_frontier > 0
mv1 r1
mv1 r2

> SELECT
    mvs.name
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_materialized_views mvs
    ON frontiers.object_id = mvs.id
  WHERE
    frontiers.object_id LIKE 'u%' AND
    frontiers.read_frontier > 0 AND
    frontiers.write_frontier > 0
mv1


# Test that frontiers of indexes are reported.

> CREATE INDEX idx1 ON t1 (a)

> SELECT
    indexes.name,
    replicas.name
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  JOIN mz_indexes indexes
    ON frontiers.object_id = indexes.id
  LEFT JOIN mz_cluster_replicas replicas
    ON frontiers.replica_id = replicas.id
  WHERE
    frontiers.object_id LIKE 'u%' AND
    frontiers.write_frontier > 0
idx1 r1
idx1 r2

> SELECT
    indexes.name
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_indexes indexes
    ON frontiers.object_id = indexes.id
  WHERE
    frontiers.object_id LIKE 'u%' AND
    frontiers.read_frontier > 0 AND
    frontiers.write_frontier > 0
idx1


# Test that frontiers of sources are reported.

> CREATE SOURCE source1
  IN CLUSTER test_source
  FROM LOAD GENERATOR COUNTER

> SELECT
    sources.name,
    replicas.name
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  JOIN mz_sources sources
    ON frontiers.object_id = sources.id
  LEFT JOIN mz_cluster_replicas replicas
    ON frontiers.replica_id = replicas.id
  WHERE
    frontiers.object_id LIKE 'u%' AND
    frontiers.write_frontier > 0
source1 s1

> SELECT
    sources.name
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_sources sources
    ON frontiers.object_id = sources.id
  WHERE
    frontiers.object_id LIKE 'u%' AND
    frontiers.read_frontier > 0 AND
    frontiers.write_frontier > 0
source1
source1_progress

# Test that frontiers of sinks are reported.

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  )

> CREATE SINK sink1
  IN CLUSTER test_source
  FROM t1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT
    sinks.name,
    replicas.name
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  JOIN mz_sinks sinks
    ON frontiers.object_id = sinks.id
  LEFT JOIN mz_cluster_replicas replicas
    ON frontiers.replica_id = replicas.id
  WHERE
    frontiers.object_id LIKE 'u%' AND
    frontiers.write_frontier > 0
sink1 s1

> SELECT
    sinks.name
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_sinks sinks
    ON frontiers.object_id = sinks.id
  WHERE
    frontiers.object_id LIKE 'u%' AND
    frontiers.read_frontier IS NULL AND
    frontiers.write_frontier > 0
sink1

# Test that the frontiers of introspection sources are reported.

> SELECT
    replicas.name
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  JOIN mz_indexes indexes
    ON frontiers.object_id = indexes.id
  JOIN mz_clusters clusters
    ON indexes.cluster_id = clusters.id
  LEFT JOIN mz_cluster_replicas replicas
    ON frontiers.replica_id = replicas.id
  WHERE
    indexes.name LIKE 'mz_active_peeks_per_worker_u%_primary_idx' AND
    frontiers.write_frontier > 0 AND
    clusters.name = 'test'
r1
r2

> SELECT
    count(*)
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_indexes indexes
    ON frontiers.object_id = indexes.id
  JOIN mz_clusters clusters
    ON indexes.cluster_id = clusters.id
  WHERE
    indexes.name LIKE 'mz_active_peeks_per_worker_u%_primary_idx' AND
    frontiers.read_frontier > 0 AND
    frontiers.write_frontier > 0 AND
    clusters.name = 'test'
1

# Test that the frontiers of tables are reported.

> SELECT
    tables.name
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_tables tables
    ON frontiers.object_id = tables.id
  WHERE
    frontiers.object_id LIKE 'u%' AND
    frontiers.read_frontier > 0 AND
    frontiers.write_frontier > 0
t1

# Test that the frontiers of storage-managed collections are reported.

> SELECT
    sources.name
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_sources sources
    ON frontiers.object_id = sources.id
  WHERE
    sources.name = 'mz_frontiers' AND
    frontiers.read_frontier > 0 AND
    frontiers.write_frontier > 0
mz_frontiers

# Test that frontiers are added when replicas are created.

> SELECT
    objects.name,
    replicas.name
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  JOIN mz_objects objects
    ON frontiers.object_id = objects.id
  JOIN mz_cluster_replicas replicas
    ON frontiers.replica_id = replicas.id
  JOIN mz_clusters clusters
    ON replicas.cluster_id = clusters.id
  WHERE
    objects.id LIKE 'u%' AND
    frontiers.write_frontier > 0 AND
    clusters.name = 'test'
idx1 r1
idx1 r2
mv1  r1
mv1  r2

> CREATE CLUSTER REPLICA test.r3 SIZE '1'

> SELECT
    objects.name,
    replicas.name
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  JOIN mz_objects objects
    ON frontiers.object_id = objects.id
  JOIN mz_cluster_replicas replicas
    ON frontiers.replica_id = replicas.id
  JOIN mz_clusters clusters
    ON replicas.cluster_id = clusters.id
  WHERE
    objects.id LIKE 'u%' AND
    frontiers.write_frontier > 0 AND
    clusters.name = 'test'
idx1 r1
idx1 r2
idx1 r3
mv1  r1
mv1  r2
mv1  r3

# Test that frontiers are removed when replicas are dropped.

> DROP CLUSTER REPLICA test.r1

> SELECT
    objects.name,
    replicas.name
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  JOIN mz_objects objects
    ON frontiers.object_id = objects.id
  JOIN mz_cluster_replicas replicas
    ON frontiers.replica_id = replicas.id
  JOIN mz_clusters clusters
    ON replicas.cluster_id = clusters.id
  WHERE
    objects.id LIKE 'u%' AND
    frontiers.write_frontier > 0 AND
    clusters.name = 'test'
idx1 r2
idx1 r3
mv1  r2
mv1  r3

# Test that empty frontiers show up as NULL.

> CREATE MATERIALIZED VIEW mv2 AS SELECT 1

> SELECT
    replicas.name,
    frontiers.write_frontier
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  JOIN mz_materialized_views mvs
    ON frontiers.object_id = mvs.id
  JOIN mz_cluster_replicas replicas
    ON frontiers.replica_id = replicas.id
  WHERE
    mvs.name = 'mv2'
r2 <null>
r3 <null>

> SELECT
    frontiers.read_frontier,
    frontiers.write_frontier
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_materialized_views mvs
    ON frontiers.object_id = mvs.id
  WHERE
    mvs.name = 'mv2'
0 <null>

# Test that frontiers are removed when objects are dropped.

> DROP MATERIALIZED VIEW mv1
> DROP MATERIALIZED VIEW mv2
> DROP INDEX idx1
> DROP SOURCE source1 CASCADE
> DROP SINK sink1
> DROP TABLE t1

> SELECT *
  FROM mz_internal.mz_cluster_replica_frontiers frontiers
  WHERE object_id LIKE 'u%'

> SELECT *
  FROM mz_internal.mz_frontiers frontiers
  WHERE object_id LIKE 'u%'

# Test that frontiers are correctly initialized on for collections on clusters
# with zero replicas.

> CREATE CLUSTER empty SIZE '1', REPLICATION FACTOR 0
> CREATE TABLE t2 (a int)
> CREATE INDEX idx2 IN CLUSTER empty ON t2 (a)

> SELECT read_frontier > 0, read_frontier = write_frontier
  FROM mz_internal.mz_frontiers
  JOIN mz_indexes ON (id = object_id)
  WHERE name = 'idx2'
true true
