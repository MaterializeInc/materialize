# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true

# Clean up cluster manually, since testdrive does not automatically clean up
# clusters.
> DROP CLUSTER IF EXISTS storage;

# Create a table for use throughout the test.
> CREATE TABLE t (a int)

# Create a cluster for sources and sinks.
> CREATE CLUSTER storage REPLICAS ()

# Querying a cluster with no replicas does not succeed.
> SET cluster = storage
! SELECT 1
contains:CLUSTER "storage" has no replicas available to service request

# Specifying `IN CLUSTER` and `SIZE` simultaneously is banned.
! CREATE SOURCE loadgen IN CLUSTER storage FROM LOAD GENERATOR COUNTER WITH (SIZE = '1')
contains:only one of IN CLUSTER or SIZE can be set

# Creating a source in an empty, zero-replica cluster should work.
> CREATE SOURCE loadgen IN CLUSTER storage FROM LOAD GENERATOR COUNTER

! ALTER SOURCE loadgen SET (SIZE = '1')
contains:cannot change the size of a source or sink created with IN CLUSTER

# Create indexes and materialized views in a storage cluster is banned.
! CREATE INDEX bad IN CLUSTER storage ON t (a)
contains:cannot create this kind of item in a cluster that contains sources or sinks
! CREATE MATERIALIZED VIEW bad IN CLUSTER storage AS SELECT 1
contains:cannot create this kind of item in a cluster that contains sources or sinks

# Executing queries on a storage cluster is banned.
! SELECT 1
contains:cannot execute queries on cluster containing sources or sinks

# Only one replica of a storage cluster is permitted.
> CREATE CLUSTER REPLICA storage.r1 SIZE '1'
! CREATE CLUSTER REPLICA storage.r2 SIZE '1'
contains:cannot create more than one replica of a cluster containing sources or sinks

# Creating indexes and materialized views in a cluster is permitted once the
# source has been dropped.
> DROP SOURCE loadgen;
> CREATE INDEX idx IN CLUSTER storage ON t (a)
> CREATE MATERIALIZED VIEW mv IN CLUSTER storage AS SELECT 1

# As is running queries.
> SELECT 1
1

# Recreating the source is banned.
! CREATE SOURCE loadgen IN CLUSTER storage FROM LOAD GENERATOR COUNTER
contains:cannot create source in cluster containing indexes or materialized views

# Creating a second replica is now allowed.
> CREATE CLUSTER REPLICA storage.r2 SIZE '1'

# The source cannot be readded even after the index and materialized views
# are dropped because of the second replica.
> DROP INDEX idx
> DROP MATERIALIZED VIEW mv
! CREATE SOURCE loadgen IN CLUSTER storage FROM LOAD GENERATOR COUNTER
contains:cannot create source in cluster with more than one replica

# Creating the source is allowed after dropping the second replica
> DROP CLUSTER REPLICA storage.r2
> CREATE SOURCE loadgen IN CLUSTER storage FROM LOAD GENERATOR COUNTER

# Test that `DROP CLUSTER` only succeeds with `CASCADE`.
! DROP CLUSTER storage
contains:cannot drop cluster with active objects
> DROP CLUSTER storage CASCADE

> SET cluster = default

# Test that a cluster can contain multiple sources and sinks, and verify that
# the sources and sinks produce the correct output.

> CREATE CLUSTER storage REPLICAS (r1 (SIZE '1'))

$ kafka-create-topic topic=data1 partitions=1

$ kafka-create-topic topic=data2 partitions=1

$ kafka-ingest format=bytes topic=data1
a
b

$ kafka-ingest format=bytes topic=data2
aa
bb

> CREATE CONNECTION kafka
  TO KAFKA (BROKER '${testdrive.kafka-addr}')

> CREATE SOURCE data1
  IN CLUSTER storage
  FROM KAFKA CONNECTION kafka (TOPIC 'testdrive-data1-${testdrive.seed}')
  FORMAT TEXT

> CREATE SOURCE data2
  IN CLUSTER storage
  FROM KAFKA CONNECTION kafka (TOPIC 'testdrive-data2-${testdrive.seed}')
  FORMAT TEXT

> SELECT * FROM data1
a
b

> SELECT * FROM data2
aa
bb

> CREATE MATERIALIZED VIEW view1 AS SELECT text FROM data1

> CREATE MATERIALIZED VIEW view2 AS SELECT text || text AS text FROM data2

> CREATE SINK sink1
  IN CLUSTER storage
  FROM view1
  INTO KAFKA CONNECTION kafka (TOPIC 'sink1-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM

! ALTER SINK sink1 SET (SIZE = '1')
contains:cannot change the size of a source or sink created with IN CLUSTER

> CREATE SINK sink2
  IN CLUSTER storage
  FROM view2
  INTO KAFKA CONNECTION kafka (TOPIC 'sink2-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.sink1 key=false sort-messages=true
{"before": null, "after": {"text": "a"}}
{"before": null, "after": {"text": "b"}}

$ kafka-verify-data format=json sink=materialize.public.sink2 key=false sort-messages=true
{"before": null, "after": {"text": "aaaa"}}
{"before": null, "after": {"text": "bbbb"}}

# Test that the replica can be sized up and the sources and sinks correctly
# restart.

> DROP CLUSTER REPLICA storage.r1
> CREATE CLUSTER REPLICA storage.r1 SIZE '2'

$ kafka-ingest format=bytes topic=data1
c

$ kafka-ingest format=bytes topic=data2
cc

> SELECT * FROM data1
a
b
c

> SELECT * FROM data2
aa
bb
cc

$ kafka-verify-data format=json sink=materialize.public.sink1 key=false sort-messages=true
{"before": null, "after": {"text": "c"}}

$ kafka-verify-data format=json sink=materialize.public.sink2 key=false sort-messages=true
{"before": null, "after": {"text": "cccc"}}

# Test that the `size` and `cluster_id` fields are correctly populated in the
# system catalog for sources and sinks.
> SELECT s.name, s.size, c.name
  FROM mz_sources s
  JOIN mz_clusters c ON c.id = s.cluster_id
  WHERE s.id LIKE 'u%'
data1  <null>  storage
data2  <null>  storage
> SELECT s.name, s.size, c.name
  FROM mz_sinks s
  JOIN mz_clusters c ON c.id = s.cluster_id
  WHERE s.id LIKE 'u%'
sink1  <null>  storage
sink2  <null>  storage

> DROP CLUSTER storage CASCADE
