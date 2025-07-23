# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SET transaction_isolation = 'strict serializable'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

# Clean up cluster manually, since testdrive does not automatically clean up
# clusters.
> DROP CLUSTER IF EXISTS storage;

# Create a table for use throughout the test.
> CREATE TABLE t (a int)

# Create a cluster for sources and sinks.
> CREATE CLUSTER storage REPLICAS ()

# Querying a cluster with no replicas does not succeed.
> SET cluster = storage
! SELECT generate_series(1, 1)
contains:CLUSTER "storage" has no replicas available to service request

# Creating a source in an empty, zero-replica cluster should work.
> CREATE SOURCE loadgen IN CLUSTER storage FROM LOAD GENERATOR COUNTER (UP TO 100)

! ALTER SOURCE loadgen SET (SIZE = '1')
contains:Expected one of TIMESTAMP or RETAIN, found SIZE

# Create indexes and materialized views in a storage cluster is allowed.
> CREATE INDEX t_idx IN CLUSTER storage ON t (a)
> CREATE MATERIALIZED VIEW mv IN CLUSTER storage AS SELECT 1

> CREATE CLUSTER REPLICA storage.r1 SIZE = '1'

# Executing queries on a storage cluster is allowed.
> SELECT generate_series(1, 1)
1

# Creating sources on clusters containing compute objects is allowed
> CREATE SOURCE lg2 IN CLUSTER storage FROM LOAD GENERATOR COUNTER (UP TO 100)

# Test that `DROP CLUSTER` only succeeds with `CASCADE`.
! DROP CLUSTER storage
contains:cannot drop cluster "storage" because other objects depend on it
> DROP CLUSTER storage CASCADE

> SET cluster = quickstart

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
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

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
contains:Expected one of PARTITION or SNAPSHOT or VERSION

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
