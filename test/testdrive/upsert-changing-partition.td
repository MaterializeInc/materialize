# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=1

# Regression test on UPSERT behavior when a key changes partition.

$ kafka-create-topic topic=changing-partition partitions=3

$ kafka-ingest format=bytes key-format=bytes topic=changing-partition key-terminator=: partition=1
k1:1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER changing_partition_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE changing_partition
  IN CLUSTER changing_partition_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-changing-partition-${testdrive.seed}')
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  ENVELOPE UPSERT

> SELECT * FROM changing_partition
key  text
---------
k1   1

# an update to `k1` from a lower partition is ignored.
$ kafka-ingest format=bytes key-format=bytes topic=changing-partition key-terminator=: partition=0
k1:2
k2:2

> SELECT * FROM changing_partition
key  text
---------
k1   1
k2   2

# Higher partitions override.
$ kafka-ingest format=bytes key-format=bytes topic=changing-partition key-terminator=: partition=2
k1:3

> SELECT * FROM changing_partition
key  text
---------
k1   3
k2   2

> ALTER CLUSTER changing_partition_cluster SET (REPLICATION FACTOR 0)
> ALTER CLUSTER changing_partition_cluster SET (REPLICATION FACTOR 1)

# After restart, an update in a lower partition overrides. This verifies the expected
# behavior: only monotonically increasing partitions for keys have a consistent behavior.
$ kafka-ingest format=bytes key-format=bytes topic=changing-partition key-terminator=: partition=0
k1:4

> SELECT * FROM changing_partition
key  text
---------
k1   4
k2   2
