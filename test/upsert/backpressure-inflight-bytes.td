# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This tests that upsert does not panic with backpressure max inflight bytes configured
$ kafka-create-topic topic=upsert

$ kafka-ingest format=bytes topic=upsert key-format=bytes key-terminator=:
bird:goose
animal:whale

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> DROP CLUSTER IF EXISTS storage_cluster CASCADE;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_disk_cluster_replicas = true
ALTER SYSTEM SET enable_managed_clusters = true
ALTER SYSTEM SET storage_dataflow_max_inflight_bytes TO 1
ALTER SYSTEM SET storage_dataflow_max_inflight_bytes_to_cluster_size_percent TO 10

> CREATE CLUSTER storage_cluster SIZE 'mem-2', REPLICATION FACTOR 1, DISK = true

> CREATE SOURCE upsert
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT

> SELECT count(*) from upsert
2
