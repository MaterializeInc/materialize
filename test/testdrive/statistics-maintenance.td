# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000

> CREATE CLUSTER cluster1 (SIZE 'scale=1,workers=2')

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE TABLE t (a text, b text)
> CREATE MATERIALIZED VIEW simple_view AS SELECT * from t;
> INSERT INTO t VALUES ('key1', 'value')

# Setup various sinks and sources
> CREATE SINK sink1
  IN CLUSTER cluster1
  FROM simple_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'topic-${testdrive.seed}')
  KEY (a)
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-create-topic topic=upsert partitions=1
$ kafka-ingest format=bytes topic=upsert key-format=bytes key-terminator=:
one:two

> BEGIN
> CREATE SOURCE upsert1
  IN CLUSTER cluster1
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )

> CREATE TABLE upsert1_tbl FROM SOURCE upsert1 (REFERENCE "testdrive-upsert-${testdrive.seed}")
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
  ENVELOPE UPSERT
> COMMIT

> SELECT * FROM upsert1_tbl
one two

# The `CREATE TABLE ... FROM SOURCE` commands caused a recreation of the
# respective source dataflows, during which we might have lost the statistics
# about committed updates from the snapshot. Ingest some more data to ensure we
# see some `updates_committed`.
$ kafka-ingest format=bytes topic=upsert key-format=bytes key-terminator=:
two:three

# Ensure we produce statistics
> SELECT s.name, SUM(u.messages_staged), SUM(u.messages_committed), SUM(u.bytes_staged) > 0, SUM(bytes_staged) = SUM(bytes_committed)
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('sink1')
  GROUP BY s.name
sink1 1 1 true true

> SELECT s.name,
  SUM(u.updates_committed) > 0,
  SUM(u.messages_received) >= 2
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('upsert1')
  GROUP BY s.id, s.name
upsert1 true true

# Shut down the cluster
> ALTER CLUSTER cluster1 SET (REPLICATION FACTOR = 0)

# Statistics should remain the same
> SELECT s.name, SUM(u.messages_staged), SUM(u.messages_committed), SUM(u.bytes_staged) > 0, SUM(bytes_staged) = SUM(bytes_committed)
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('sink1')
  GROUP BY s.name
sink1 1 1 true true

> SELECT s.name,
  SUM(u.updates_committed) > 0,
  SUM(u.messages_received) >= 2
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('upsert1')
  GROUP BY s.id, s.name
upsert1 true true

# Ingest some more data, and ensure counters are maintained

> ALTER CLUSTER cluster1 SET (REPLICATION FACTOR = 1)
> INSERT INTO t VALUES ('key1', 'value')
$ kafka-ingest format=bytes topic=upsert key-format=bytes key-terminator=:
three:four

# Statistics should remain the same
> SELECT s.name, SUM(u.messages_staged), SUM(u.messages_committed), SUM(u.bytes_staged) > 0, SUM(bytes_staged) = SUM(bytes_committed)
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('sink1')
  GROUP BY s.name
sink1 2 2 true true

> SELECT s.name,
  SUM(u.updates_committed) > 0,
  SUM(u.messages_received) >= 4
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('upsert1')
  GROUP BY s.id, s.name
upsert1 true true
