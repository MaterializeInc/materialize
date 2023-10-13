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
> DROP CLUSTER IF EXISTS cluster1;

> CREATE CLUSTER cluster1 REPLICAS (r1 (SIZE '2'))

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE TABLE t (a text, b text)
> CREATE MATERIALIZED VIEW simple_view AS SELECT * from t;
> INSERT INTO t VALUES ('key1', 'value')

# Setup various sinks and sources
> CREATE SINK sink_size FROM simple_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'topic-${testdrive.seed}')
  KEY (a)
  FORMAT JSON
  ENVELOPE DEBEZIUM
  WITH (SIZE = '2')

> CREATE SINK sink_cluster
  IN CLUSTER cluster1
  FROM simple_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'topic-${testdrive.seed}')
  KEY (a)
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-create-topic topic=upsert partitions=1
$ kafka-ingest format=bytes topic=upsert key-format=bytes key-terminator=:
one:two

> CREATE SOURCE upsert_size
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
  ENVELOPE UPSERT
  WITH (SIZE = '2')

> CREATE SOURCE upsert_cluster
  IN CLUSTER cluster1
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
  ENVELOPE UPSERT

> CREATE SCHEMA subsources_size;
> CREATE SCHEMA subsources_cluster;
> CREATE SOURCE subsources_size.s FROM LOAD GENERATOR AUCTION FOR ALL TABLES WITH (SIZE = '2');
> CREATE SOURCE subsources_cluster.s IN CLUSTER cluster1 FROM LOAD GENERATOR AUCTION FOR ALL TABLES;

# NOTE: These queries are slow to succeed because the default metrics scraping
# interval is 30 seconds. Here we are just ensuring metrics were populated.

> SELECT s.name, SUM(u.messages_staged), SUM(u.messages_committed), SUM(u.bytes_staged) > 0, SUM(bytes_staged) = SUM(bytes_committed)
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics u ON s.id = u.id
  WHERE s.name IN ('sink_size', 'sink_cluster')
  GROUP BY s.name
sink_size 1 1 true true
sink_cluster 1 1 true true

> SELECT s.name,
  SUM(u.updates_committed) > 0
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert_size', 'upsert_cluster', 's', 'bids')
  GROUP BY s.id, s.name
upsert_size true
upsert_cluster true
s false
s false
bids true
bids true

# We have to obtain these before we delete the sink.
$ set-from-sql var=sink-size-id
SELECT s.id
FROM mz_sinks s
WHERE s.name IN ('sink_size')
$ set-from-sql var=sink-cluster-id
SELECT s.id
FROM mz_sinks s
WHERE s.name IN ('sink_cluster')

$ set-from-sql var=upsert-size-id
SELECT s.id
FROM mz_sources s
WHERE s.name IN ('upsert_size')
$ set-from-sql var=upsert-cluster-id
SELECT s.id
FROM mz_sources s
WHERE s.name IN ('upsert_cluster')

# We also need to check that subsources and top-level
# sources are cleared out.
$ set-from-sql var=subsources-size-top-level-id
SELECT s.id
FROM mz_sources s
JOIN mz_schemas sch
ON sch.id = s.schema_id
WHERE s.name IN ('s') AND sch.name = 'subsources_size'
$ set-from-sql var=subsources-size-bids-id
SELECT s.id
FROM mz_sources s
JOIN mz_schemas sch
ON sch.id = s.schema_id
WHERE s.name IN ('bids') AND sch.name = 'subsources_size'

$ set-from-sql var=subsources-cluster-top-level-id
SELECT s.id
FROM mz_sources s
JOIN mz_schemas sch
ON sch.id = s.schema_id
WHERE s.name IN ('s') AND sch.name = 'subsources_cluster'
$ set-from-sql var=subsources-cluster-bids-id
SELECT s.id
FROM mz_sources s
JOIN mz_schemas sch
ON sch.id = s.schema_id
WHERE s.name IN ('bids') AND sch.name = 'subsources_cluster'

> DROP SINK sink_size
> DROP SINK sink_cluster
> DROP SOURCE upsert_size
> DROP SOURCE upsert_cluster
> DROP SOURCE subsources_size.s
> DROP SOURCE subsources_cluster.s

> SELECT COUNT(*)
  FROM mz_internal.mz_sink_statistics u
  WHERE u.id = '${sink-size-id}'
0
> SELECT COUNT(*)
  FROM mz_internal.mz_sink_statistics u
  WHERE u.id = '${sink-cluster-id}'
0
> SELECT COUNT(*)
  FROM mz_internal.mz_sink_statistics u
  WHERE u.id = '${upsert-size-id}'
0
> SELECT COUNT(*)
  FROM mz_internal.mz_sink_statistics u
  WHERE u.id = '${upsert-cluster-id}'
0
> SELECT COUNT(*)
  FROM mz_internal.mz_sink_statistics u
  WHERE u.id = '${subsources-size-top-level-id}'
0
> SELECT COUNT(*)
  FROM mz_internal.mz_sink_statistics u
  WHERE u.id = '${subsources-size-bids-id}'
0
> SELECT COUNT(*)
  FROM mz_internal.mz_sink_statistics u
  WHERE u.id = '${subsources-cluster-top-level-id}'
0
> SELECT COUNT(*)
  FROM mz_internal.mz_sink_statistics u
  WHERE u.id = '${subsources-cluster-bids-id}'
0

> DROP CLUSTER cluster1
