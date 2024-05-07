# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-replica-size=1

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_default_metadata_fetch_interval = 1000
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=upsert partitions=2

$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "fish", "f2": 1000}
{"key": "bird1"} {"f1":"goose", "f2": 1}
{"key": "birdmore"} {"f1":"geese", "f2": 2}
{"key": "mammal1"} {"f1": "moose", "f2": 1}
{"key": "bird1"}
{"key": "birdmore"} {"f1":"geese", "f2": 56}
{"key": "mammalmore"} {"f1": "moose", "f2": 42}
{"key": "mammal1"}
{"key": "mammalmore"} {"f1":"moose", "f2": 2}

$ kafka-create-topic topic=metrics-test partitions=1
$ kafka-ingest topic=metrics-test format=bytes
jack,jill
goofus,gallant

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CLUSTER stats_cluster SIZE '${arg.default-replica-size}'

> CREATE SOURCE upsert
  IN CLUSTER stats_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE OFFSET
  ENVELOPE UPSERT

# Adding a select here so that the ingests after this
# triggers lookup from the upsert state
> SELECT key, f1, f2 FROM upsert
key           f1      f2
------------------------
fish          fish    1000
birdmore      geese   56
mammalmore    moose   2

> SELECT
    s.name,
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
upsert 9 9

$ set-from-sql var=previous-offset-known
SELECT
    (SUM(u.offset_known))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('upsert')


$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "mammalmore"}

# Snapshot counts don't move...
> SELECT
    s.name,
    SUM(u.offset_known) > ${previous-offset-known},
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
upsert true 9 9

# ...even if we restart.

$ set-from-sql var=pre-restart-offset-committed
SELECT
    (SUM(u.offset_committed))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('upsert')

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 0)

$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "mammalmore"} {"f1":"moose", "f2": 100}

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 1)

> SELECT
    s.name,
    SUM(u.offset_committed) > ${pre-restart-offset-committed},
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
upsert true 9 9

> DROP SOURCE upsert CASCADE
