# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

$ kafka-create-topic topic=upsert

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

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT

> CREATE CONNECTION c_conn
  FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}'

> BEGIN
> CREATE SOURCE upsert
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )

> CREATE TABLE upsert_tbl FROM SOURCE upsert (REFERENCE "testdrive-upsert-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION c_conn
  ENVELOPE UPSERT
> COMMIT

> SELECT * from upsert_tbl
key           f1       f2
---------------------------
fish          fish     1000
birdmore      geese    56
mammalmore    moose    2

# NOTE: These queries are slow to succeed because the default metrics scraping
# interval is 30 seconds.
#
# Ensure that statistics are correctly updated
> SELECT
    SUM(u.bytes_indexed) > 0,
    SUM(u.records_indexed),
    bool_and(u.rehydration_latency IS NOT NULL)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics_raw u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')
  GROUP BY t.name
  ORDER BY t.name
true 3 true

# Write another part to test backpressure
$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "fish", "f2": 1001}

> SELECT * from upsert_tbl
key           f1       f2
---------------------------
fish          fish     1001
birdmore      geese    56
mammalmore    moose    2
