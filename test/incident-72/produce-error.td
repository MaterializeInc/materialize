# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

> CREATE CLUSTER source_cluster SIZE '1';

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=textavro

$ kafka-ingest format=avro topic=textavro key-format=bytes key-terminator=: schema=${schema}
fish: {"f1": "fish", "f2": 1000}

> CREATE SOURCE textavro
  IN CLUSTER source_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textavro-${testdrive.seed}')
  KEY FORMAT TEXT
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE UPSERT

> SELECT * FROM textavro
fish fish 1000

# Now ingest some garbage to produce the error
$ kafka-ingest topic=textavro key-format=bytes key-terminator=: format=bytes
fish: garbage

! SELECT * FROM textavro
exact: Envelope error: Upsert: Value error: Decode error: Text: avro deserialization error: wrong Confluent-style avro serialization magic: expected 0, got 32 (original bytes: [20, 67, 61, 72, 62, 61, 67, 65]), decoded key: Row{[String("fish")]}

# Turn off the cluster for now so that we can control when healing happens in the subsequent tests
> ALTER CLUSTER source_cluster SET (REPLICATION FACTOR 0)
