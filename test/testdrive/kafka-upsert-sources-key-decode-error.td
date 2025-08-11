# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

$ set keyschema-1-int-key={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "int"}
    ]
  }

$ set keyschema-1-long-key={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"nokey", "type":"string"}
        ]
    }

$ kafka-create-topic topic=int2long

$ kafka-ingest format=avro topic=int2long key-format=avro key-schema=${keyschema-1-int-key} schema=${schema}
{"key": 1234} {"nokey": "nokey1"}

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE int2long
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-int2long-${testdrive.seed}')

> CREATE TABLE int2long_tbl FROM SOURCE int2long (REFERENCE "testdrive-int2long-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT
> COMMIT

$ kafka-ingest format=avro topic=int2long key-format=avro key-schema=${keyschema-1-long-key} schema=${schema}
{"key": 999999999999} {"nokey": "nokey1"}

! SELECT * FROM int2long_tbl
contains:Writer schema has type `Long`, but reader schema has type `Int` for field `Key.key`
