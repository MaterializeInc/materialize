# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# A very simple smoke test that will be run against various Kafka versions
#

$ kafka-create-topic topic=input_csv

> CREATE SOURCE input_csv (first, second)
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input_csv-${testdrive.seed}'
  FORMAT CSV WITH 2 COLUMNS;

$ kafka-ingest format=bytes topic=input_csv
1,2
2,3

> SELECT * from input_csv;
first second
------------
1     2
2     3

$ file-append path=input.proto
syntax = "proto3";

message Input {
    string field = 1;
}

$ protobuf-compile-descriptors inputs=input.proto output=input.pb

$ kafka-create-topic topic=input_proto

> CREATE SOURCE input_proto
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input_proto-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Input' USING SCHEMA FILE '${testdrive.temp-dir}/input.pb'

$ kafka-ingest format=protobuf topic=input_proto message=Input descriptor-file=input.pb timestamp=1
{"field": "a"}
{"field": "b"}

> SELECT * from input_proto
field
-----
a
b

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] },
      { "name": "op", "type": "string" },
      {
        "name": "source",
        "type": {
          "type": "record",
          "name": "Source",
          "namespace": "whatever",
          "fields": [
            { "name": "snapshot", "type": "boolean" },
            { "name": "lsn", "type": ["long", "null"] },
            { "name": "sequence", "type": ["string", "null"] }
          ]
        }
      }
    ]
  }

$ kafka-create-topic topic=input_avro

$ kafka-ingest format=avro topic=input_avro schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 123}}, "op": "c", "source": {"snapshot": false, "lsn": null, "sequence": null}}

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE input_avro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input_avro-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT * FROM input_avro
a
---
123
