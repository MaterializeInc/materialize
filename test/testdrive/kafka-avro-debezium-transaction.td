# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test support for Avro sources without using the Confluent Schema Registry.

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
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] },
      {
        "name": "source",
        "type": {
          "type": "record",
          "name": "Source",
          "namespace": "io.debezium.connector.mysql",
          "fields": [
            {
              "name": "file",
              "type": "string"
            },
            {
              "name": "pos",
              "type": "long"
            },
            {
              "name": "row",
              "type": "int"
            },
            {
              "name": "snapshot",
              "type": [
                {
                  "type": "boolean",
                  "connect.default": false
                },
                "null"
              ],
              "default": false
            }
          ],
          "connect.name": "io.debezium.connector.mysql.Source"
        }
      },
      {
        "name": "transaction",
        "type": {
          "type": "record",
          "name": "Transaction",
          "namespace": "whatever",
          "fields": [
            {
              "name": "total_order",
              "type": ["long", "null"]
            },
            {
              "name": "id",
              "type": "string"
            }
          ]
        }
      }
    ]
  }

$ set txschema={
    "type": "record",
    "name": "TransactionMetadataValue",
    "namespace": "io.debezium.connector.common",
    "fields": [
      {"name": "status", "type": "string"},
      {"name": "id", "type": "string"},
      {
        "name": "event_count",
        "type": ["null", "long"],
        "default": null
      },
      {
        "name": "data_collections",
        "type": [
          "null",
          {
            "type": "array",
            "items": {
              "type": "record",
              "name": "ConnectDefault",
              "namespace": "io.confluent.connect.Avro",
              "fields": [
                {"name": "data_collection", "type": "string"},
                {"name": "event_count", "type": "long"}
              ]
            }
          }
        ],
        "default": null
      }
    ],
    "connect.name": "io.debezium.connector.common.TransactionMetadataValue"
  }

$ kafka-create-topic topic=data-txdata

> CREATE MATERIALIZED SOURCE data_txdata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-txdata-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${txschema}'
  ENVELOPE NONE


$ kafka-ingest format=avro topic=data-txdata schema=${txschema} timestamp=4
{"status": "BEGIN", "id": "1", "event_count": null, "data_collections": null}
{"status": "END", "id": "1", "event_count": {"long": 3}, "data_collections": {"array": [{"event_count": 3, "data_collection": "testdrive-data-${testdrive.seed}"}]}}


$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "1"}}
{"before": null, "after": {"row": {"a": 2, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "1"}}

# Create a source using an inline schema.

> CREATE MATERIALIZED SOURCE data_schema_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH(tx_metadata = data_txdata)
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 4, "b": 5}}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "1"}}


> SELECT a, b FROM data_schema_inline
a  b
-----
1  1
2  3
4  5

# XXX(chae): TODO: allow reading when only have some of the data that's described by tx: e.g. be able to read if we've submiited the the BEGIN/END tx_id 5 records but not the tx_id 5 data records

$ kafka-ingest format=avro topic=data-txdata schema=${txschema} timestamp=7
{"status": "BEGIN", "id": "5", "event_count": null, "data_collections": null}
{"status": "END", "id": "5", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "testdrive-data-${testdrive.seed}"}]}}


$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 8, "b": 9}}, "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "5"}}


> CREATE SINK data_sink FROM data_schema_inline
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-sink-${testdrive.seed}'
  WITH (reuse_topic=true) FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# Check that repeated Debezium messages are skipped.
$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 4, "b": 5}}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "1"}}
{"before": null, "after": {"row": {"a": 8, "b": 9}}, "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "5"}}

> SELECT a, b FROM data_schema_inline
a  b
----
1  1
2  3
4  5
8  9

# Create a source using a file schema. This should fail if the named schema file
# does not exist.

! CREATE MATERIALIZED SOURCE data_schema_file
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA FILE 'data-schema.json'
  ENVELOPE DEBEZIUM
contains:No such file or directory

$ file-append path=data-schema.json
\${schema}

> CREATE MATERIALIZED SOURCE data_schema_file
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH(tx_metadata = data_txdata)
  FORMAT AVRO USING SCHEMA FILE '${testdrive.temp-dir}/data-schema.json'
  ENVELOPE DEBEZIUM

> SELECT a, b FROM data_schema_file
a  b
----
1  1
2  3
4  5
8  9

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ kafka-verify format=avro sink=materialize.public.data_sink sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 2, "b": 3}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 4, "b": 5}}, "transaction": {"id": "<TIMESTAMP>"}}

$ kafka-verify format=avro sink=materialize.public.data_sink sort-messages=true
{"before": null, "after": {"row": {"a": 8, "b": 9}}, "transaction": {"id": "<TIMESTAMP>"}}
