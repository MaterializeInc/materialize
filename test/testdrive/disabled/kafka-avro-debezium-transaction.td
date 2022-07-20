# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test support for Avro sources without using the Confluent Schema Registry.

# This test is broken.
# See: https://github.com/MaterializeInc/materialize/issues/13629

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
      { "name": "op", "type": "string" },
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

$ set txschema-bad-schema={
    "type": "record",
    "name": "TransactionMetadataValue",
    "namespace": "io.debezium.connector.common",
    "fields": [
      {"name": "status", "type": "string"},
      {
        "name": "id",
        "type": ["null", "string"]
      },
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

> CREATE SOURCE data_txdata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-txdata-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${txschema}'
  ENVELOPE NONE


$ kafka-ingest format=avro topic=data-txdata schema=${txschema} timestamp=4
{"status": "BEGIN", "id": "1", "event_count": null, "data_collections": null}
{"status": "END", "id": "1", "event_count": {"long": 4}, "data_collections": {"array": [{"event_count": 3, "data_collection": "testdrive-data-${testdrive.seed}"}, {"event_count": 1, "data_collection": "testdrive-data2-${testdrive.seed}"}]}}


$ kafka-create-topic topic=data

$ kafka-create-topic topic=data2

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": {"boolean": false}}, "op": "c", "transaction": {"total_order": null, "id": "1"}}
{"before": null, "after": {"row": {"a": 2, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"boolean": false}}, "op": "c", "transaction": {"total_order": null, "id": "1"}}

#
# Create a source using an inline schema.
#
> CREATE SOURCE data_schema_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM (
      TRANSACTION METADATA (SOURCE data_txdata, COLLECTION 'testdrive-data-${testdrive.seed}')
  )

> CREATE SOURCE data2_schema_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data2-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM (
      TRANSACTION METADATA (SOURCE data_txdata, COLLECTION 'testdrive-data2-${testdrive.seed}')
  )

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 4, "b": 5}}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "op": "c", "transaction": {"total_order": null, "id": "1"}}

# Note that this should still work even if data2 (which shares the transaction metadata source) isn't able to progress!
> SELECT a, b FROM data_schema_inline
a  b
-----
1  1
2  3
4  5

$ kafka-ingest format=avro topic=data-txdata schema=${txschema} timestamp=2
{"status": "BEGIN", "id": "5", "event_count": null, "data_collections": null}
{"status": "END", "id": "5", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "testdrive-data-${testdrive.seed}"}]}}


$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 8, "b": 9}}, "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "op": "c", "transaction": {"total_order": null, "id": "5"}}


> CREATE SINK data_sink FROM data_schema_inline
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-sink-${testdrive.seed}'
  WITH (reuse_topic=true) FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# Check that repeated Debezium messages are skipped.
$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 4, "b": 5}}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "op": "c", "transaction": {"total_order": null, "id": "1"}}
{"before": null, "after": {"row": {"a": 8, "b": 9}}, "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "op": "c", "transaction": {"total_order": null, "id": "5"}}

> SELECT a, b FROM data_schema_inline
a  b
----
1  1
2  3
4  5
8  9

# Now do data2
$ kafka-ingest format=avro topic=data2 schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 101, "b": 101}}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": {"boolean": false}}, "op": "c", "transaction": {"total_order": null, "id": "1"}}

> SELECT a, b FROM data2_schema_inline
a    b
---------
101  101

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ kafka-verify format=avro sink=materialize.public.data_sink sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 2, "b": 3}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 4, "b": 5}}, "transaction": {"id": "<TIMESTAMP>"}}

$ kafka-verify format=avro sink=materialize.public.data_sink sort-messages=true
{"before": null, "after": {"row": {"a": 8, "b": 9}}, "transaction": {"id": "<TIMESTAMP>"}}

#
# Test reading from the source when tx and data don't match
#
# We want the next message to have a different timestamp
> BEGIN
> DECLARE c CURSOR FOR TAIL data_schema_inline WITH (snapshot = false, progress = true)
> FETCH 1 c
mz_timestamp  mz_progressed  mz_diff  a       b
-------------------------------------------------
<TIMESTAMP>   true           <null>   <null>  <null>
> COMMIT

$ kafka-ingest format=avro topic=data-txdata schema=${txschema} timestamp=2
{"status": "BEGIN", "id": "7", "event_count": null, "data_collections": null}
{"status": "END", "id": "7", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "testdrive-data-${testdrive.seed}"}]}}

> SELECT a, b FROM data_schema_inline
a  b
-----
1  1
2  3
4  5
8  9


$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 2, "b": 7}}, "source": {"file": "binlog3", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "op": "c", "transaction": {"total_order": null, "id": "7"}}

> SELECT a, b FROM data_schema_inline
a  b
-----
1  1
2  3
4  5
8  9
2  7

# We want the next message to have a different timestamp
> BEGIN
> DECLARE c CURSOR FOR TAIL data_schema_inline WITH (snapshot = false, progress = true)
> FETCH 1 c
mz_timestamp  mz_progressed  mz_diff  a       b
-------------------------------------------------
<TIMESTAMP>   true           <null>   <null>  <null>
> COMMIT

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 3, "b": 9}}, "source": {"file": "binlog4", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "op": "c", "transaction": {"total_order": null, "id": "9"}}

> SELECT a, b FROM data_schema_inline
a  b
-----
1  1
2  3
4  5
8  9
2  7

$ kafka-ingest format=avro topic=data-txdata schema=${txschema} timestamp=2
{"status": "BEGIN", "id": "9", "event_count": null, "data_collections": null}
{"status": "END", "id": "9", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "testdrive-data-${testdrive.seed}"}]}}

> SELECT a, b FROM data_schema_inline
a  b
-----
1  1
2  3
4  5
8  9
2  7
3  9

$ unset-regex

# Reingest to verify that we keep transactionality
> CREATE SOURCE data_sink_reingest
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-sink-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

> SELECT after::text FROM data_sink_reingest ORDER BY transaction ASC
(1,1)
(2,3)
(4,5)
(8,9)
(2,7)
(3,9)

> SELECT COUNT(*) AS event_count FROM data_sink_reingest GROUP BY transaction ORDER BY transaction ASC
3
1
1
1

#
# Testing tx_metadata specification
#
$ kafka-create-topic topic=data-txdata-bad-schema
$ kafka-create-topic topic=data-bad-schema

> CREATE SOURCE data_txdata_bad_schema
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-txdata-bad-schema-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${txschema-bad-schema}'
  ENVELOPE NONE


$ kafka-ingest format=avro topic=data-txdata-bad-schema schema=${txschema-bad-schema} timestamp=4
{"status": "BEGIN", "id": null, "event_count": null, "data_collections": null}
{"status": "END", "id": {"string": "1"}, "event_count": {"long": 3}, "data_collections": {"array": [{"event_count": 3, "data_collection": "testdrive-data-${testdrive.seed}"}]}}

! CREATE SOURCE data_schema_inline_with_bad_schema_tx_metadata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-bad-schema-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM (
      TRANSACTION METADATA (
          SOURCE data_txdata_bad_schema,
          COLLECTION 'testdrive-data-bad-schema-${testdrive.seed}'
      )
  )
contains:'id' column must be of type non-nullable string

! CREATE SOURCE data_schema_inline_with_sink
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM (
      TRANSACTION METADATA (SOURCE data_sink, COLLECTION 'testdrive-data-${testdrive.seed}')
  )
contains:provided TRANSACTION METADATA SOURCE materialize.public.data_sink is not a source
