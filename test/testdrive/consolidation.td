# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set trxschemakey={
      "name": "io.debezium.connector.common.TransactionMetadataKey",
      "type": "record",
      "fields": [
          {
              "name": "id",
              "type": "string"
          }
      ]
  }

$ set trxschema={
    "type":"record", "name":"TransactionMetadataValue", "namespace":"io.debezium.connector.common",
    "fields":[
    {"name":"status","type":"string"},
    {"name":"id","type": "string"},
    {"name": "event_count",
    "type": ["null", "long"],
    "default": null
    },
    {"name":"data_collections","type":["null",{"type":"array",
    "items": {"type":"record",
    "name":"ConnectDefault",
    "namespace":"io.confluent.connect.Avro",
    "fields": [ {
    "name": "data_collection",
    "type": "string"
    },
    {
    "name": "event_count",
    "type": "long" }]}}],
    "default": null}],
    "connect.name": "io.debezium.connector.common.TransactionMetadataValue"
    }

$ set names-schema={
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
              {"name": "num", "type": "long"},
              {"name": "name", "type": "string"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data-consistency

$ kafka-create-topic topic=names

$ kafka-ingest format=avro topic=names schema=${names-schema} timestamp=1
{"before": null, "after": {"num": 1, "name": "one"}}
{"before": {"num": 1, "name": "one"}, "after": {"num": 2, "name": "two"}}
{"before": {"num": 2, "name": "two"}, "after": {"num": 3, "name": "three"}}

$ kafka-ingest format=avro topic=data-consistency timestamp=1 schema=${trxschemakey}
{"id": "1"}

$ kafka-ingest format=avro topic=data-consistency timestamp=1 schema=${trxschema}
{"status":"BEGIN","id":"1","event_count":null,"data_collections":null}
{"status":"END","id":"1","event_count":3,"data_collections":[{"event_count": 3, "data_collection": "testdrive-names-${testdrive.seed}"}]}

> CREATE MATERIALIZED SOURCE names
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-names-${testdrive.seed}'
  with (consistency='testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${names-schema}'
  ENVELOPE DEBEZIUM

> CREATE SINK snk1 FROM names
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.snk1
{"before": null, "after": {"num": 3, "name": "three"}}
