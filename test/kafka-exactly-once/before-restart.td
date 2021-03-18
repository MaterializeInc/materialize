# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
      { "name": "after", "type": ["row", "null"] }
    ]
  }

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

$ kafka-create-topic topic=input-consistency

$ kafka-create-topic topic=input

> CREATE MATERIALIZED SOURCE input_byo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
    WITH (consistency = 'testdrive-input-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE SINK output FROM input_byo
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-sink-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-ingest format=avro topic=input-consistency timestamp=1 schema=${trxschemakey}
{"id": "10"}
{"id": "30"}
{"id": "40"}

$ kafka-ingest format=avro topic=input-consistency timestamp=1 schema=${trxschema}
{"status":"BEGIN","id":"10","event_count":null,"data_collections":null}
{"status":"END","id":"10","event_count":{"long": 4},"data_collections":{"array": [{"event_count": 4, "data_collection": "testdrive-input-${testdrive.seed}"}]}}
{"status":"BEGIN","id":"30","event_count":null,"data_collections":null}
{"status":"END","id":"30","event_count":{"long": 2},"data_collections":{"array": [{"event_count": 2, "data_collection": "testdrive-input-${testdrive.seed}"}]}}
{"status":"BEGIN","id":"40","event_count":null,"data_collections":null}
{"status":"END","id":"40","event_count":{"long": 2},"data_collections":{"array": [{"event_count": 2, "data_collection": "testdrive-input-${testdrive.seed}"}]}}

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 1, "b": 1}}}
{"before": null, "after": {"row": {"a": 2, "b": 1}}}
{"before": null, "after": {"row": {"a": 3, "b": 1}}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}}

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=3
{"before": null, "after": {"row": {"a": 11, "b": 11}}}
{"before": null, "after": {"row": {"a": 22, "b": 11}}}

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=4
{"before": null, "after": {"row": {"a": 3, "b": 4}}}
{"before": null, "after": {"row": {"a": 5, "b": 6}}}

$ kafka-verify format=avro sink=materialize.public.output
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 2, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 3, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 11, "b": 11}}, "transaction": {"id": "2"}}
{"before": null, "after": {"row": {"a": 22, "b": 11}}, "transaction": {"id": "2"}}
{"before": null, "after": {"row": {"a": 3, "b": 4}}, "transaction": {"id": "3"}}
{"before": null, "after": {"row": {"a": 5, "b": 6}}, "transaction": {"id": "3"}}

> SELECT * FROM input_byo
a  b
------
1   1
2   1
3   1
1   2
11  11
22  11
3   4
5   6
