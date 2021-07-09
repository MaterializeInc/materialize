# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=\d{13} replacement=<TIMESTAMP>

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

> CREATE MATERIALIZED SOURCE input_rt
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE SINK output_rt FROM input_rt
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-rt-sink-${testdrive.seed}'
  WITH (exactly_once=true, consistency_topic='output-rt-sink-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE MATERIALIZED SOURCE input_byo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
    WITH (consistency_topic = 'testdrive-input-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE SINK output_byo FROM input_byo
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-byo-sink-${testdrive.seed}'
  WITH (exactly_once=true, consistency_topic='output-byo-sink-consistency-${testdrive.seed}')
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

$ kafka-verify format=avro sink=materialize.public.output_byo sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 2, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 3, "b": 1}}, "transaction": {"id": "1"}}

$ kafka-verify format=avro sink=materialize.public.output_byo sort-messages=true
{"before": null, "after": {"row": {"a": 11, "b": 11}}, "transaction": {"id": "2"}}
{"before": null, "after": {"row": {"a": 22, "b": 11}}, "transaction": {"id": "2"}}

$ kafka-verify format=avro sink=materialize.public.output_byo sort-messages=true
{"before": null, "after": {"row": {"a": 3, "b": 4}}, "transaction": {"id": "3"}}
{"before": null, "after": {"row": {"a": 5, "b": 6}}, "transaction": {"id": "3"}}

# can't distinguish "transactions" with real-time timestamping

$ kafka-verify format=avro sink=materialize.public.output_rt sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 11, "b": 11}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 2, "b": 1}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 22, "b": 11}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 3, "b": 1}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 3, "b": 4}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"a": 5, "b": 6}}, "transaction": {"id": "<TIMESTAMP>"}}

# Wait a bit to allow timestamp compaction to happen. We need to ensure that we
# get correct results even with compaction, which re-timestamps earlier data
# at later timestamps upon restarting.

> SELECT mz_internal.mz_sleep(2);
<null>
