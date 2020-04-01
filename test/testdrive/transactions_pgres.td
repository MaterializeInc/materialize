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

$ set trxschema={
    "name": "trx",
    "type": "record",
    "fields": [
      {"name": "id", "type": "string"},
      {"name": "status", "type": "string"},
      {"name": "event_count", "type": ["long","null"]},
      {"name": "data_collections", "type": ["array","null"],
       "items": {
         "name": "data",
         "type": "record",
         "fields": [
           {"name": "event_count", "type": "long"},
           {"name": "data_collection", "type": "string"}
         ]
        }
      }
     ]
  }

$ kafka-create-topic topic=data-consistency

$ kafka-create-topic topic=data

$ kafka-ingest format=bytes topic=data-consistency timestamp=1
{ "status": "BEGIN", "id": "570", "event_count": null, "data_collections": null}
{ "status": "END", "id": "570", "event_count": 1, "data_collections": [ { "data_collection": "data", "event_count": 4}, {"data_collection": "foo2", "event_count": 0 }]}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 1}}
{"before": null, "after": {"a": 2, "b": 1}}
{"before": null, "after": {"a": 3, "b": 1}}
{"before": null, "after": {"a": 1, "b": 2}}

> CREATE MATERIALIZED SOURCE data_byo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
    WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM
