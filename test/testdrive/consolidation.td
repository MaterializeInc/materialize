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

$ kafka-ingest format=avro topic=names schema=${names-schema} timestamp=2
{"before": {"num": 3, "name": "three"}, "after": {"num": 4, "name": "four"}}
{"before": {"num": 4, "name": "four"}, "after": {"num": 5, "name": "five"}}

$ kafka-ingest format=avro topic=data-consistency timestamp=2 schema=${trxschemakey}
{"id": "2"}
{"id": "3"}

$ kafka-ingest format=avro topic=data-consistency timestamp=2 schema=${trxschema}
{"status":"BEGIN","id":"2","event_count":null,"data_collections":null}
{"status":"END","id":"2","event_count":1,"data_collections":[{"event_count": 1, "data_collection": "testdrive-names-${testdrive.seed}"}]}
{"status":"BEGIN","id":"3","event_count":null,"data_collections":null}
{"status":"END","id":"3","event_count":1,"data_collections":[{"event_count": 1, "data_collection": "testdrive-names-${testdrive.seed}"}]}

> SELECT * FROM names AS OF 2;
num name
---------
4 "four"

> ALTER INDEX materialize.public.names_primary_idx SET(logical_compaction_window='1ms')

$ kafka-ingest format=avro topic=names schema=${names-schema} timestamp=3
{"before": {"num": 5, "name": "five"}, "after": {"num": 6, "name": "six"}}

$ kafka-ingest format=avro topic=data-consistency timestamp=3 schema=${trxschemakey}
{"id": "4"}

$ kafka-ingest format=avro topic=data-consistency timestamp=3 schema=${trxschema}
{"status":"BEGIN","id":"4","event_count":null,"data_collections":null}
{"status":"END","id":"4","event_count":1,"data_collections":[{"event_count": 1, "data_collection": "testdrive-names-${testdrive.seed}"}]}

! SELECT * FROM names AS OF 2;
Timestamp (2) is not valid for all inputs

# Reset the compaction window back to default (currently 60,000)
> ALTER INDEX materialize.public.names_primary_idx RESET(logical_compaction_window)

$ kafka-ingest format=avro topic=names schema=${names-schema} timestamp=4
{"before": {"num": 6, "name": "six"}, "after": {"num": 7, "name": "seven"}}
{"before": {"num": 7, "name": "seven"}, "after": {"num": 8, "name": "eight"}}

$ kafka-ingest format=avro topic=data-consistency timestamp=4 schema=${trxschemakey}
{"id": "5"}
{"id": "6"}

$ kafka-ingest format=avro topic=data-consistency timestamp=4 schema=${trxschema}
{"status":"BEGIN","id":"5","event_count":null,"data_collections":null}
{"status":"END","id":"5","event_count":1,"data_collections":[{"event_count": 1, "data_collection": "testdrive-names-${testdrive.seed}"}]}
{"status":"BEGIN","id":"6","event_count":null,"data_collections":null}
{"status":"END","id":"6","event_count":1,"data_collections":[{"event_count": 1, "data_collection": "testdrive-names-${testdrive.seed}"}]}

> SELECT * FROM names AS OF 4;
num name
---------
6 "six"
