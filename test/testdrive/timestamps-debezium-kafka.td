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

$ set trxschema= {
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

$ kafka-create-topic topic=consistency

$ kafka-create-topic topic=foo

$ kafka-create-topic topic=bar

$ kafka-ingest format=avro topic=foo schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 1}}
{"before": null, "after": {"a": 2, "b": 2}}

$ kafka-ingest format=avro topic=bar schema=${schema} timestamp=1
{"before": null, "after": {"a": 10, "b": 1}}

> CREATE MATERIALIZED SOURCE server.data_foo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-foo-${testdrive.seed}'
    WITH (consistency = 'testdrive-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE MATERIALIZED SOURCE server.data_bar
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-bar-${testdrive.seed}'
    WITH (consistency = 'testdrive-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM


> CREATE MATERIALIZED VIEW foo AS SELECT b, sum(a) FROM data_foo GROUP BY b

> CREATE MATERIALIZED VIEW bar AS SELECT b, sum(a) FROM data_bar GROUP BY b

> CREATE MATERIALIZED VIEW join AS SELECT * FROM foo JOIN bar USING (b);

! SELECT * FROM bar;
At least one input has no complete timestamps yet:

! SELECT * FROM foo;
At least one input has no complete timestamps yet:

! SELECT * FROM join ;
At least one input has no complete timestamps yet:

$ kafka-ingest format=avro topic=consistency timestamp=1 schema=${trxschemakey}
{"id": "1"}

$ kafka-ingest format=avro topic=consistency timestamp=1 schema=${trxschema}
{"status":"BEGIN","id":"1","event_count":null,"data_collections":null}
{"status":"END","id":"1","event_count":0,"data_collections":[]}
{"status":"BEGIN","id":"2","event_count":null,"data_collections":null}
{"status":"END","id":"2","event_count":2,"data_collections":[{"event_count": 2, "data_collection": "testdrive-foo-${testdrive.seed}"},{"event_count": 1, "data_collection": "testdrive-bar-${testdrive.seed}"}]}

> SELECT * FROM foo;
b  sum
------
1  1
2  2

> SELECT * FROM bar;
b  sum
------
1  10

> SELECT * FROM join;
a  a  b
------
1  1  10

$ kafka-ingest format=avro topic=foo schema=${schema} timestamp=1
{"before": null, "after": {"a": 3, "b": 3}}

$ kafka-ingest format=avro topic=bar schema=${schema} timestamp=1
{"before": null, "after": {"a": 30, "b": 3}}

$ kafka-ingest format=avro topic=consistency timestamp=1 schema=${trxschema}
{"status":"BEGIN","id":"3","event_count":null,"data_collections":null}
{"status":"END","id":"3","event_count":2,"data_collections":[{"event_count": 2, "data_collection": "testdrive-foo-${testdrive.seed}"},{"event_count": 1, "data_collection": "testdrive-bar-${testdrive.seed}"}]}

> SELECT * FROM foo;
b  sum
------
1  1
2  2

> SELECT * FROM bar;
b  sum
------
1  10
3  30

> SELECT * FROM join;
a  a  b
------
1  1  10

$ kafka-ingest format=avro topic=foo schema=${schema} timestamp=1
{"before": null, "after": {"a": 4, "b": 4}}

> SELECT * FROM foo;
b  sum
------
1  1
2  2
3  3
4  4

> SELECT * FROM join;
a  a  b
------
1  1  10
3  3  30
