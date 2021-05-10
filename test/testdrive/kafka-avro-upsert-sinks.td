# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test Avro UPSERT sinks. Some tests that should cover both DEBEZIUM and UPSERT
# sinks are only in kafka-avro-upsert-sinks.td

# Test a basic sink with multiple rows.

> CREATE VIEW data (a, b) AS VALUES (1, 1), (2, 1), (3, 1), (4, 1)

> CREATE VIEW data_keyed AS SELECT a, max(b) AS b FROM data GROUP BY a

> CREATE SINK data_sink FROM data_keyed
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'data-sink'
  KEY (a)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE UPSERT

$ kafka-verify format=avro sink=materialize.public.data_sink sort-messages=true
{"a": 1} {"a": 1, "b": 1}
{"a": 2} {"a": 2, "b": 1}
{"a": 3} {"a": 3, "b": 1}
{"a": 4} {"a": 4, "b": 1}

# More complicated scenarios: super keys, consistency input/output

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

$ kafka-create-topic topic=consistency
$ kafka-create-topic topic=input

> CREATE MATERIALIZED SOURCE input
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
    WITH (consistency = 'testdrive-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE VIEW input_keyed AS SELECT a, max(b) as b FROM input GROUP BY a

> CREATE SINK input_sink FROM input_keyed
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'input-sink' KEY (a)
  WITH (consistency = true) FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE UPSERT

# requesting to key by (a, b) is fine when (a) is a unique key

> CREATE SINK input_sink_multiple_keys FROM input_keyed
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'input-sink' KEY (b, a)
  WITH (consistency = true) FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE UPSERT

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 1, "b": 1}}}
{"before": null, "after": {"row": {"a": 2, "b": 2}}}

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 3, "b": 1}}}
{"before": null, "after": {"row": {"a": 4, "b": 2}}}
{"before": null, "after": {"row": {"a": 1, "b": 7}}}

$ kafka-ingest format=avro topic=consistency timestamp=1 schema=${trxschema}
{"status":"BEGIN","id":"1","event_count":null,"data_collections":null}
{"status":"END","id":"1","event_count":{"long": 2},"data_collections":{"array": [{"event_count": 2, "data_collection": "testdrive-input-${testdrive.seed}"}]}}
{"status":"BEGIN","id":"2","event_count":null,"data_collections":null}
{"status":"END","id":"2","event_count":{"long": 2},"data_collections":{"array": [{"event_count": 2, "data_collection": "testdrive-input-${testdrive.seed}"}]}}
{"status":"BEGIN","id":"3","event_count":null,"data_collections":null}
{"status":"END","id":"3","event_count":{"long": 1},"data_collections":{"array": [{"event_count": 1, "data_collection": "testdrive-input-${testdrive.seed}"}]}}

> SELECT * FROM input;
a  b
------
1  1
2  2
3  1
4  2
1  7

# Compare sorted messages within each transaction. We know that messages of one
# transaction appear together as one "bundle" in the output. But there is no
# guarantee on the order within a transaction.

$ kafka-verify format=avro sink=materialize.public.input_sink sort-messages=true
{"a": 1} {"a": 1, "b": 1, "transaction": {"id": "1"}}
{"a": 2} {"a": 2, "b": 2, "transaction": {"id": "1"}}

$ kafka-verify format=avro sink=materialize.public.input_sink sort-messages=true
{"a": 3} {"a": 3, "b": 1, "transaction": {"id": "2"}}
{"a": 4} {"a": 4, "b": 2, "transaction": {"id": "2"}}

$ kafka-verify format=avro sink=materialize.public.input_sink sort-messages=true
{"a": 1} {"a": 1, "b": 7, "transaction": {"id": "3"}}

$ kafka-verify format=avro sink=materialize.public.input_sink consistency=debezium
{"id": "1", "status": "BEGIN", "event_count": null}
{"id": "1", "status": "END", "event_count": {"long": 2}}
{"id": "2", "status": "BEGIN", "event_count": null}
{"id": "2", "status": "END", "event_count": {"long": 2}}
{"id": "3", "status": "BEGIN", "event_count": null}
{"id": "3", "status": "END", "event_count": {"long": 1}}

# Again, compare split by transaction. See comment just above.

$ kafka-verify format=avro sink=materialize.public.input_sink_multiple_keys sort-messages=true
{"a": 1, "b": 1} {"a": 1, "b": 1, "transaction": {"id": "1"}}
{"a": 2, "b": 2} {"a": 2, "b": 2, "transaction": {"id": "1"}}

$ kafka-verify format=avro sink=materialize.public.input_sink_multiple_keys sort-messages=true
{"a": 3, "b": 1} {"a": 3, "b": 1, "transaction": {"id": "2"}}
{"a": 4, "b": 2} {"a": 4, "b": 2, "transaction": {"id": "2"}}

# missing key denotes DELETE

$ kafka-verify format=avro sink=materialize.public.input_sink_multiple_keys sort-messages=true
{"a": 1, "b": 1}
{"a": 1, "b": 7} {"a": 1, "b": 7, "transaction": {"id": "3"}}

# verify if/when input deletions are emitted to an UPSERT sink

$ kafka-create-topic topic=input-with-deletions

> CREATE MATERIALIZED SOURCE input_with_deletions
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-with-deletions-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE VIEW input_with_deletions_keyed AS SELECT a, max(b) as b FROM input_with_deletions GROUP BY a

> CREATE SINK input_with_deletions_sink FROM input_with_deletions_keyed
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'input-sink' KEY (a)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE UPSERT

$ kafka-ingest format=avro topic=input-with-deletions schema=${schema}
{"before": null, "after": {"row": {"a": 1, "b": 1}}}

$ kafka-verify format=avro sink=materialize.public.input_with_deletions_sink sort-messages=true
{"a": 1} {"a": 1, "b": 1}

$ kafka-ingest format=avro topic=input-with-deletions schema=${schema}
{"before": null, "after": {"row": {"a": 1, "b": 2}}}

$ kafka-verify format=avro sink=materialize.public.input_with_deletions_sink sort-messages=true
{"a": 1} {"a": 1, "b": 2}

# deletion of the "shadowed" input should not cause downstream updates

$ kafka-ingest format=avro topic=input-with-deletions schema=${schema}
{"before": {"row": {"a": 1, "b": 1}}, "after": null}

$ kafka-verify format=avro sink=materialize.public.input_with_deletions_sink sort-messages=true

$ kafka-ingest format=avro topic=input-with-deletions schema=${schema}
{"before": {"row": {"a": 1, "b": 2}}, "after": null}

# now we should see a NULL update on the key, which means a DELETE

$ kafka-verify format=avro sink=materialize.public.input_with_deletions_sink sort-messages=true
{"a": 1}

$ kafka-ingest format=avro topic=input-with-deletions schema=${schema}
{"before": null, "after": {"row": {"a": 1, "b": 1}}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}}

$ kafka-verify format=avro sink=materialize.public.input_with_deletions_sink sort-messages=true
{"a": 1} {"a": 1, "b": 2}

$ kafka-ingest format=avro topic=input-with-deletions schema=${schema}
{"before": {"row": {"a": 1, "b": 2}}, "after": null}

# removing the occluding input should "reveal" the previous input again
#
$ kafka-verify format=avro sink=materialize.public.input_with_deletions_sink sort-messages=true
{"a": 1} {"a": 1, "b": 1}
