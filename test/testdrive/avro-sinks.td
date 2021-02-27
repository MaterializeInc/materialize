# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test Avro sinks.

# Test that we invent field names for unnamed columns.

> CREATE VIEW unnamed_cols AS SELECT 1, 2 AS b, 3;

> CREATE SINK unnamed_cols_sink FROM unnamed_cols
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'unnamed-cols-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.unnamed_cols_sink
{"before": null, "after": {"row": {"column1": 1, "b": 2, "column3": 3}}}

# Test that invented field names do not clash with named columns.

> CREATE VIEW clashing_cols AS SELECT 1, 2 AS column1, 3 as b, 4 as b, 5 as b;

> CREATE SINK clashing_cols_sink FROM clashing_cols
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'clashing-cols-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.clashing_cols_sink
{"before": null, "after": {"row": {"column1": 1, "column1_1": 2, "b": 3, "b1": 4, "b2": 5}}}

# Test a basic sink with multiple rows.

> CREATE VIEW data (a, b) AS VALUES (1, 1), (2, 1), (3, 1), (1, 2)

> CREATE SINK data_sink FROM data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.data_sink
{"before": null, "after": {"row": {"a": 1, "b": 1}}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}}
{"before": null, "after": {"row": {"a": 2, "b": 1}}}
{"before": null, "after": {"row": {"a": 3, "b": 1}}}

# Test date/time types.

> CREATE VIEW datetime_data (date, ts, ts_tz) AS VALUES
  (DATE '2000-01-01', TIMESTAMP '2000-01-01 10:10:10.111', TIMESTAMPTZ '2000-01-01 10:10:10.111+02'),
  (DATE '2000-02-01', TIMESTAMP '2000-02-01 10:10:10.111', TIMESTAMPTZ '2000-02-01 10:10:10.111+02')

> CREATE SINK datetime_data_sink FROM datetime_data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'datetime-data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.datetime_data_sink
{"before": null, "after": {"row": {"date": 10988, "ts": 949399810111000, "ts_tz": 949392610111000}}}
{"before": null, "after": {"row": {"date": 10957, "ts": 946721410111000, "ts_tz": 946714210111000}}}

> CREATE VIEW time_data (time) AS VALUES (TIME '01:02:03'), (TIME '01:02:04')

> CREATE SINK time_data_sink FROM time_data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'time-data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.time_data_sink
{"before": null, "after": {"row": {"time": 3723000000}}}
{"before": null, "after": {"row": {"time": 3724000000}}}

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

> CREATE SINK input_sink FROM input
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'input-sink' KEY (a)
  WITH (consistency = true) FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK input_sink_multiple_keys FROM input
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'input-sink' KEY (b, a)
  WITH (consistency = true) FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

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

$ kafka-verify format=avro sink=materialize.public.input_sink
{"a": 1} {"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "1"}}
{"a": 2} {"before": null, "after": {"row": {"a": 2, "b": 2}}, "transaction": {"id": "1"}}
{"a": 3} {"before": null, "after": {"row": {"a": 3, "b": 1}}, "transaction": {"id": "2"}}
{"a": 4} {"before": null, "after": {"row": {"a": 4, "b": 2}}, "transaction": {"id": "2"}}

$ kafka-verify format=avro sink=materialize.public.input_sink consistency=debezium
{"id": "1", "status": "BEGIN", "event_count": null}
{"id": "1", "status": "END", "event_count": {"long": 2}}
{"id": "2", "status": "BEGIN", "event_count": null}
{"id": "2", "status": "END", "event_count": {"long": 2}}
! CREATE SINK bad_sink FROM input
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'input-sink' KEY (a, a)
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
Repeated column name in sink key: a

> CREATE VIEW ambiguous (a, a) AS SELECT * FROM (VALUES (1, 2))

! CREATE SINK bad_sink FROM ambiguous
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'input-sink' KEY (a)
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
Ambiguous column: a

$ kafka-verify format=avro sink=materialize.public.input_sink_multiple_keys
{"b": 1, "a": 1} {"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "1"}}
{"b": 2, "a": 2} {"before": null, "after": {"row": {"a": 2, "b": 2}}, "transaction": {"id": "1"}}
{"b": 1, "a": 3} {"before": null, "after": {"row": {"a": 3, "b": 1}}, "transaction": {"id": "2"}}
{"b": 2, "a": 4} {"before": null, "after": {"row": {"a": 4, "b": 2}}, "transaction": {"id": "2"}}

> CREATE VIEW json_data (a, b) AS VALUES ('{"a":1, "b":2}'::jsonb, 2)

# Sinks with JSON columns should not crash - see https://github.com/MaterializeInc/materialize/issues/4722
> CREATE SINK json_data_sink FROM json_data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'json-data-sink'
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
